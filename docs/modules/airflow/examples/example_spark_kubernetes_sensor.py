#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Optional, Dict
from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook


class SparkKubernetesSensor(BaseSensorOperator):  # <3>
    template_fields = ("application_name", "namespace")
    # See https://github.com/stackabletech/spark-k8s-operator/pull/460/files#diff-d737837121132af6b60f50279a78464b05dcfd06c05d1d090f4198a5e962b5f6R371
    # Unknown is set immediately so it must be excluded from the failed states.
    FAILURE_STATES = "Failed"
    SUCCESS_STATES = "Succeeded"

    def __init__(
        self,
        *,
        application_name: str,
        namespace: Optional[str] = None,
        kubernetes_conn_id: str = "kubernetes_in_cluster",  # <2>
        api_group: str = "spark.stackable.tech",
        api_version: str = "v1alpha1",
        poke_interval: float = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.api_group = api_group
        self.api_version = api_version
        self.poke_interval = poke_interval

    def poke(self, context: Dict) -> bool:
        self.log.info("Poking: %s", self.application_name)
        response = self.hook.get_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural="sparkapplications",
            name=self.application_name,
            namespace=self.namespace,
        )
        try:
            application_state = response["status"]["phase"]
        except KeyError:
            self.log.debug(
                f"SparkApplication status could not be established: {response}"
            )
            return False
        if application_state in self.FAILURE_STATES:
            raise AirflowException(
                f"SparkApplication failed with state: {application_state}"
            )
        elif application_state in self.SUCCESS_STATES:
            self.log.info("SparkApplication ended successfully")
            return True
        else:
            self.log.info("SparkApplication is still in state: %s", application_state)
            return False
