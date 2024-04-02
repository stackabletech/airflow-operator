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

"""Example DAG demonstrating how to apply a Kubernetes Resource from Airflow running in-cluster"""
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.utils import yaml
import os
from stackable.spark_kubernetes_sensor import SparkKubernetesSensor
from stackable.spark_kubernetes_operator import SparkKubernetesOperator


with DAG(  # <4>
        dag_id='sparkapp_dag',
        schedule_interval=None,
        start_date=datetime(2022, 1, 1),
        catchup=False,
        dagrun_timeout=timedelta(minutes=60),
        tags=['example'],
        params={},
) as dag:

    def load_body_to_dict(body):
        try:
            body_dict = yaml.safe_load(body)
        except yaml.YAMLError as e:
            raise AirflowException(f"Exception when loading resource definition: {e}\n")
        return body_dict

    yaml_path = os.path.join(os.environ.get('AIRFLOW__CORE__DAGS_FOLDER'), 'pyspark_pi.yaml')

    with open(yaml_path, 'r') as file:
        crd = file.read()
    with open('/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as file:
        ns = file.read()

    document = load_body_to_dict(crd)
    application_name = 'pyspark-pi-' + datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    document.update({'metadata': {'name': application_name, 'namespace': ns}})

    t1 = SparkKubernetesOperator(  # <5>
        task_id='spark_pi_submit',
        namespace=ns,
        application_file=document,
        do_xcom_push=True,
        dag=dag,
    )

    t2 = SparkKubernetesSensor(  # <6>
        task_id='spark_pi_monitor',
        namespace=ns,
        application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
        poke_interval=5,
        dag=dag,
    )

    t1 >> t2  # <7>
