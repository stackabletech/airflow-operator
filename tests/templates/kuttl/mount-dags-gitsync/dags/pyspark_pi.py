from datetime import datetime, timedelta
from airflow import DAG
from typing import TYPE_CHECKING, Optional, Sequence, Dict
from kubernetes import client
from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils import yaml
import os
import json

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SparkKubernetesOperator(BaseOperator):
    template_fields: Sequence[str] = ('application_file', 'namespace')
    template_ext: Sequence[str] = ('.yaml', '.yml', '.json')
    ui_color = '#f4a460'

    def __init__(
            self,
            *,
            application_file: str,
            namespace: Optional[str] = None,
            kubernetes_conn_id: str = 'kubernetes_in_cluster',
            api_group: str = 'spark.stackable.tech',
            api_version: str = 'v1alpha1',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.plural = "sparkapplications"

    def execute(self, context: 'Context'):
        hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.log.info("Creating SparkApplication...")
        self.log.info(json.dumps(self.application_file, indent=4))
        response = hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=self.application_file,
            namespace=self.namespace,
        )
        return response


class SparkKubernetesSensor(BaseSensorOperator):
    template_fields = ("application_name", "namespace")
    FAILURE_STATES = ("Failed", "Unknown")
    SUCCESS_STATES = "Succeeded"

    def __init__(
            self,
            *,
            application_name: str,
            attach_log: bool = False,
            namespace: Optional[str] = None,
            kubernetes_conn_id: str = 'kubernetes_in_cluster',
            api_group: str = 'spark.stackable.tech',
            api_version: str = 'v1alpha1',
            poke_interval: float = 60,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.attach_log = attach_log
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.api_group = api_group
        self.api_version = api_version
        self.poke_interval = poke_interval

    def _log_driver(self, application_state: str, response: dict) -> None:
        if not self.attach_log:
            return
        status_info = response["status"]
        if "driverInfo" not in status_info:
            return
        driver_info = status_info["driverInfo"]
        if "podName" not in driver_info:
            return
        driver_pod_name = driver_info["podName"]
        namespace = response["metadata"]["namespace"]
        log_method = self.log.error if application_state in self.FAILURE_STATES else self.log.info
        try:
            log = ""
            for line in self.hook.get_pod_logs(driver_pod_name, namespace=namespace):
                log += line.decode()
            log_method(log)
        except client.rest.ApiException as e:
            self.log.warning(
                "Could not read logs for pod %s. It may have been disposed.\n"
                "Make sure timeToLiveSeconds is set on your SparkApplication spec.\n"
                "underlying exception: %s",
                driver_pod_name,
                e,
            )

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
            self.log.debug(f"SparkApplication status could not be established: {response}")
            return False
        if self.attach_log and application_state in self.FAILURE_STATES + self.SUCCESS_STATES:
            self._log_driver(application_state, response)
        if application_state in self.FAILURE_STATES:
            raise AirflowException(f"SparkApplication failed with state: {application_state}")
        elif application_state in self.SUCCESS_STATES:
            self.log.info("SparkApplication ended successfully")
            return True
        else:
            self.log.info("SparkApplication is still in state: %s", application_state)
            return False


with DAG(
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
    application_name = 'pyspark-pi-' + datetime.utcnow().strftime('%Y%m%d%H%M%S')
    document.update({'metadata': {'name': application_name, 'namespace': ns}})

    t1 = SparkKubernetesOperator(
        task_id='spark_pi_submit',
        namespace=ns,
        application_file=document,
        do_xcom_push=True,
        dag=dag,
    )

    t2 = SparkKubernetesSensor(
        task_id='spark_pi_monitor',
        namespace=ns,
        application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
        poke_interval=5,
        dag=dag,
    )

    t1 >> t2
