from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.utils import yaml
import os
import sys
print(sys.path)

from .stackable.spark_kubernetes_sensor import SparkKubernetesSensor  # noqa: E402
from stackable.spark_kubernetes_operator import SparkKubernetesOperator  # noqa: E402


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
