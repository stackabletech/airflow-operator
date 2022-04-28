#!/usr/bin/env python

import requests
import time


def assert_metric(role, metric):
    response = requests.get(f'http://airflow-{role}-default:9102/metrics')
    assert response.status_code == 200, \
        f"Metrics could not be retrieved from the {role}."
    assert metric in response.text, \
        f"The {role} metrics do not contain the metric {metric}."


# Trigger a DAG run to create metrics
dag_id = 'example_trigger_target_dag'
dag_conf = {'message': "Hello World"}

rest_url = 'http://airflow-webserver-default:8080/api/v1'
auth = ('airflow', 'airflow')

response = requests.patch(f'{rest_url}/dags/{dag_id}', auth=auth, json={'is_paused': False})
response = requests.post(f'{rest_url}/dags/{dag_id}/dagRuns', auth=auth, json={'conf': dag_conf})

assert response.status_code == 200, "DAG run could not be triggered."

# Wait for the metrics to be consumed by the statsd-exporter
time.sleep(2)

assert_metric('scheduler', 'airflow_scheduler_heartbeat')
assert_metric('webserver', 'airflow_task_instance_created_BashOperator')
assert_metric('worker', 'airflow_ti_successes')
