#!/usr/bin/env python

import requests
import time
import sys
import logging


def assert_metric(role, metric):
    metric_response = requests.get(f'http://airflow-{role}-default:9102/metrics')
    assert metric_response.status_code == 200, \
        f"Metrics could not be retrieved from the {role}."
    return metric in metric_response.text


# Trigger a DAG run to create metrics
dag_id = 'sparkapp_dag'

rest_url = 'http://airflow-webserver-default:8080/api/v1'
auth = ('airflow', 'airflow')

# allow a few moments for the DAGs to be registered to all roles
time.sleep(10)

response = requests.patch(f'{rest_url}/dags/{dag_id}', auth=auth, json={'is_paused': False})
response = requests.post(f'{rest_url}/dags/{dag_id}/dagRuns', auth=auth, json={})

# Wait for the metrics to be consumed by the statsd-exporter
time.sleep(5)
# Test the DAG in a loop. Each time we call the script a new job will be started: we can avoid
# or minimize this by looping over the check instead.
iterations = 9
loop = 0
while True:
    try:
        logging.info(f"Response code: {response.status_code}")
        assert response.status_code == 200, "DAG run could not be triggered."
        # Worker is not deployed with the kubernetes executor so retrieve success metric from scheduler
        # (disable line-break flake checks)
        if ((assert_metric('scheduler', 'airflow_scheduler_heartbeat'))
                and (assert_metric('scheduler', 'airflow_dagrun_duration_success_sparkapp_dag_count'))):  # noqa: W503, W504
            break
        time.sleep(10)
        loop += 1
        if loop == iterations:
            logging.error("Still waiting for metrics. Exiting to start a new loop....")
            # force re-try of script
            sys.exit(1)
    except AssertionError as error:
        logging.warning(f"Encountered: {error}")
