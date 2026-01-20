#!/usr/bin/env python

import requests
import time
import sys
import logging
from datetime import datetime, timezone


def assert_metric(role, metric):
    metric_response = requests.get(
        f"http://airflow-{role}-default-metrics:9102/metrics"
    )
    assert metric_response.status_code == 200, (
        f"Metrics could not be retrieved from the {role}."
    )
    return metric in metric_response.text


now = datetime.now(timezone.utc)
ts = now.strftime("%Y-%m-%dT%H:%M:%S.%f") + now.strftime("%z")

# Trigger a DAG run to create metrics
dag_id = "sparkapp_dag"
dag_data = {"logical_date": f"{ts}"}

print(f"DAG-Data: {dag_data}")

# allow a few moments for the DAGs to be registered to all roles
time.sleep(10)

rest_url = "http://airflow-webserver:8080/api/v2"
token_url = "http://airflow-webserver:8080/auth/token"

data = {"username": "airflow", "password": "airflow"}

headers = {"Content-Type": "application/json"}

response = requests.post(token_url, headers=headers, json=data)

if response.status_code == 200 or response.status_code == 201:
    token_data = response.json()
    access_token = token_data["access_token"]
    print(f"Access Token: {access_token}")
else:
    print(f"Failed to obtain access token: {response.status_code} - {response.text}")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

# activate DAG
response = requests.patch(
    f"{rest_url}/dags/{dag_id}", headers=headers, json={"is_paused": False}
)
if response.status_code != 200:
    logging.error("DAG run could not be activated")
    sys.exit(1)

# trigger DAG
response = requests.post(
    f"{rest_url}/dags/{dag_id}/dagRuns", headers=headers, json=dag_data
)
if response.status_code != 200:
    logging.error("DAG run could not be triggered")
    sys.exit(1)

# Wait for the metrics to be consumed by the statsd-exporter
time.sleep(5)
# Test the DAG in a loop. Each time we call the script a new job will be started: we can avoid
# or minimize this by looping over the check instead.
iterations = 9
loop = 0
while True:
    try:
        # Worker is not deployed with the kubernetes executor so retrieve success metric from scheduler
        # (disable line-break flake checks)
        if (assert_metric("scheduler", "airflow_scheduler_heartbeat")) and (
            assert_metric(
                "scheduler", "airflow_dagrun_duration_success_sparkapp_dag_count"
            )
        ):  # noqa: W503, W504
            break
        time.sleep(10)
        loop += 1
        if loop == iterations:
            logging.error("Still waiting for metrics. Exiting to start a new loop....")
            # force re-try of script
            sys.exit(1)
    except AssertionError as error:
        logging.warning(f"Encountered: {error}")
