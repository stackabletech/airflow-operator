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
dag_id_0 = "sparkapp_dag_0"
dag_id_1 = "sparkapp_dag_1"
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

# Two DAGs from two different repos/branches will need to be triggered and activated
# activate DAG
response_0 = requests.patch(
    f"{rest_url}/dags/{dag_id_0}", headers=headers, json={"is_paused": False}
)
response_1 = requests.patch(
    f"{rest_url}/dags/{dag_id_1}", headers=headers, json={"is_paused": False}
)
# trigger DAG
response_0 = requests.post(
    f"{rest_url}/dags/{dag_id_0}/dagRuns", headers=headers, json=dag_data
)
response_1 = requests.post(
    f"{rest_url}/dags/{dag_id_1}/dagRuns", headers=headers, json=dag_data
)

# Wait for the metrics to be consumed by the statsd-exporter
time.sleep(5)
# Test the DAG in a loop. Each time we call the script a new job will be started: we can avoid
# or minimize this by looping over the check instead.
iterations = 9
loop = 0
while True:
    try:
        logging.info(f"Response code: {response.status_code}")
        assert response_0.status_code == 200, (
            f"DAG from git-sync 0 run could not be triggered because of status code: {response_0.status_code}"
        )
        assert response_1.status_code == 200, (
            f"DAG from git-sync 1 run could not be triggered. {response_1.status_code}"
        )
        # Worker is not deployed with the kubernetes executor so retrieve success metric from scheduler
        # (disable line-break flake checks)
        if (
            (assert_metric("scheduler", "airflow_scheduler_heartbeat"))
            and (
                assert_metric(
                    "scheduler", "airflow_dagrun_duration_success_sparkapp_dag_0_count"
                )
            )
            and (
                assert_metric(
                    "scheduler", "airflow_dagrun_duration_success_sparkapp_dag_1_count"
                )
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
