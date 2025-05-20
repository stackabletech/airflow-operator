#!/usr/bin/env python

import requests
import time
import sys
from datetime import datetime, timezone


def exception_handler(exception_type, exception, traceback):
    print(f"{exception_type.__name__}: {exception.args}")


sys.excepthook = exception_handler


def assert_metric(role, role_group, metric):
    metric_response = requests.get(f"http://airflow-{role}-{role_group}:9102/metrics")
    assert metric_response.status_code == 200, (
        f"Metrics could not be retrieved from the {role}-{role_group}."
    )
    return metric in metric_response.text


try:
    role_group = sys.argv[1]
except IndexError:
    role_group = "default"

now = datetime.now(timezone.utc)
ts = now.strftime("%Y-%m-%dT%H:%M:%S.%f") + now.strftime("%z")

# Trigger a DAG run to create metrics
dag_id = "example_trigger_target_dag"
dag_data = {"logical_date": f"{ts}", "conf": {"message": "Hello World"}}

print(f"DAG-Data: {dag_data}")

# allow a few moments for the DAGs to be registered to all roles
time.sleep(10)

rest_url = f"http://airflow-webserver-{role_group}:8080/api/v2"
token_url = f"http://airflow-webserver-{role_group}:8080/auth/token"

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
# trigger DAG
response = requests.post(
    f"{rest_url}/dags/{dag_id}/dagRuns", headers=headers, json=dag_data
)

# Test the DAG in a loop. Each time we call the script a new job will be started: we can avoid
# or minimize this by looping over the check instead.
iterations = 4
loop = 0
while True:
    assert response.status_code == 200, "DAG run could not be triggered."
    # Wait for the metrics to be consumed by the statsd-exporter
    time.sleep(5)
    # (disable line-break flake checks)
    if (
        (assert_metric("scheduler", role_group, "airflow_scheduler_heartbeat"))
        and (
            assert_metric(
                "webserver", role_group, "airflow_task_instance_created_BashOperator"
            )
        )  # noqa: W503, W504
        and (
            assert_metric(
                "scheduler",
                role_group,
                "airflow_dagrun_duration_success_example_trigger_target_dag_count",
            )
        )
    ):  # noqa: W503, W504
        break
    time.sleep(10)
    loop += 1
    if loop == iterations:
        # force re-try of script
        sys.exit(1)
