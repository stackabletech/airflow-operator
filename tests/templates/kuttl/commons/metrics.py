#!/usr/bin/env python

import requests
import time
import sys


def exception_handler(exception_type, exception, traceback):
    print(f"{exception_type.__name__}: {exception.args}")


sys.excepthook = exception_handler


def assert_metric(role, role_group, metric):
    metric_response = requests.get(
        f"http://airflow-{role}-{role_group}-metrics:9102/metrics"
    )
    assert metric_response.status_code == 200, (
        f"Metrics could not be retrieved from the {role}-{role_group}."
    )
    return metric in metric_response.text


try:
    role_group = sys.argv[1]
except IndexError:
    role_group = "default"

# Trigger a DAG run to create metrics
dag_id = "example_trigger_target_dag"
dag_conf = {"message": "Hello World"}

rest_url = f"http://airflow-webserver-{role_group}:8080/api/v1"
auth = ("airflow", "airflow")

# allow a few moments for the DAGs to be registered to all roles
time.sleep(10)

response = requests.patch(
    f"{rest_url}/dags/{dag_id}", auth=auth, json={"is_paused": False}
)
response = requests.post(
    f"{rest_url}/dags/{dag_id}/dagRuns", auth=auth, json={"conf": dag_conf}
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
