#!/usr/bin/env python

import requests
import time
import sys
from datetime import datetime, timezone
import argparse
import logging


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


# Check if dag run state is "success", "queued", or "running"
# TODO: in future, we could wait on it.
# See: https://airflow.apache.org/docs/apache-airflow/3.1.6/stable-rest-api-ref.html#operation/wait_dag_run_until_finished
def assert_dag_started(rest_url, headers, dag_id, dag_run_id):
    dag_run_response = requests.get(
        f"{rest_url}/dags/{dag_id}/dagRuns/{dag_run_id}", headers=headers
    )
    dag_run_state = dag_run_response.json()["state"]
    print(f"DAG RUN STATE: {dag_run_state}")
    return (
        dag_run_state == "success"
        or dag_run_state == "queued"
        or dag_run_state == "running"
    )


def metrics_v3(role_group: str) -> None:
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.%f") + now.strftime("%z")

    # Trigger a deferrable DAG run to create metrics
    dag_id = "core_deferrable_sleep_demo"
    dag_data = {"logical_date": f"{ts}"}

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
        print(
            f"Failed to obtain access token: {response.status_code} - {response.text}"
        )
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

    dag_run_id = response.json()["dag_run_id"]

    print(f"DAG RUN ID: {dag_run_id}")

    # Test the DAG in a loop. Each time we call the script a new job will be started: we can avoid
    # or minimize this by looping over the check instead.
    iterations = 4
    loop = 0
    while True:
        assert response.status_code == 200, "DAG run could not be triggered."
        # Wait for the metrics to be consumed by the statsd-exporter
        time.sleep(5)
        heartbeat_metric = "airflow_scheduler_heartbeat"
        dag_run_success_count_metric = f"airflow_dagrun_duration_success_{dag_id}_count"
        if (
            assert_dag_started(rest_url, headers, dag_id, dag_run_id)
            and assert_metric("scheduler", role_group, heartbeat_metric)
            and assert_metric("scheduler", role_group, dag_run_success_count_metric)
        ):
            break
        time.sleep(10)
        loop += 1
        if loop == iterations:
            # force re-try of script
            sys.exit(1)


def metrics_v2(role_group: str) -> None:
    # Trigger a DAG run to create metrics
    dag_id = "core_deferrable_sleep_demo"

    rest_url = "http://airflow-webserver:8080/api/v1"
    auth = ("airflow", "airflow")

    # allow a few moments for the DAGs to be registered to all roles
    time.sleep(10)

    response = requests.patch(
        f"{rest_url}/dags/{dag_id}", auth=auth, json={"is_paused": False}
    )
    response = requests.post(
        f"{rest_url}/dags/{dag_id}/dagRuns", auth=auth, json={"conf": {}}
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
                    "webserver",
                    role_group,
                    "airflow_task_instance_created_CoreDeferrableSleepOperator",
                )
            )  # noqa: W503, W504
            and (
                assert_metric(
                    "scheduler",
                    role_group,
                    "airflow_dagrun_duration_success_core_deferrable_sleep_demo_count",
                )
            )
        ):  # noqa: W503, W504
            break
        time.sleep(10)
        loop += 1
        if loop == iterations:
            # force re-try of script
            sys.exit(1)


if __name__ == "__main__":
    log_level = "DEBUG"
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="Airflow metrics script")
    parser.add_argument(
        "--role-group", type=str, default="default", help="Role group to check"
    )
    parser.add_argument("--airflow-version", type=str, help="Airflow version")
    opts = parser.parse_args()

    if opts.airflow_version and opts.airflow_version.startswith("3"):
        metrics_v3(opts.role_group)
    else:
        metrics_v2(opts.role_group)
