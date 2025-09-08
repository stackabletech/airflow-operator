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


def collect_all_sources(payload):
    # Return a flat list of every source string found in payload['content'].
    sources = []
    for entry in payload.get("content", []):
        if isinstance(entry, dict) and isinstance(entry.get("sources"), list):
            sources.extend(entry["sources"])
    return sources


def prefix_is_matched(tasks, dag_run_id_root_url, headers, required_prefix) -> bool:
    for ti in tasks["task_instances"]:
        task_id = ti["task_id"]
        try_number = ti["try_number"]
        logs = requests.get(
            f"{dag_run_id_root_url}/{task_id}/logs/{try_number}",
            headers=headers,
            params={"full_content": "true"},
        ).json()
        print(f"Logs/full-content: {logs}")
        all_sources = collect_all_sources(logs)
        if not all_sources:
            print("No 'sources' arrays were found yet in the payload...")
        else:
            # Look for *any* source that starts with the required prefix
            matching = [s for s in all_sources if s.startswith(required_prefix)]
            if matching:
                for matched in matching:
                    print(
                        f"Validation passed - at least one source starts with the required prefix: {matched}"
                    )
                    return True
            else:
                print(f"No source found yet beginning with '{required_prefix}'...")
    return False


def remote_logging() -> None:
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.%f") + now.strftime("%z")

    # Trigger a DAG run to create logs
    dag_id = "example_trigger_target_dag"
    dag_data = {"logical_date": f"{ts}", "conf": {"message": "Hello World"}}
    required_prefix = "s3://my-bucket/dag_id=example_trigger_target_dag"

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

    # get logs
    dagRuns = requests.post(
        f"{rest_url}/dags/{dag_id}/dagRuns", headers=headers, json=dag_data
    )

    iterations = 4
    loop = 0
    while True:
        # get latest logs
        response = requests.get(
            f"{rest_url}/dags/{dag_id}/dagRuns", params={"limit": 1}, headers=headers
        )
        dagRuns = response.json()
        if response.status_code == 200:
            latest_run = dagRuns["dag_runs"][0]
            dag_run_id = latest_run["dag_run_id"]
            print(f"Latest run ID: {dag_run_id}")
            dag_run_id_root_url = (
                f"{rest_url}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
            )
            tasks = requests.get(
                f"{dag_run_id_root_url}",
                headers=headers,
            ).json()
            if prefix_is_matched(tasks, dag_run_id_root_url, headers, required_prefix):
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

    parser = argparse.ArgumentParser(description="Airflow remote logging script")
    parser.add_argument("--airflow-version", type=str, help="Airflow version")
    opts = parser.parse_args()

    if opts.airflow_version and not opts.airflow_version.startswith("2."):
        remote_logging()
    else:
        # should not happen as we are using airflow-latest
        print("Remote logging is not tested for version < 3.x!")
        sys.exit(1)
