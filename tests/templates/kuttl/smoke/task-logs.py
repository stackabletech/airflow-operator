#!/usr/bin/env python
"""Assert that a task instance's log can be read back through the api-server.

This guards against writer/reader disagreeing about where task logs live: from Airflow 3.1 on
the Task SDK writes them to `[logging] base_log_folder` (from `airflow.cfg`), while the
api-server and the worker's log server resolve them through
`LOGGING_CONFIG['handlers']['task']['base_log_folder']` of the custom logging config. If the two
point at different directories, every log page in the UI reports the log as missing even though
the task ran and wrote its log.

The calling test step only runs this for the CeleryExecutor: KubernetesExecutor task Pods are
deleted once the task finishes, so their log server is gone and the api-server cannot read the log
back regardless of where it was written (that case needs remote logging, see the `remote-logging`
test).
"""

import argparse
import sys
import time

import requests

DAG_ID = "example_bash_operator"
TASK_ID = "runme_0"

REST_URL = "http://airflow-webserver:8080/api/v2"
TOKEN_URL = "http://airflow-webserver:8080/auth/token"

# What the api-server returns instead of the log when it cannot find the file. The wording is
# checked as a substring because it is followed by the worker's host name.
LOG_NOT_FOUND = "Log file not found"

# A successful `runme_0` run produces far more than this; the failure mode produces none at all.
MIN_LOG_LINES = 3


def get_token() -> str:
    response = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/json"},
        json={"username": "airflow", "password": "airflow"},
    )
    response.raise_for_status()
    return response.json()["access_token"]


def wait_for_dag(headers, timeout: int = 120) -> None:
    """Wait until the DAG processor has registered the example DAGs.

    This is run once (see the calling TestStep), so it cannot rely on being retried.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if (
            requests.get(f"{REST_URL}/dags/{DAG_ID}", headers=headers).status_code
            == 200
        ):
            return
        time.sleep(5)
    sys.exit(f"{DAG_ID} was not registered within {timeout}s")


def trigger_dag(headers) -> str:
    requests.patch(
        f"{REST_URL}/dags/{DAG_ID}", headers=headers, json={"is_paused": False}
    ).raise_for_status()

    # An empty body is rejected with 422; `logical_date: null` triggers a run "now".
    response = requests.post(
        f"{REST_URL}/dags/{DAG_ID}/dagRuns",
        headers=headers,
        json={"logical_date": None},
    )
    response.raise_for_status()
    return response.json()["dag_run_id"]


def wait_for_task_instance(headers, dag_run_id: str, timeout: int = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = requests.get(
            f"{REST_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{TASK_ID}",
            headers=headers,
        )
        if response.status_code == 200:
            task_instance = response.json()
            print(
                f"{TASK_ID}: state={task_instance['state']} host={task_instance['hostname']}"
            )
            if task_instance["state"] == "success":
                return
            if task_instance["state"] in ("failed", "upstream_failed", "skipped"):
                sys.exit(f"{TASK_ID} ended up in state {task_instance['state']}")
        time.sleep(5)
    sys.exit(f"{TASK_ID} did not succeed within {timeout}s")


def log_lines(payload) -> tuple[list, str]:
    """Return the structured log lines and the whole payload rendered as text.

    The shape of `content` changed over the 3.x line (plain text, list of strings, list of
    structured messages), so both are derived defensively. Only actual log lines carry a
    timestamp; the "Log message source details" group and error messages do not.
    """
    content = payload["content"]
    if isinstance(content, str):
        return [], content

    lines = []
    text = []
    for entry in content:
        if isinstance(entry, dict):
            text.extend(str(value) for value in entry.values())
            if entry.get("timestamp"):
                lines.append(entry)
        else:
            text.append(str(entry))
    return lines, "\n".join(text)


def sources(payload) -> list[str]:
    """Return the log locations the api-server reports for this attempt (if any)."""
    content = payload["content"]
    if isinstance(content, str):
        return []

    result = []
    in_group = False
    for entry in content:
        event = entry.get("event", "") if isinstance(entry, dict) else str(entry)
        if event == "::group::Log message source details":
            in_group = True
        elif event == "::endgroup::":
            in_group = False
        elif in_group:
            result.append(event)
    return result


def assert_log_is_readable(headers, dag_run_id: str) -> None:
    response = requests.get(
        f"{REST_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{TASK_ID}/logs/1",
        headers=headers,
        params={"full_content": "true"},
    )
    response.raise_for_status()
    payload = response.json()
    lines, text = log_lines(payload)

    # The api-server reports where it read the log from in a "Log message source details" group.
    for source in sources(payload):
        print(f"Log source: {source}")

    if LOG_NOT_FOUND in text:
        print(f"Log response: {text}")
        sys.exit(
            f"The api-server cannot read back the log of {DAG_ID}.{TASK_ID}. The task ran and "
            "wrote its log, so writer and reader disagree about the log directory: check that "
            "the task handler's 'base_log_folder' in log_config.py matches "
            "'[logging] base_log_folder' from airflow.cfg."
        )
    if len(lines) < MIN_LOG_LINES:
        sys.exit(f"Expected at least {MIN_LOG_LINES} log lines, got {len(lines)}")

    print(f"Read back {len(lines)} log lines for {DAG_ID}.{TASK_ID}")


def main(airflow_version: str) -> None:
    if airflow_version.startswith("2."):
        # Airflow 2 serves task logs through a different API; not covered here.
        print(f"Skipping: not applicable to Airflow {airflow_version}")
        return

    headers = {
        "Authorization": f"Bearer {get_token()}",
        "Content-Type": "application/json",
    }

    wait_for_dag(headers)

    dag_run_id = trigger_dag(headers)
    print(f"Triggered {DAG_ID}: {dag_run_id}")

    wait_for_task_instance(headers, dag_run_id)
    assert_log_is_readable(headers, dag_run_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Airflow task log retrieval test")
    parser.add_argument(
        "--airflow-version", type=str, required=True, help="Airflow version"
    )
    opts = parser.parse_args()

    main(opts.airflow_version)
