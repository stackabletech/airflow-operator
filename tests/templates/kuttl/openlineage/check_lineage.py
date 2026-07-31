#!/usr/bin/env python
"""Trigger a DAG run and assert that OpenLineage events reached the test receiver.

Runs inside the `test-airflow-python` pod. Triggers `openlineage_test_dag`, then polls the
test receiver's `/events` endpoint until it has captured lineage events for that DAG,
including a terminal `COMPLETE` event.
"""

import argparse
import sys
import time
from datetime import datetime, timezone

import requests
import urllib3

DAG_ID = "openlineage_test_dag"


def get_token(base_url: str) -> str:
    resp = requests.post(
        f"{base_url}/auth/token",
        headers={"Content-Type": "application/json"},
        json={"username": "airflow", "password": "airflow"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def trigger_dag(base_url: str, headers: dict) -> bool:
    # Unpause the DAG, then trigger a run. Mirrors the working commons/metrics.py trigger: the
    # Airflow 3 API expects an ISO logical_date (a null value can be rejected with a 422).
    # Returns True only if the run was actually accepted (2xx); the DAG may not be registered yet
    # (ConfigMap mount + dag-processor parse takes time), in which case the caller must retry.
    now = datetime.now(timezone.utc)
    logical_date = now.strftime("%Y-%m-%dT%H:%M:%S.%f") + now.strftime("%z")
    patch_resp = requests.patch(
        f"{base_url}/api/v2/dags/{DAG_ID}",
        headers=headers,
        json={"is_paused": False},
        timeout=10,
    )
    if patch_resp.status_code >= 300:
        print(f"Unpause not ready yet: {patch_resp.status_code} {patch_resp.text}")
        return False
    resp = requests.post(
        f"{base_url}/api/v2/dags/{DAG_ID}/dagRuns",
        headers=headers,
        json={"logical_date": logical_date, "conf": {}},
        timeout=10,
    )
    print(f"Trigger response: {resp.status_code} {resp.text}")
    return resp.status_code < 300


def receiver_has_lineage(receiver_url: str) -> bool:
    resp = requests.get(f"{receiver_url}/events", timeout=10, verify=False)
    if resp.status_code != 200:
        return False
    body = resp.text
    return DAG_ID in body and "COMPLETE" in body


if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    parser = argparse.ArgumentParser(description="OpenLineage lineage check")
    parser.add_argument(
        "--webserver-url",
        default="http://airflow-webserver-default-headless:8080",
    )
    parser.add_argument(
        "--receiver-url",
        required=True,
        help="Base URL of the OpenLineage test receiver, e.g. http://openlineage-receiver:5000",
    )
    opts = parser.parse_args()

    # Allow a few moments for the DAG to be registered on all roles.
    time.sleep(15)

    triggered = False
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            if not triggered:
                token = get_token(opts.webserver_url)
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                }
                # Keep retrying until the run is actually accepted - the DAG may not be
                # registered yet on the first attempts.
                triggered = trigger_dag(opts.webserver_url, headers)

            if triggered and receiver_has_lineage(opts.receiver_url):
                print("OpenLineage events for the DAG were received.")
                sys.exit(0)
        except Exception as e:  # noqa: BLE001
            print(f"Retrying after error: {e}")

        time.sleep(10)

    print("Timed out waiting for OpenLineage events at the receiver.")
    sys.exit(1)
