"""
Regression test for OPA-based DAG authorization on Airflow 3.

The OPA rules in 31-opa-rules.yaml grant jane.doe access to everything and
grant john.doe nothing. john.doe is also FAB Admin, so if OPA is not
consulted he would inherit full access from FAB. Both the direct-resource
endpoint and the listing endpoint must therefore go through OPA:

  - jane.doe: GET /api/v2/dags/example_trigger_target_dag -> 200
  - jane.doe: GET /api/v2/dags                            -> 200, contains DAG
  - john.doe: both endpoints deny / return empty

The assertions retry for a short window to absorb OPA bundle-puller
latency (Stackable OPA reloads ConfigMap-backed rules on an interval,
not instantly).
"""

import logging
import sys
import time

import requests

logging.basicConfig(
    level="INFO", format="%(levelname)s: %(message)s", stream=sys.stdout
)
log = logging.getLogger(__name__)

URL = "http://localhost:8080"
API = f"{URL}/api/v2"

USER = {"username": "jane.doe", "password": "T8mn72D9"}
OTHER_USER = {"username": "john.doe", "password": "T8mn72D9"}
DAG_ID = "example_trigger_target_dag"

RETRY_TIMEOUT_SECONDS = 60
RETRY_INTERVAL_SECONDS = 3


def obtain_access_token(user):
    response = requests.post(
        f"{URL}/auth/token",
        headers={"Content-Type": "application/json"},
        json={"username": user["username"], "password": user["password"]},
    )
    if response.status_code not in (200, 201):
        log.error(
            "Failed to obtain access token: %s - %s",
            response.status_code,
            response.text,
        )
        sys.exit(1)
    log.info("Got access token for %s", user["username"])
    return response.json()["access_token"]


def retry(label, check):
    """Run `check()` until it returns None (= success) or timeout.

    `check` returns None on pass and an AssertionError instance on fail.
    On timeout the most recent failure is raised.
    """
    deadline = time.monotonic() + RETRY_TIMEOUT_SECONDS
    last_failure = None
    attempt = 0
    while True:
        attempt += 1
        result = check()
        if result is None:
            log.info("%s: ok (attempt %d)", label, attempt)
            return
        last_failure = result
        if time.monotonic() >= deadline:
            log.info("%s: giving up after %d attempts", label, attempt)
            raise last_failure
        log.info(
            "%s: not ready yet (attempt %d), retrying in %ds",
            label,
            attempt,
            RETRY_INTERVAL_SECONDS,
        )
        time.sleep(RETRY_INTERVAL_SECONDS)


def assert_other_user_has_no_access():
    """Negative control: john.doe is FAB Admin but has no OPA rules.

    If OPA is consulted, both endpoints default-deny. If OPA were bypassed
    and FAB took over, Admin would see everything - which would make
    jane.doe's assertions meaningless.
    """
    token = obtain_access_token(OTHER_USER)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    direct = requests.get(f"{API}/dags/{DAG_ID}", headers=headers)
    log.info(
        "negative control: GET /api/v2/dags/{id} for %s -> %s",
        OTHER_USER["username"],
        direct.status_code,
    )
    if direct.status_code == 200:
        raise AssertionError(
            f"OPA enforcement broken: GET /api/v2/dags/{DAG_ID} returned 200 "
            f"for {OTHER_USER['username']} (Admin, no OPA rules). OPA should "
            f"have default-denied this request."
        )

    listing = requests.get(f"{API}/dags", params={"limit": 1000}, headers=headers)
    log.info(
        "negative control: GET /api/v2/dags for %s -> %s",
        OTHER_USER["username"],
        listing.status_code,
    )
    if listing.status_code == 200:
        dag_ids = [d["dag_id"] for d in listing.json().get("dags", [])]
        if dag_ids:
            raise AssertionError(
                f"OPA enforcement broken: GET /api/v2/dags returned {dag_ids} "
                f"for {OTHER_USER['username']} (Admin, no OPA rules). OPA "
                f"should have filtered to empty or denied outright."
            )


def main():
    assert_other_user_has_no_access()

    token = obtain_access_token(USER)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def check_direct():
        r = requests.get(f"{API}/dags/{DAG_ID}", headers=headers)
        if r.status_code == 200:
            return None
        return AssertionError(
            f"GET /api/v2/dags/{DAG_ID} returned {r.status_code} for "
            f"{USER['username']}, even though OPA allows it.\n"
            f"Response: {r.text}"
        )

    def check_listing():
        # dag_id_pattern + a wide limit so we don't depend on Airflow's
        # default page size (50 in Airflow 3 - example_trigger_target_dag
        # alphabetically falls past the first page in a default install).
        r = requests.get(
            f"{API}/dags",
            params={"dag_id_pattern": DAG_ID, "limit": 1000},
            headers=headers,
        )
        if r.status_code != 200:
            return AssertionError(
                f"GET /api/v2/dags returned {r.status_code} for "
                f"{USER['username']}, even though OPA allows it.\n"
                f"Response: {r.text}"
            )
        dag_ids = sorted(d["dag_id"] for d in r.json().get("dags", []))
        if DAG_ID not in dag_ids:
            return AssertionError(
                f"GET /api/v2/dags returned {dag_ids} for "
                f"{USER['username']}. OPA allows {DAG_ID} but the listing "
                f"path filtered it out."
            )
        return None

    retry("GET /api/v2/dags/{id}", check_direct)
    retry("GET /api/v2/dags", check_listing)

    log.info("All assertions passed.")


main()
