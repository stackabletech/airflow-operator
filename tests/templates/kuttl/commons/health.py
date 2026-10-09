#!/usr/bin/env python
import logging
import requests
import sys
import time
import argparse

# Every component of the health endpoint reports its heartbeat under a key named
# after itself, e.g. `latest_scheduler_heartbeat` for the `scheduler`. The
# exception is `metadatabase`, which reports a status only.
HEARTBEAT_KEY = "latest_{}_heartbeat".format


def health_url(airflow_version):
    """Return the health endpoint of the given Airflow version."""
    if airflow_version and airflow_version.startswith("3"):
        return "http://airflow-webserver:8080/api/v2/monitor/health"
    else:
        return "http://airflow-webserver:8080/api/v1/health"


def component_states(health, components):
    """Return the (status, heartbeat) pair of every requested component."""
    return {
        name: (health[name]["status"], health[name].get(HEARTBEAT_KEY(name)))
        for name in components
    }


def all_healthy(states, baseline):
    """Check that every component is healthy.

    If a baseline was taken, every component must also have sent a heartbeat
    since, i.e. one that differs from the baseline.
    """
    return all(
        status == "healthy" and (baseline is None or heartbeat != baseline[name])
        for name, (status, heartbeat) in states.items()
    )


if __name__ == "__main__":
    log_level = "DEBUG"
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="Health check script")
    parser.add_argument("--airflow-version", type=str, help="Airflow version")
    parser.add_argument(
        "--component",
        action="append",
        default=[],
        metavar="NAME",
        help="Require this component to report healthy, e.g. `scheduler`. "
        "Can be repeated. Without it, a reachable endpoint is enough.",
    )
    parser.add_argument(
        "--heartbeat-changed",
        action="store_true",
        help="Additionally require every --component to send a heartbeat that "
        "differs from the first one observed. Use this to assert that a "
        "component recovered, rather than that it was healthy at some point.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=0,
        help="Give up after this many seconds. The default of 0 retries until "
        "the caller times out.",
    )
    opts = parser.parse_args()

    url = health_url(opts.airflow_version)
    deadline = time.monotonic() + opts.timeout if opts.timeout else None
    baseline = None

    count = 0

    while True:
        try:
            count = count + 1
            res = requests.get(url, timeout=5)
            code = res.status_code
            if code == 200:
                states = component_states(res.json(), opts.component)

                if opts.heartbeat_changed and baseline is None:
                    baseline = {name: beat for name, (_, beat) in states.items()}
                    print(f"Heartbeats to be superseded {baseline} ....")
                elif all_healthy(states, baseline):
                    break
                else:
                    print(
                        f"Components are not (yet) healthy {states}, retrying attempt no [{count}] ...."
                    )
            else:
                print(
                    f"Got non 200 status code [{code}], retrying attempt no [{count}] ...."
                )
        except requests.exceptions.Timeout:
            print(f"Connection timed out, retrying attempt no [{count}] ....")
        except requests.ConnectionError as e:
            print(f"Connection Error: {str(e)}")
        except requests.RequestException as e:
            print(f"General Error: {str(e)}")
        except Exception as e:
            print(
                f"General error occurred {str(e)}, retrying attempt no [{count}] ...."
            )

        if deadline and time.monotonic() > deadline:
            sys.exit(
                f"The health check did not succeed within {opts.timeout} seconds, "
                "see the attempts above."
            )

        # Wait a little bit before retrying
        time.sleep(1)
    sys.exit(0)
