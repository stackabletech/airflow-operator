#!/usr/bin/env python
import logging
import requests
import sys
import time
import argparse

if __name__ == "__main__":
    log_level = "DEBUG"
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="Health check script")
    parser.add_argument("--role-group", type=str, default="default", help="Role group to check")
    parser.add_argument("--airflow-version", type=str, help="Airflow version")
    opts = parser.parse_args()

    url = f"http://airflow-webserver-{opts.role_group}:8080/api/v1/health"
    if opts.airflow_version and opts.airflow_version.startswith("3"):
        url = f"http://airflow-webserver-{opts.role_group}:8080/api/v2/monitor/health"

    count = 0

    while True:
        try:
            count = count + 1
            res = requests.get(url, timeout=5)
            code = res.status_code
            if code == 200:
                break
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

        # Wait a little bit before retrying
        time.sleep(1)
    sys.exit(0)
