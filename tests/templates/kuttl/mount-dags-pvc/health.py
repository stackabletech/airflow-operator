#!/usr/bin/env python
import logging
import requests
import sys

if __name__ == "__main__":
    result = 0
    log_level = 'DEBUG'
    logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s: %(message)s', stream=sys.stdout)

    http_code = requests.get("http://airflow-webserver-default:8080/api/v1/health").status_code

    if http_code != 200:
        result = 1
    sys.exit(result)
