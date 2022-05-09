#!/bin/bash

# Execute tests
git clone -b "$GIT_BRANCH" https://github.com/stackabletech/airflow-operator.git
(cd airflow-operator/ && ./scripts/run_tests.sh)
exit_code=$?

# save logfiles and exit
./operator-logs.sh airflow > /target/airflow-operator.log
exit $exit_code
