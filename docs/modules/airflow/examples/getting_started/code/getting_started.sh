#!/usr/bin/env bash
set -euo pipefail

# DO NOT EDIT THE SCRIPT
# Instead, update the j2 template, and regenerate it for dev with `make render-docs`.

# This script contains all the code snippets from the guide, as well as some assert tests
# to test if the instructions in the guide work. The user *could* use it, but it is intended
# for testing only.
# The script will install the operators, create an airflow instance and briefly open a port
# forward and connect to the airflow instance to make sure it is up and running.
# No running processes are left behind (i.e. the port-forwarding is closed at the end)

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

echo "Adding bitnami Helm Chart repository and dependencies (Postgresql and Redis)"
# tag::helm-add-bitnami-pgs[]
helm install airflow-postgresql oci://registry-1.docker.io/bitnamicharts/postgresql \
  --version 16.5.0 \
  --set image.repository=bitnamilegacy/postgresql \
  --set volumePermissions.image.repository=bitnamilegacy/os-shell \
  --set metrics.image.repository=bitnamilegacy/postgres-exporter \
  --set global.security.allowInsecureImages=true \
  --set auth.database=airflow \
  --set auth.username=airflow \
  --set auth.password=airflow \
  --wait
# end::helm-add-bitnami-pgs[]
# tag::helm-add-bitnami-redis[]
helm install airflow-redis oci://registry-1.docker.io/bitnamicharts/redis \
  --version 20.11.3 \
  --set global.security.allowInsecureImages=true \
  --set image.repository=bitnamilegacy/redis \
  --set sentinel.image.repository=bitnamilegacy/redis-sentinel \
  --set metrics.image.repository=bitnamilegacy/redis-exporter \
  --set volumePermissions.image.repository=bitnamilegacy/os-shell \
  --set kubectl.image.repository=bitnamilegacy/kubectl \
  --set sysctl.image.repository=bitnamilegacy/os-shell \
  --set replica.replicaCount=1 \
  --set auth.password=redis \
  --wait
# end::helm-add-bitnami-redis[]

case "$1" in
"helm")
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator oci://oci.stackable.tech/sdp-charts/commons-operator --version 25.11.0-rc1
helm install --wait secret-operator oci://oci.stackable.tech/sdp-charts/secret-operator --version 25.11.0-rc1
helm install --wait listener-operator oci://oci.stackable.tech/sdp-charts/listener-operator --version 25.11.0-rc1
helm install --wait airflow-operator oci://oci.stackable.tech/sdp-charts/airflow-operator --version 25.11.0-rc1
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=25.11.0-rc1 \
  secret=25.11.0-rc1 \
  listener=25.11.0-rc1 \
  airflow=25.11.0-rc1
# end::stackablectl-install-operators[]
;;
*)
echo "Need to give 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Creating credentials secret"
# tag::apply-airflow-credentials[]
kubectl apply -f airflow-credentials.yaml
# end::apply-airflow-credentials[]

echo "Creating Airflow cluster"
# tag::install-airflow[]
kubectl apply -f airflow.yaml
# end::install-airflow[]

sleep 5

echo "Awaiting Airflow rollout finish ..."
# tag::watch-airflow-rollout[]
kubectl rollout status --watch --timeout=5m statefulset/airflow-webserver-default
kubectl rollout status --watch --timeout=5m statefulset/airflow-worker-default
kubectl rollout status --watch --timeout=5m statefulset/airflow-scheduler-default
kubectl rollout status --watch --timeout=5m statefulset/airflow-dagprocessor-default
kubectl rollout status --watch --timeout=5m statefulset/airflow-triggerer-default
# end::watch-airflow-rollout[]

echo "Starting port-forwarding of port 8080"
# shellcheck disable=2069 # we want all output to be blackholed
# tag::port-forwarding[]
kubectl port-forward svc/airflow-webserver 8080 2>&1 >/dev/null &
# end::port-forwarding[]
PORT_FORWARD_PID=$!
# shellcheck disable=2064 # we want the PID evaluated now, not at the time the trap is called
trap "kill $PORT_FORWARD_PID" EXIT
sleep 5

echo "Checking if web interface is reachable ..."
return_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/login/)
if [ "$return_code" == 200 ]; then
  echo "Webserver UI reachable!"
else
  echo "Could not reach Webserver UI."
  exit 1
fi

sleep 5

server_health() {
  # tag::server-health[]
  curl -s -XGET http://localhost:8080/api/v2/monitor/health
  # end::server-health[]
}

echo "Checking webserver health ..."
health=$(server_health | jq -r '.scheduler.status')
if [ "$health" == "healthy" ]; then
  echo "We have a healthy webserver!"
else
  echo "Webserver does not have the expected status. Detected status: " "$health"
  exit 1
fi

enable_dag() {
  # tag::enable-dag[]
  ACCESS_TOKEN=$(
    curl -s -XPOST http://localhost:8080/auth/token \
    -H 'Content-Type: application/json' \
        -d '{
        "username": "airflow",
        "password": "airflow"
        }' | jq '.access_token' | tr -d '"'
  )
  curl -s -H "Authorization: Bearer $ACCESS_TOKEN" -H "Content-Type: application/json" \
  -XPATCH http://localhost:8080/api/v2/dags/example_trigger_target_dag \
  -d '{"is_paused": false}' | jq '.is_paused'
  # end::enable-dag[]
}
SLEEP_SECONDS=10
echo "Sleeping for $SLEEP_SECONDS seconds to wait for the DAG to be registered"
sleep "$SLEEP_SECONDS"
echo "Triggering a DAG run. Enable DAG..."
paused=$(enable_dag)
echo "DAG paused: $paused"

run_dag() {
  # tag::run-dag[]
  ACCESS_TOKEN=$(
    curl -s -XPOST http://localhost:8080/auth/token \
    -H 'Content-Type: application/json' \
        -d '{
        "username": "airflow",
        "password": "airflow"
        }' | jq '.access_token' | tr -d '"'
  )
  curl -s -H "Authorization: Bearer $ACCESS_TOKEN" -H "Content-Type: application/json" \
  -XPOST http://localhost:8080/api/v2/dags/example_trigger_target_dag/dagRuns \
    -d '{"logical_date": null,"conf": {"message": "Hello World"}}' | jq -r '.dag_run_id'
  # end::run-dag[]
}

dag_id=$(run_dag)

request_dag_status() {
  # tag::check-dag[]
  ACCESS_TOKEN=$(
    curl -s -XPOST http://localhost:8080/auth/token \
    -H 'Content-Type: application/json' \
        -d '{
        "username": "airflow",
        "password": "airflow"
        }' | jq '.access_token' | tr -d '"'
  )
  curl -s -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type:application/json' \
    -XGET http://localhost:8080/api/v2/dags/example_trigger_target_dag/dagRuns/"$dag_id" | jq -r '.state'
  # end::check-dag[]
}

dag_state=$(request_dag_status)

while [[ "$dag_state" == "running" || "$dag_state" == "queued" ]]; do
  echo "Awaiting DAG completion ..."
  sleep 5
  dag_state=$(request_dag_status)
done

echo "Checking DAG result ..."
if [ "$dag_state" == "success" ]; then
  echo "DAG run successful for ID: $dag_id"
else
  echo "The DAG was not successful. State: $dag_state"
  exit 1
fi
