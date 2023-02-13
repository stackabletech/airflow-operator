#!/usr/bin/env bash
set -euo pipefail

# This script contains all the code snippets from the guide, as well as some assert tests
# to test if the instructions in the guide work. The user *could* use it, but it is intended
# for testing only.
# The script will install the operators, create a superset instance and briefly open a port
# forward and connect to the superset instance to make sure it is up and running.
# No running processes are left behind (i.e. the port-forwarding is closed at the end)

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

echo "Adding bitnami Helm Chart repository and dependencies (Postgresql and Redis)"
# tag::helm-add-bitnami-repo[]
helm repo add bitnami https://charts.bitnami.com/bitnami
# end::helm-add-bitnami-repo[]
# tag::helm-add-bitnami-pgs[]
helm install --wait airflow-postgresql bitnami/postgresql --version 12.1.5 \
    --set auth.username=airflow \
    --set auth.password=airflow \
    --set auth.database=airflow
# end::helm-add-bitnami-pgs[]
# tag::helm-add-bitnami-redis[]
helm install --wait airflow-redis bitnami/redis \
    --set auth.password=redis \
    --version 17.3.7 \
    --set replica.replicaCount=1
# end::helm-add-bitnami-redis[]

case "$1" in
"helm")
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-dev https://repo.stackable.tech/repository/helm-dev/
# end::helm-add-repo[]
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator stackable-dev/commons-operator --version 0.5.0-nightly
helm install --wait secret-operator stackable-dev/secret-operator --version 0.7.0-nightly
helm install --wait airflow-operator stackable-dev/airflow-operator --version 0.7.0-nightly
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.5.0-nightly \
  secret=0.7.0-nightly \
  airflow=0.7.0-nightly
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

for (( i=1; i<=15; i++ ))
do
  echo "Waiting for AirflowDB to appear ..."
  if eval kubectl get airflowdb airflow; then
    break
  fi

  sleep 1
done

echo "Waiting on AirflowDB to become ready ..."
# tag::wait-airflowdb[]
kubectl wait airflowdb/airflow \
  --for jsonpath='{.status.condition}'=Ready \
  --timeout 300s
# end::wait-airflowdb[]

sleep 5

echo "Awaiting Airflow rollout finish ..."
# tag::watch-airflow-rollout[]
kubectl rollout status --watch statefulset/airflow-webserver-default
kubectl rollout status --watch statefulset/airflow-worker-default
kubectl rollout status --watch statefulset/airflow-scheduler-default
# end::watch-airflow-rollout[]

echo "Starting port-forwarding of port 8080"
# tag::port-forwarding[]
kubectl port-forward svc/airflow-webserver 8080 2>&1 >/dev/null &
# end::port-forwarding[]
PORT_FORWARD_PID=$!
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
  curl -s -XGET http://localhost:8080/api/v1/health
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
  curl -s --user airflow:airflow -H 'Content-Type:application/json' \
    -XPATCH http://localhost:8080/api/v1/dags/example_complex \
    -d '{"is_paused": false}'
  # end::enable-dag[]
}
echo "Triggering a DAG run. Enable DAG..."
enable_dag

run_dag() {
  # tag::run-dag[]
  curl -s --user airflow:airflow -H 'Content-Type:application/json' \
    -XPOST http://localhost:8080/api/v1/dags/example_complex/dagRuns \
    -d '{}' | jq -r '.dag_run_id'
  # end::run-dag[]
}

dag_id=$(run_dag)

request_dag_status() {
  # tag::check-dag[]
  curl -s --user airflow:airflow -H 'Content-Type:application/json' \
    -XGET http://localhost:8080/api/v1/dags/example_complex/dagRuns/"$dag_id" | jq -r '.state'
  # end::check-dag[]
}

while [[ "$(request_dag_status)" == "running" || "$(request_dag_status)" == "queued" ]]; do
  echo "Awaiting DAG completion ..."
  sleep 5
done

dag_state=$(request_dag_status)

echo "Checking DAG result ..."
if [ "$dag_state" == "success" ]; then
  echo "DAG run successful for ID: " "$dag_id"
else
  echo "The DAG was not successful. State: " "$dag_state"
  exit 1
fi