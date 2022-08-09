#!/usr/bin/env bash
set -euo pipefail

# The getting started guide script
# It uses tagged regions which are included in the documentation
# https://docs.asciidoctor.org/asciidoc/latest/directives/include-tagged-regions/
#
# There are two variants to go through the guide - using stackablectl or helm
# The script takes either 'stackablectl' or 'helm' as an argument
#
# The script can be run as a test as well, to make sure that the tutorial works
# It includes some assertions throughout, and at the end especially.

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

echo "Adding bitnami Helm Chart repository and dependencies (Postgresql and Redis)"
# tag::helm-add-bitnami-deps[]
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install --wait airflow-postgresql bitnami/postgresql --version 11.0.0 \
    --set auth.username=airflow \
    --set auth.password=airflow \
    --set auth.database=airflow
helm install --wait airflow-redis bitnami/redis \
    --set auth.password=redis \
    --version 16.8.7 \
    --set replica.replicaCount=1
# end::helm-add-bitnami-deps[]

case "$1" in
"helm")
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-dev https://repo.stackable.tech/repository/helm-dev/
# end::helm-add-repo[]
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator stackable-dev/commons-operator --version 0.3.0-nightly
helm install --wait secret-operator stackable-dev/secret-operator --version 0.6.0-nightly
helm install --wait airflow-operator stackable-dev/airflow-operator --version 0.5.0-nightly
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.3.0-nightly \
  secret=0.6.0-nightly \
  airflow=0.5.0-nightly
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

echo "Waiting on AirflowDB ..."
# tag::wait-airflowdb[]
kubectl wait airflowdb/airflow \
  --for jsonpath='{.status.condition}'=Ready \
  --timeout 300s
# end::wait-airflowdb[]

sleep 5

echo "Awaiting Airflow rollout finish"
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
