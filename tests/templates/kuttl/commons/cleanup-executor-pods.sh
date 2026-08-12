#!/usr/bin/env sh
# Shared teardown for KubernetesExecutor tests: force-deletes leftover DAG task pods so
# that kuttl can delete the namespace within its timeout. Their Vector sidecar ignores
# SIGTERM (it is not PID 1) and would linger for the full 300s grace period.
# The proper fix is in operator-rs (making Vector PID 1 via exec).

# With --skip-delete the namespace is kept for debugging, so this must not run. kuttl does
# not expose the flag to test steps, hence looking for it in the argv of the kuttl ancestor.
pid=$(ps -o ppid= -p "$$" 2>/dev/null | tr -d '[:space:]')
while [ "${pid:-0}" -ge 1 ]; do
  args=$(ps -o args= -p "$pid" 2>/dev/null)
  case "$args" in
    *kuttl*)
      case "$args" in
        *--skip-delete*)
          echo "kuttl was started with --skip-delete, keeping the AirflowCluster"
          exit 0
          ;;
      esac
      ;;
  esac
  pid=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d '[:space:]')
done

# shellcheck disable=SC2154 # NAMESPACE is set by kuttl for test-step commands
kubectl delete airflowcluster --all -n "$NAMESPACE" --wait=false 2>/dev/null || true

if kubectl wait --for=delete pod -l app.kubernetes.io/name=airflow -n "$NAMESPACE" --timeout=120s 2>/dev/null; then
  exit 0
fi
kubectl delete pods -l app.kubernetes.io/name=airflow -n "$NAMESPACE" --grace-period=0 --force 2>/dev/null || true
kubectl wait --for=delete pod -l app.kubernetes.io/name=airflow -n "$NAMESPACE" --timeout=300s
