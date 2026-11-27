#!/usr/bin/env bash
# Runs Jupyter and/or Airflow inside a single container.
#   SERVICES=all      -> Jupyter + Airflow (default)
#   SERVICES=jupyter  -> Jupyter only (lowest RAM usage)
#   SERVICES=airflow  -> Airflow only
# Any other argument (e.g. `docker compose run lab python x.py`) is executed directly.

set -euo pipefail

if [[ $# -gt 0 ]]; then
  exec "$@"
fi

SERVICES="${SERVICES:-all}"
mkdir -p /opt/airflow/dags /workspace/notebooks /workspace/data

start_airflow() {
  echo "[lab] Waiting for the Airflow database to be ready..."
  for i in $(seq 1 30); do
    airflow db check >/dev/null 2>&1 && break
    if [[ $i -eq 30 ]]; then echo "[lab] Airflow database is not reachable:"; airflow db check; exit 1; fi
    sleep 2
  done
  airflow db migrate >/dev/null
  echo "[lab] Starting Airflow (scheduler, dag-processor, api-server) on port 8080"
  mkdir -p "$AIRFLOW_HOME/logs"
  airflow scheduler     >>"$AIRFLOW_HOME/logs/scheduler.out" 2>&1 &
  airflow dag-processor >>"$AIRFLOW_HOME/logs/dag-processor.out" 2>&1 &
  airflow api-server --port 8080 >>"$AIRFLOW_HOME/logs/api-server.out" 2>&1 &
}

start_jupyter() {
  echo "[lab] Starting JupyterLab on port 8888"
  jupyter lab --ip=0.0.0.0 --port=8888 --no-browser \
    --ServerApp.root_dir=/workspace \
    --IdentityProvider.token='' --ServerApp.password='' \
    --ServerApp.disable_check_xsrf=True &
}

case "$SERVICES" in
  all)     start_airflow; start_jupyter ;;
  jupyter) start_jupyter ;;
  airflow) start_airflow ;;
  *) echo "Unknown SERVICES value: $SERVICES (choose all|jupyter|airflow)"; exit 1 ;;
esac

echo "[lab] Ready. Jupyter: http://localhost:8888  Airflow: http://localhost:8080"
# Stop the container if one of the main processes dies
wait -n
