#!/usr/bin/env bash
set -euo pipefail

ITERATIONS="${1:-24}"

poll_run() {
  local run_id="$1"
  while true; do
    state=$(docker compose exec -T airflow airflow dags state electricity_region_data_backfill "$run_id" | tr -d '\r')
    echo "run ${run_id} state=${state}"
    if [[ "$state" == "success" ]]; then
      return 0
    fi
    if [[ "$state" == "failed" ]]; then
      return 1
    fi
    sleep 15
  done
}

echo "[1/3] Starting containers..."
docker compose up -d --build

echo "[2/3] Applying warehouse SQL on the running Postgres instance..."
bash scripts/apply_warehouse_sql.sh

echo "[3/3] Triggering ${ITERATIONS} backfill run(s)..."
for iteration in $(seq 1 "${ITERATIONS}"); do
  echo "Backfill iteration ${iteration}"
  trigger_output=$(docker compose exec -T airflow airflow dags trigger electricity_region_data_backfill)
  echo "$trigger_output"
  run_id=$(echo "$trigger_output" | awk -F'|' '/electricity_region_data_backfill/ {gsub(/^ +| +$/, "", $3); print $3}')
  if [[ -z "$run_id" ]]; then
    echo "Could not determine run_id from trigger output"
    exit 1
  fi
  poll_run "$run_id"
done
