#!/usr/bin/env bash
set -euo pipefail

RUN_DATE="${1:-$(date -u +"%Y-%m-%dT%H:%M:%S")}" 

echo "[1/4] Starting containers..."
docker compose up -d --build

echo "[2/4] Applying warehouse SQL on the running Postgres instance..."
bash scripts/apply_warehouse_sql.sh

echo "[3/4] Running Airflow DAG test for ${RUN_DATE} ..."
docker compose exec --user airflow airflow airflow dags test electricity_region_data_incremental "${RUN_DATE}"

echo "[4/4] Validating rows in platinum.region_demand_daily ..."
docker compose exec postgres psql -U platform -d platform -c "select min(date) as min_date, max(date) as max_date, count(*) as rows from platinum.region_demand_daily;"
