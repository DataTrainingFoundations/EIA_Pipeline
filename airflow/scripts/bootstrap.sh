#!/usr/bin/env bash
set -euo pipefail

export PATH="/home/airflow/.local/bin:${PATH}"
export SPARK_IVY_CACHE="${SPARK_IVY_CACHE:-/tmp/.ivy}"
export AIRFLOW_DAG_UNPAUSE_DELAY_SECONDS="${AIRFLOW_DAG_UNPAUSE_DELAY_SECONDS:-90}"
export AIRFLOW_PLATINUM_DAG_UNPAUSE_DELAY_SECONDS="${AIRFLOW_PLATINUM_DAG_UNPAUSE_DELAY_SECONDS:-180}"
readonly DATASET_DAGS=(
  electricity_region_data_incremental
  electricity_region_data_backfill
  electricity_fuel_type_data_incremental
  electricity_fuel_type_data_backfill
  electricity_region_data_bronze_hourly_verification
  electricity_fuel_type_data_bronze_hourly_verification
  electricity_region_data_bronze_hourly_repair
  electricity_fuel_type_data_bronze_hourly_repair
)
readonly PLATINUM_DAGS=(
  platinum_grid_operations_hourly
  platinum_resource_planning_daily
)
readonly ALL_DAGS=("${DATASET_DAGS[@]}" "${PLATINUM_DAGS[@]}")
readonly BRONZE_WRITE_POOLS=(
  electricity_region_data_bronze_write
  electricity_fuel_type_data_bronze_write
)

wait_for_airflow_db() {
  for _ in $(seq 1 60); do
    if airflow db check >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  echo "timed out waiting for the Airflow metadata database" >&2
  return 1
}

wait_for_dag_registration() {
  local expected_count="${#ALL_DAGS[@]}"
  for _ in $(seq 1 24); do
    local registered_count
    registered_count="$(
      airflow dags list 2>/dev/null | awk -F'|' '
        NR > 2 {
          dag=$1
          gsub(/^[[:space:]]+|[[:space:]]+$/, "", dag)
          if (dag != "") {
            count++
          }
        }
        END { print count + 0 }
      '
    )"
    if [[ "${registered_count}" -ge "${expected_count}" ]]; then
      return 0
    fi
    sleep 5
  done
  echo "timed out waiting for DAG registration" >&2
  return 1
}

unpause_and_verify() {
  local dag_id="$1"
  for _ in $(seq 1 12); do
    airflow dags unpause "${dag_id}" >/dev/null 2>&1 || true
    if airflow dags list | awk -F'|' -v dag="${dag_id}" '
      $1 ~ dag {
        paused=$4
        gsub(/[[:space:]]/, "", paused)
        if (paused == "False") {
          found=1
        }
      }
      END { exit(found ? 0 : 1) }
    '; then
      return 0
    fi
    sleep 5
  done
  echo "failed to verify unpause for ${dag_id}" >&2
  return 1
}

cleanup() {
  for pid in "${scheduler_pid:-}" "${triggerer_pid:-}" "${webserver_pid:-}" "${unpause_pid:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
}

trap cleanup EXIT INT TERM

rm -f /opt/airflow/airflow-webserver.pid /opt/airflow/airflow-scheduler.pid

wait_for_airflow_db
airflow db migrate

airflow users create \
  --username "${AIRFLOW_WWW_USER_USERNAME:-admin}" \
  --password "${AIRFLOW_WWW_USER_PASSWORD:-admin}" \
  --firstname "${AIRFLOW_WWW_USER_FIRSTNAME:-Airflow}" \
  --lastname "${AIRFLOW_WWW_USER_LASTNAME:-Admin}" \
  --role Admin \
  --email "${AIRFLOW_WWW_USER_EMAIL:-admin@example.com}" || true

for pool_name in "${BRONZE_WRITE_POOLS[@]}"; do
  airflow pools set "${pool_name}" 1 "Serialize Bronze writers per dataset to prevent duplicate Kafka replay writes" || true
done

# Keep pipeline DAGs paused during service bootstrap so the first scheduled run
# starts after Spark/Kafka/MinIO are actually ready.
for dag_id in "${DATASET_DAGS[@]}" "${PLATINUM_DAGS[@]}"; do
  airflow dags pause "${dag_id}" || true
done

airflow scheduler &
scheduler_pid=$!

airflow triggerer &
triggerer_pid=$!

mkdir -p "${SPARK_IVY_CACHE}"
spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --conf "spark.jars.ivy=${SPARK_IVY_CACHE}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /workspace/spark/jobs/noop.py
spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --conf "spark.jars.ivy=${SPARK_IVY_CACHE}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3" \
  /workspace/spark/jobs/noop.py

(
  for _ in $(seq 1 60); do
    if curl -fsS http://localhost:8080/health > /dev/null 2>&1; then
      break
    fi
    sleep 5
  done

  sleep "${AIRFLOW_DAG_UNPAUSE_DELAY_SECONDS}"
  wait_for_dag_registration

  for dag_id in "${DATASET_DAGS[@]}"; do
    unpause_and_verify "${dag_id}" || true
  done

  sleep "${AIRFLOW_PLATINUM_DAG_UNPAUSE_DELAY_SECONDS}"

  for dag_id in "${PLATINUM_DAGS[@]}"; do
    unpause_and_verify "${dag_id}" || true
  done
) &
unpause_pid=$!

airflow webserver --port 8080 &
webserver_pid=$!

wait "${webserver_pid}"
