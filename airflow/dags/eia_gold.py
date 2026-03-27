"""
eia_gold.py
===========
DAG: eia_gold
-------------
Watches MinIO silver/ for clean partitions (written by eia_silver) and,
when ALL datasets for the target date are ready, runs the gold aggregation
Spark job to produce final serving tables directly in MinIO gold/.

HOW IT WORKS
------------
A single PythonSensor checks that every dataset listed in dataset_registry.yml
has a silver partition for the target date:

    s3a://silver/eia/<dataset_id>/date=<today>/

When all partitions are present the sensor succeeds and the gold job runs:

    gold_serving.py --date <today>

The gold job (see spark/jobs/gold_serving.py) replaces the old two-step
gold → platinum approach.  It reads silver data, produces all aggregations,
and writes denormalized serving tables that map 1:1 to the PostgreSQL schema:

    gold/eia/fact_generation_hourly/date=<date>/
    gold/eia/fact_demand_hourly/date=<date>/
    gold/eia/dim_balancing_authority/date=<date>/
    gold/eia/dim_fuel_type/date=<date>/
    gold/eia/agg_daily_generation/date=<date>/
    gold/eia/agg_daily_demand_peak/date=<date>/

A final PythonOperator then upserts those Parquet files into PostgreSQL.

TRIGGERING
----------
- Scheduled every hour at :45 (gives silver ~15 min after eia_ingest at :15
  and eia_silver pick-up at :30).
- Trigger manually with {"date": "YYYY-MM-DD"} to reprocess any partition.

SENSOR BEHAVIOUR
----------------
- poke_interval: 60 s
- timeout:       7200 s (2 hours — allows for slow silver jobs)
- mode: "reschedule"

ENVIRONMENT VARIABLES
---------------------
MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD,
KAFKA_BROKER,
POSTGRES_HOST / PORT / DB / USER / PASSWORD
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import psycopg2
import pyarrow.parquet as pq
import s3fs
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

# ── Environment ────────────────────────────────────────────────────────────────
MINIO_ENDPOINT    = os.environ.get("MINIO_ENDPOINT",     "http://minio:9000")
MINIO_USER        = os.environ.get("MINIO_ROOT_USER",    "minioadmin")
MINIO_PASSWORD    = os.environ.get("MINIO_ROOT_PASSWORD","minioadmin")
KAFKA_BROKER      = os.environ.get("KAFKA_BROKER",       "kafka:9092")

POSTGRES_HOST     = os.environ.get("POSTGRES_HOST",     "postgres")
POSTGRES_PORT     = os.environ.get("POSTGRES_PORT",     "5432")
POSTGRES_DB       = os.environ.get("POSTGRES_DB",       "platform")
POSTGRES_USER     = os.environ.get("POSTGRES_USER",     "platform")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "platform")

REGISTRY_PATH  = Path("/opt/airflow/ingestion/src/dataset_registry.yml")

SPARK_MASTER   = "spark://spark-master:7077"
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
SPARK_JOBS_DIR = "/opt/spark/jobs"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_registry() -> list[dict]:
    with open(REGISTRY_PATH) as fh:
        return yaml.safe_load(fh).get("datasets", [])


def _resolve_date(context: dict) -> str:
    conf = context["dag_run"].conf or {}
    return conf.get("date") or context["ds"]


def _spark_cmd(script: str, args: str = "") -> str:
    env_str = (
        f"MINIO_ENDPOINT={MINIO_ENDPOINT} "
        f"MINIO_ROOT_USER={MINIO_USER} "
        f"MINIO_ROOT_PASSWORD={MINIO_PASSWORD} "
        f"KAFKA_BROKER={KAFKA_BROKER} "
        f"SPARK_MASTER={SPARK_MASTER} "
    )
    submit = (
        f"mkdir -p /tmp/ivy2 && "
        f"/opt/spark/bin/spark-submit "
        f"--master {SPARK_MASTER} "
        f"--packages {SPARK_PACKAGES} "
        f"--conf spark.jars.ivy=/tmp/ivy2 "
        f"--conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} "
        f"--conf spark.hadoop.fs.s3a.access.key={MINIO_USER} "
        f"--conf spark.hadoop.fs.s3a.secret.key={MINIO_PASSWORD} "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
        f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
        f"--conf spark.hadoop.fs.s3a.aws.credentials.provider="
        f"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
        f"{SPARK_JOBS_DIR}/{script} {args}"
    )
    return f"docker exec $(docker ps -qf name=spark-master) bash -c '{env_str}{submit}'"


# ── Task callables ─────────────────────────────────────────────────────────────

def _all_silver_partitions_exist(**context) -> bool:
    """
    Sensor: returns True only when every dataset in the registry has a
    silver partition for the target date.
    """
    target_date = _resolve_date(context)
    datasets    = _load_registry()

    fs = s3fs.S3FileSystem(
        key=MINIO_USER,
        secret=MINIO_PASSWORD,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )

    all_ready = True
    for ds in datasets:
        ds_id  = ds["id"].replace("_hourly", "").replace("_monthly", "")
        prefix = f"silver/eia/{ds_id}/date={target_date}/"
        try:
            files = fs.ls(prefix)
            if files:
                print(f"[sensor] ✓ {prefix} ({len(files)} files)")
            else:
                print(f"[sensor] ✗ Empty: {prefix}")
                all_ready = False
        except FileNotFoundError:
            print(f"[sensor] ✗ Missing: {prefix}")
            all_ready = False
        except Exception as exc:
            print(f"[sensor] Error checking {prefix}: {exc}")
            all_ready = False

    return all_ready


def _run_gold(**context) -> None:
    """Run the consolidated gold serving Spark job."""
    target_date = _resolve_date(context)
    cmd = _spark_cmd("gold_serving.py", f"--date {target_date}")

    print(f"[gold] Building serving tables for {target_date}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"Gold Spark job failed for {target_date}:\n{result.stderr}")
    print(f"[gold] Spark job complete for {target_date}")


def _load_gold_to_postgres(**context) -> None:
    """
    Upsert gold serving Parquet partitions from MinIO into PostgreSQL.
    Table configs map directly to the warehouse schema (002_platinum_schema.sql).
    """
    from psycopg2.extras import execute_values

    target_date = _resolve_date(context)

    fs = s3fs.S3FileSystem(
        key=MINIO_USER,
        secret=MINIO_PASSWORD,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    # Each entry: MinIO gold path → PostgreSQL table → upsert SQL
    table_configs = [
        {
            "table":   "dim_balancing_authority",
            "path":    f"gold/eia/dim_balancing_authority/date={target_date}",
            "columns": ["ba_code", "ba_name"],
            "upsert":  """
                INSERT INTO dim_balancing_authority (ba_code, ba_name) VALUES %s
                ON CONFLICT (ba_code) DO UPDATE SET ba_name = EXCLUDED.ba_name;
            """,
        },
        {
            "table":   "dim_fuel_type",
            "path":    f"gold/eia/dim_fuel_type/date={target_date}",
            "columns": ["fuel_code", "fuel_name"],
            "upsert":  """
                INSERT INTO dim_fuel_type (fuel_code, fuel_name) VALUES %s
                ON CONFLICT (fuel_code) DO UPDATE SET fuel_name = EXCLUDED.fuel_name;
            """,
        },
        {
            "table":   "fact_generation_hourly",
            "path":    f"gold/eia/fact_generation_hourly/date={target_date}",
            "columns": [
                "record_id", "period_ts", "ba_code", "ba_name",
                "fuel_code", "fuel_name", "generation_gwh", "partition_date",
            ],
            "upsert":  """
                INSERT INTO fact_generation_hourly
                    (record_id, period_ts, ba_code, ba_name,
                     fuel_code, fuel_name, generation_gwh, partition_date)
                VALUES %s
                ON CONFLICT (record_id) DO UPDATE SET
                    generation_gwh = EXCLUDED.generation_gwh,
                    partition_date = EXCLUDED.partition_date;
            """,
        },
        {
            "table":   "fact_demand_hourly",
            "path":    f"gold/eia/fact_demand_hourly/date={target_date}",
            "columns": [
                "record_id", "period_ts", "ba_code", "ba_name",
                "demand_gwh", "forecast_gwh", "partition_date",
            ],
            "upsert":  """
                INSERT INTO fact_demand_hourly
                    (record_id, period_ts, ba_code, ba_name,
                     demand_gwh, forecast_gwh, partition_date)
                VALUES %s
                ON CONFLICT (record_id) DO UPDATE SET
                    demand_gwh   = EXCLUDED.demand_gwh,
                    forecast_gwh = EXCLUDED.forecast_gwh,
                    partition_date = EXCLUDED.partition_date;
            """,
        },
        {
            "table":   "agg_daily_generation",
            "path":    f"gold/eia/agg_daily_generation/date={target_date}",
            "columns": ["report_date", "fuel_code", "fuel_name", "total_gwh", "partition_date"],
            "upsert":  """
                INSERT INTO agg_daily_generation
                    (report_date, fuel_code, fuel_name, total_gwh, partition_date)
                VALUES %s
                ON CONFLICT (report_date, fuel_code) DO UPDATE SET
                    total_gwh      = EXCLUDED.total_gwh,
                    partition_date = EXCLUDED.partition_date;
            """,
        },
        {
            "table":   "agg_daily_demand_peak",
            "path":    f"gold/eia/agg_daily_demand_peak/date={target_date}",
            "columns": ["report_date", "ba_code", "ba_name", "peak_gwh", "partition_date"],
            "upsert":  """
                INSERT INTO agg_daily_demand_peak
                    (report_date, ba_code, ba_name, peak_gwh, partition_date)
                VALUES %s
                ON CONFLICT (report_date, ba_code) DO UPDATE SET
                    peak_gwh       = EXCLUDED.peak_gwh,
                    partition_date = EXCLUDED.partition_date;
            """,
        },
    ]

    with conn:
        with conn.cursor() as cur:
            for cfg in table_configs:
                try:
                    dataset  = pq.ParquetDataset(cfg["path"], filesystem=fs)
                    tbl      = dataset.read(columns=cfg["columns"])
                    col_data = [tbl.column(c).to_pylist() for c in cfg["columns"]]
                    rows     = list(zip(*col_data))
                    if not rows:
                        print(f"[postgres] No rows for {cfg['table']} — skipping")
                        continue
                    execute_values(cur, cfg["upsert"], rows, page_size=1000)
                    print(f"[postgres] Upserted {len(rows):,} rows → {cfg['table']}")
                except Exception as exc:
                    print(f"[postgres] ERROR loading {cfg['table']}: {exc}")
                    raise
    conn.close()
    print(f"[postgres] All gold tables loaded for {target_date}")


# ── DAG ────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eia_gold",
    description=(
        "Sense silver partitions in MinIO, run gold aggregation Spark job, "
        "and upsert results into PostgreSQL."
    ),
    schedule_interval="45 * * * *",   # :45 past — after ingest (:15) + silver (:30)
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "gold", "sensor"],
    max_active_runs=1,
    params={
        "date": "",   # Override target date; leave blank to use today (ds)
    },
) as dag:

    sense_silver = PythonSensor(
        task_id="sense_all_silver_partitions",
        python_callable=_all_silver_partitions_exist,
        poke_interval=60,
        timeout=7200,
        mode="reschedule",
        doc_md=(
            "Wait until every dataset in dataset_registry.yml has a silver "
            "partition for the target date before starting gold aggregation."
        ),
    )

    run_gold = PythonOperator(
        task_id="gold_aggregations",
        python_callable=_run_gold,
        doc_md=(
            "Run gold_serving.py Spark job — reads silver/, writes denormalized "
            "serving tables to gold/eia/<table>/date=<date>/."
        ),
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=_load_gold_to_postgres,
        doc_md="Upsert all gold Parquet tables into PostgreSQL warehouse.",
    )

    sense_silver >> run_gold >> load_postgres
