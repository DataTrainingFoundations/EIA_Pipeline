"""
eia_silver.py
=============
DAG: eia_silver
---------------
Watches Snowflake RAW tables for new data and, when rows are present for the
target date, runs the silver clean/validate/deduplicate Spark job per dataset.

HOW IT WORKS
------------
A PythonSensor per dataset queries the Snowflake raw table for rows where
_FETCHED_AT falls on the target date.  When rows are found the corresponding
silver Spark job fires:

    silver_clean_transform.py --dataset <dataset_id> --date <date>

The Spark job reads raw rows directly from Snowflake (via the Snowflake Spark
Connector), cleans and deduplicates them, then writes clean Parquet to:

    s3a://silver/eia/<dataset_id>/date=<date>/

TRIGGERING
----------
- Scheduled every 30 minutes so it catches data shortly after eia_ingest.
- Trigger manually with {"date": "YYYY-MM-DD"} to reprocess a specific date.

SENSOR BEHAVIOUR
----------------
- poke_interval: 60 s
- timeout:       3600 s (1 hour — alerts if ingest is broken)
- mode: "reschedule" (frees the worker slot between pokes)

ENVIRONMENT VARIABLES
---------------------
SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,
MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

# ── Environment ────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT",     "http://minio:9000")
MINIO_USER     = os.environ.get("MINIO_ROOT_USER",    "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD","minioadmin")

REGISTRY_PATH  = Path("/opt/airflow/ingestion/src/dataset_registry.yml")

SPARK_MASTER   = "spark://spark-master:7077"
SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    # Snowflake Spark connector — reads directly from Snowflake in Spark jobs
    "net.snowflake:snowflake-jdbc:3.16.1,"
    "net.snowflake:spark-snowflake_2.13:2.16.0-spark_3.4"
)
SPARK_JOBS_DIR = "/opt/spark/jobs"

_SNOWFLAKE_ENV_KEYS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_registry() -> list[dict]:
    with open(REGISTRY_PATH) as fh:
        return yaml.safe_load(fh).get("datasets", [])


def _resolve_date(context: dict) -> str:
    conf = context["dag_run"].conf or {}
    return conf.get("date") or context["ds"]


def _snowflake_session():
    """Open a lightweight Snowpark session for sensor queries."""
    from snowflake.snowpark import Session
    return Session.builder.configs({
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE",  "EIA"),
        "schema":    os.environ.get("SNOWFLAKE_SCHEMA",    "RAW"),
    }).create()


# ── Task callables ─────────────────────────────────────────────────────────────

def _snowflake_rows_exist(dataset_id: str, **context) -> bool:
    """
    Sensor: returns True when the Snowflake RAW table for this dataset
    contains rows fetched on the target date (_FETCHED_AT::DATE = target_date).
    """
    target_date = _resolve_date(context)
    registry    = _load_registry()
    dataset     = next((d for d in registry if d["id"] == dataset_id), None)

    if dataset is None:
        raise ValueError(f"Dataset '{dataset_id}' not found in registry.")

    table = dataset.get("snowflake_table")
    if not table:
        raise ValueError(f"Dataset '{dataset_id}' has no 'snowflake_table' configured.")

    session = _snowflake_session()
    try:
        # Check for rows fetched on the target date
        result = session.sql(
            f"SELECT COUNT(*) AS n FROM {table} "
            f"WHERE TRY_TO_DATE(_FETCHED_AT) = '{target_date}' "
            f"LIMIT 1"
        ).collect()
        count = result[0]["N"] if result else 0
        if count > 0:
            print(f"[sensor] {table}: {count:,} rows found for {target_date}")
            return True
        else:
            print(f"[sensor] {table}: no rows yet for {target_date}")
            return False
    except Exception as exc:
        # Table may not exist yet on first ever run
        print(f"[sensor] {table} query error (table may not exist yet): {exc}")
        return False
    finally:
        session.close()


def _run_silver(dataset_id: str, **context) -> None:
    """
    Run silver_clean_transform.py for one dataset.
    The Spark job reads from Snowflake and writes clean Parquet to MinIO silver/.
    """
    target_date = _resolve_date(context)

    # Strip frequency suffix for the Spark --dataset arg
    spark_dataset = dataset_id.replace("_hourly", "").replace("_monthly", "")

    sf_url = (
        f"{os.environ.get('SNOWFLAKE_ACCOUNT', '')}.snowflakecomputing.com"
    )

    env_str = (
        f"MINIO_ENDPOINT={MINIO_ENDPOINT} "
        f"MINIO_ROOT_USER={MINIO_USER} "
        f"MINIO_ROOT_PASSWORD={MINIO_PASSWORD} "
        f"SPARK_MASTER={SPARK_MASTER} "
        # Snowflake vars for the Spark connector
        f"SNOWFLAKE_URL={sf_url} "
        + " ".join(
            f"{k}={os.environ.get(k, '')}"
            for k in _SNOWFLAKE_ENV_KEYS
        )
        + " "
    )

    submit_cmd = (
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
        f"{SPARK_JOBS_DIR}/silver_clean_transform.py "
        f"--dataset {spark_dataset} --date {target_date}"
    )
    full_cmd = (
        f"docker exec $(docker ps -qf name=spark-master) "
        f"bash -c '{env_str}{submit_cmd}'"
    )

    print(f"[silver] {spark_dataset} for {target_date}")
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(
            f"Silver job failed for '{spark_dataset}' on {target_date}:\n{result.stderr}"
        )
    print(f"[silver] Done -> s3a://silver/eia/{spark_dataset}/date={target_date}/")


# ── DAG ────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eia_silver",
    description=(
        "Sense new rows in Snowflake RAW tables and run silver "
        "clean/validate Spark jobs per dataset, writing Parquet to MinIO silver/."
    ),
    schedule_interval="*/30 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "silver", "snowflake", "sensor"],
    max_active_runs=1,
    params={
        "date": "",   # Override target date; leave blank to use today (ds)
    },
) as dag:

    datasets = _load_registry()

    for ds in datasets:
        ds_id = ds["id"]

        sense_task = PythonSensor(
            task_id=f"sense_snowflake__{ds_id}",
            python_callable=_snowflake_rows_exist,
            op_kwargs={"dataset_id": ds_id},
            poke_interval=60,
            timeout=3600,
            mode="reschedule",
            doc_md=(
                f"Wait for rows to appear in Snowflake table "
                f"`{ds.get('snowflake_table', '?')}` for the target date."
            ),
        )

        silver_task = PythonOperator(
            task_id=f"silver__{ds_id}",
            python_callable=_run_silver,
            op_kwargs={"dataset_id": ds_id},
            doc_md=(
                f"Run silver_clean_transform.py for `{ds_id}`: "
                f"read from Snowflake, clean, write Parquet to "
                f"`s3a://silver/eia/{ds_id.replace('_hourly','')}/date=<date>/`."
            ),
        )

        sense_task >> silver_task