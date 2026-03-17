"""
eia_hourly_pipeline.py
-----------------------
Airflow DAG: EIA Electricity — Hourly Refresh

Schedule: Every hour at :15 past (gives EIA time to publish)

Fetches only the last 2 hours of data to keep runs fast.
Runs the full bronze → silver → gold → platinum → postgres stack
for the current date partition.

spark_submit_cmd uses --packages (Maven download) matching the working
eia_electricity_pipeline.py pattern. Spark 4.1.0 / Scala 2.13.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import psycopg2
import pyarrow.parquet as pq
import s3fs

# ── Environment ───────────────────────────────────────────────────────────────
KAFKA_BROKER      = os.environ.get("KAFKA_BROKER", "kafka:9092")
MINIO_ENDPOINT    = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER        = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD    = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
POSTGRES_HOST     = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT     = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.environ.get("POSTGRES_DB", "platform")
POSTGRES_USER     = os.environ.get("POSTGRES_USER", "platform")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "platform")

SPARK_MASTER   = "spark://spark-master:7077"
SPARK_JOBS_DIR = "/opt/spark/jobs"

# Spark 4.1.0 uses Scala 2.13 — _2.13 suffix required
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def spark_submit_cmd(script: str, args: str = "") -> str:
    """Build a docker exec spark-submit using --packages, matching working pipeline."""
    env_vars = (
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
        f"--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
        f"{SPARK_JOBS_DIR}/{script} {args}"
    )
    return f"docker exec $(docker ps -qf name=spark-master) bash -c '{env_vars}{submit}'"


def load_platinum_to_postgres(ds: str, **kwargs) -> None:
    """Upsert platinum Parquet partitions into PostgreSQL."""
    from psycopg2.extras import execute_values

    fs = s3fs.S3FileSystem(
        key=MINIO_USER, secret=MINIO_PASSWORD,
        endpoint_url=MINIO_ENDPOINT, use_ssl=False,
    )
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )

    table_configs = [
        {
            "table": "dim_balancing_authority",
            "path": f"gold/eia/platinum_dim_balancing_authority/date={ds}",
            "columns": ["ba_code", "ba_name"],
            "upsert_sql": """
                INSERT INTO dim_balancing_authority (ba_code, ba_name)
                VALUES %s
                ON CONFLICT (ba_code) DO UPDATE SET ba_name = EXCLUDED.ba_name;
            """,
        },
        {
            "table": "dim_fuel_type",
            "path": f"gold/eia/platinum_dim_fuel_type/date={ds}",
            "columns": ["fuel_code", "fuel_name"],
            "upsert_sql": """
                INSERT INTO dim_fuel_type (fuel_code, fuel_name)
                VALUES %s
                ON CONFLICT (fuel_code) DO UPDATE SET fuel_name = EXCLUDED.fuel_name;
            """,
        },
        {
            "table": "fact_generation_hourly",
            "path": f"gold/eia/platinum_fact_generation_hourly/date={ds}",
            "columns": ["record_id", "period_ts", "ba_code", "ba_name",
                        "fuel_code", "fuel_name", "generation_gwh", "partition_date"],
            "upsert_sql": """
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
            "table": "fact_demand_hourly",
            "path": f"gold/eia/platinum_fact_demand_hourly/date={ds}",
            "columns": ["record_id", "period_ts", "ba_code", "ba_name",
                        "demand_gwh", "forecast_gwh", "partition_date"],
            "upsert_sql": """
                INSERT INTO fact_demand_hourly
                    (record_id, period_ts, ba_code, ba_name,
                     demand_gwh, forecast_gwh, partition_date)
                VALUES %s
                ON CONFLICT (record_id) DO UPDATE SET
                    demand_gwh = EXCLUDED.demand_gwh,
                    forecast_gwh = EXCLUDED.forecast_gwh,
                    partition_date = EXCLUDED.partition_date;
            """,
        },
        {
            "table": "agg_daily_generation",
            "path": f"gold/eia/platinum_agg_daily_generation/date={ds}",
            "columns": ["report_date", "fuel_code", "fuel_name", "total_gwh", "partition_date"],
            "upsert_sql": """
                INSERT INTO agg_daily_generation
                    (report_date, fuel_code, fuel_name, total_gwh, partition_date)
                VALUES %s
                ON CONFLICT (report_date, fuel_code) DO UPDATE SET
                    total_gwh = EXCLUDED.total_gwh,
                    partition_date = EXCLUDED.partition_date;
            """,
        },
        {
            "table": "agg_daily_demand_peak",
            "path": f"gold/eia/platinum_agg_daily_demand_peak/date={ds}",
            "columns": ["report_date", "ba_code", "ba_name", "peak_gwh", "partition_date"],
            "upsert_sql": """
                INSERT INTO agg_daily_demand_peak
                    (report_date, ba_code, ba_name, peak_gwh, partition_date)
                VALUES %s
                ON CONFLICT (report_date, ba_code) DO UPDATE SET
                    peak_gwh = EXCLUDED.peak_gwh,
                    partition_date = EXCLUDED.partition_date;
            """,
        },
    ]

    with conn:
        with conn.cursor() as cur:
            for cfg in table_configs:
                try:
                    dataset  = pq.ParquetDataset(cfg["path"], filesystem=fs)
                    table    = dataset.read(columns=cfg["columns"])
                    col_data = [table.column(c).to_pylist() for c in cfg["columns"]]
                    rows     = list(zip(*col_data))
                    if not rows:
                        print(f"[postgres] No rows for {cfg['table']} — skipping")
                        continue
                    execute_values(cur, cfg["upsert_sql"], rows, page_size=1000)
                    print(f"[postgres] Upserted {len(rows)} rows into {cfg['table']}")
                except Exception as exc:
                    print(f"[postgres] WARNING: Failed to load {cfg['table']}: {exc}")
                    raise
    conn.close()
    print(f"[postgres] All platinum tables loaded for {ds}")


# ── DAG ───────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eia_hourly_pipeline",
    description="EIA Electricity — hourly refresh (last 2 hours only)",
    schedule_interval="15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "electricity", "hourly"],
    max_active_runs=1,
) as dag:

    ingest = BashOperator(
        task_id="ingest_eia_to_kafka",
        bash_command=(
            "cd /opt/airflow/ingestion/src && "
            "ROLLING_DAYS_OVERRIDE=0.083 "
            "python fetch_eia.py"
        ),
        env={
            "EIA_API_KEY":         os.environ.get("EIA_API_KEY", ""),
            "KAFKA_BROKER":        KAFKA_BROKER,
            "MINIO_ROOT_USER":     MINIO_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
            "PATH":                os.environ.get("PATH", ""),
        },
    )

    bronze_generation = BashOperator(
        task_id="bronze_generation",
        bash_command=spark_submit_cmd(
            "bronze_kafka_to_minio.py",
            "--topic eia.electricity.generation --date {{ ds }}"
        ),
        doc_md="Consume generation Kafka topic → MinIO bronze/.",
    )

    bronze_demand = BashOperator(
        task_id="bronze_demand",
        bash_command=spark_submit_cmd(
            "bronze_kafka_to_minio.py",
            "--topic eia.electricity.demand --date {{ ds }}"
        ),
        doc_md="Consume demand Kafka topic → MinIO bronze/.",
    )

    silver_generation = BashOperator(
        task_id="silver_generation",
        bash_command=spark_submit_cmd(
            "silver_clean_transform.py",
            "--dataset electricity_generation --date {{ ds }}"
        ),
        doc_md="Clean and normalize generation data → MinIO silver/.",
    )

    silver_demand = BashOperator(
        task_id="silver_demand",
        bash_command=spark_submit_cmd(
            "silver_clean_transform.py",
            "--dataset electricity_demand --date {{ ds }}"
        ),
        doc_md="Clean and normalize demand data → MinIO silver/.",
    )

    gold = BashOperator(
        task_id="gold_aggregations",
        bash_command=spark_submit_cmd("gold_to_postgres.py", "--date {{ ds }}"),
        doc_md="Aggregate generation and demand → MinIO gold/.",
    )

    platinum = BashOperator(
        task_id="platinum_serving_tables",
        bash_command=spark_submit_cmd("platinum_serving_tables.py", "--date {{ ds }}"),
        doc_md="Build serving schema tables in MinIO gold/platinum_*/.",
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=load_platinum_to_postgres,
        doc_md="Upsert platinum Parquet into PostgreSQL warehouse tables.",
    )

    ingest >> [bronze_generation, bronze_demand]
    bronze_generation >> silver_generation
    bronze_demand     >> silver_demand
    [silver_generation, silver_demand] >> gold >> platinum >> load_postgres
