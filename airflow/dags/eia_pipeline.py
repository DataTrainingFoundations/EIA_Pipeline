"""
eia_pipeline.py
---------------
Airflow DAG: EIA Electricity Data Pipeline

Schedule: Daily at 06:00 UTC (EIA publishes hourly data with ~1hr lag)

Pipeline stages:
    1. ingest          — fetch_eia.py → Kafka topics
    2. bronze          — Kafka → MinIO bronze/ (raw JSON, partitioned by date)
    3. silver          — bronze → MinIO silver/ (clean Parquet, deduplicated)
    4. gold            — silver → MinIO gold/ (aggregated Parquet)
    5. platinum        — gold → MinIO gold/platinum_* (serving schema Parquet)
    6. load_postgres   — platinum Parquet → PostgreSQL warehouse tables

All Spark jobs are submitted to spark://spark-master:7077 via BashOperator.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

import psycopg2
import pyarrow.parquet as pq
import s3fs

# ── Environment ──────────────────────────────────────────────────────────────
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

POSTGRES_HOST = os.environ.get("POSTGRESd_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "platform")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "platform")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "platform")

SPARK_MASTER = "spark://spark-master:7077"
SPARK_JOBS_DIR = "/opt/spark/jobs"

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

# Run spark-submit inside the spark-master container via docker exec
def spark_submit_cmd(script: str, args: str = "") -> str:
    """Build a docker exec command that runs spark-submit on spark-master."""
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

# ── DAG defaults ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── Postgres loader ───────────────────────────────────────────────────────────
def load_platinum_to_postgres(ds: str, **kwargs) -> None:
    """
    Read each platinum Parquet partition from MinIO and UPSERT into PostgreSQL.
    Uses s3fs to read Parquet then psycopg2 for bulk insert via execute_values.
    """
    from psycopg2.extras import execute_values

    fs = s3fs.S3FileSystem(
        key=MINIO_USER,
        secret=MINIO_PASSWORD,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    # Table → MinIO path → upsert SQL mapping
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
                    dataset = pq.ParquetDataset(cfg["path"], filesystem=fs)
                    table = dataset.read(columns=cfg["columns"])
                    rows = [tuple(row) for row in table.to_pydict().values().__iter__()]

                    # Transpose dict-of-lists to list-of-tuples
                    col_data = [table.column(c).to_pylist() for c in cfg["columns"]]
                    rows = list(zip(*col_data))

                    execute_values(cur, cfg["upsert_sql"], rows, page_size=1000)
                    print(f"[postgres] Upserted {len(rows)} rows into {cfg['table']}")
                except Exception as exc:
                    print(f"[postgres] WARNING: Failed to load {cfg['table']}: {exc}")
                    raise

    conn.close()
    print(f"[postgres] All platinum tables loaded for {ds}")


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="eia_electricity_pipeline",
    description="EIA Electricity: Ingest → Bronze → Silver → Gold → Platinum → PostgreSQL",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "electricity", "pipeline"],
    max_active_runs=1,
) as dag:

    # ── Stage 1: Ingest ───────────────────────────────────────────────────────
    ingest = BashOperator(
        task_id="ingest_eia_to_kafka",
        bash_command=(
            "cd /opt/airflow/ingestion/src && "
            "python fetch_eia.py"
        ),
        env={
            "EIA_API_KEY": os.environ.get("EIA_API_KEY", ""),
            "KAFKA_BROKER": KAFKA_BROKER,
            "MINIO_ROOT_USER": MINIO_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
        },
    )

# ── Stage 2: Bronze ───────────────────────────────────────────────────────
    bronze_generation = BashOperator(
        task_id="bronze_generation",
        bash_command=(
            spark_submit_cmd("bronze_kafka_to_minio.py", "--topic eia.electricity.generation --date {{ ds }}")
        ),
        doc_md="Consume generation Kafka topic → MinIO bronze/.",
    )

    bronze_demand = BashOperator(
        task_id="bronze_demand",
        bash_command=(
            spark_submit_cmd("bronze_kafka_to_minio.py", "--topic eia.electricity.demand --date {{ ds }}")
        ),
        doc_md="Consume demand Kafka topic → MinIO bronze/.",
    )

    # ── Stage 3: Silver ───────────────────────────────────────────────────────
    silver_generation = BashOperator(
        task_id="silver_generation",
        bash_command=(
            spark_submit_cmd("silver_clean_transform.py", "--dataset electricity_generation --date {{ ds }}")
        ),
        doc_md="Clean and normalize generation data → MinIO silver/.",
    )

    silver_demand = BashOperator(
        task_id="silver_demand",
        bash_command=(
            spark_submit_cmd("silver_clean_transform.py", "--dataset electricity_demand --date {{ ds }}")
        ),
        doc_md="Clean and normalize demand data → MinIO silver/.",
    )

    # ── Stage 4: Gold ─────────────────────────────────────────────────────────
    gold = BashOperator(
        task_id="gold_aggregations",
        bash_command=(
            spark_submit_cmd("gold_to_postgres.py", "--date {{ ds }}")
        ),
        doc_md="Aggregate generation and demand → MinIO gold/.",
    )

    # ── Stage 5: Platinum ─────────────────────────────────────────────────────
    platinum = BashOperator(
        task_id="platinum_serving_tables",
        bash_command=(
            spark_submit_cmd("platinum_serving_tables.py", "--date {{ ds }}")
        ),
        doc_md="Build serving schema tables in MinIO gold/platinum_*/.",
    )

    # ── Stage 6: Load PostgreSQL ──────────────────────────────────────────────
    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=load_platinum_to_postgres,
        doc_md="Upsert platinum Parquet into PostgreSQL warehouse tables.",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    # Ingest first, then bronze in parallel, then silver in parallel,
    # then gold (needs both silver datasets), then platinum, then load
    ingest >> [bronze_generation, bronze_demand]
    bronze_generation >> silver_generation
    bronze_demand >> silver_demand
    [silver_generation, silver_demand] >> gold
    gold >> platinum >> load_postgres