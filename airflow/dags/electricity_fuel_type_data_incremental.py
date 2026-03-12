from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline_builders import build_bronze_command, bronze_write_pool, build_curated_gold_command, build_fetch_command, build_silver_command
from pipeline_support import get_dataset

DATASET = get_dataset("electricity_fuel_type_data")

with DAG(
    dag_id="electricity_fuel_type_data_incremental",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["eia", "pipeline", "electricity_fuel_type_data", "incremental"],
) as dag:
    start_expr = "{{ data_interval_start.in_timezone('UTC').isoformat() }}"
    end_expr = "{{ data_interval_end.in_timezone('UTC').isoformat() }}"
    cli_start_expr = "{{ data_interval_start.in_timezone('UTC').strftime('%Y-%m-%dT%H') }}"
    cli_end_expr = "{{ data_interval_end.in_timezone('UTC').strftime('%Y-%m-%dT%H') }}"

    ingest = BashOperator(
        task_id="ingest_to_kafka",
        bash_command=build_fetch_command("electricity_fuel_type_data", cli_start_expr, cli_end_expr, max_pages=20),
    )
    bronze = BashOperator(
        task_id="spark_bronze_batch",
        bash_command=build_bronze_command(DATASET),
        pool=bronze_write_pool("electricity_fuel_type_data"),
    )
    silver = BashOperator(
        task_id="spark_silver_batch",
        bash_command=build_silver_command(DATASET, "electricity_fuel_type_data", start_expr, end_expr),
    )
    curated_gold = BashOperator(
        task_id="spark_curated_gold_batch",
        bash_command=build_curated_gold_command("electricity_fuel_type_data", start_expr, end_expr),
    )

    ingest >> bronze >> silver >> curated_gold
