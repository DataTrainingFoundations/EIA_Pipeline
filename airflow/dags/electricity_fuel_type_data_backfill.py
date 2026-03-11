from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

from pipeline_builders import build_bronze_command, build_curated_gold_command, build_fetch_command, build_silver_command
from pipeline_constants import BACKFILL_SCHEDULE
from pipeline_support import (
    claim_next_backfill_chunk,
    enqueue_backfill_jobs,
    get_dataset,
    has_backfill_chunk,
    mark_backfill_completed,
    mark_backfill_failed,
)

DATASET = get_dataset("electricity_fuel_type_data")

with DAG(
    dag_id="electricity_fuel_type_data_backfill",
    start_date=datetime(2024, 1, 1),
    schedule=BACKFILL_SCHEDULE,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["eia", "pipeline", "electricity_fuel_type_data", "backfill"],
) as dag:
    chunk_start_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_start_utc'] }}"
    chunk_end_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_end_utc'] }}"
    chunk_cli_start_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_start_cli'] }}"
    chunk_cli_end_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_end_cli'] }}"

    enqueue = PythonOperator(
        task_id="enqueue_backfill_jobs",
        python_callable=enqueue_backfill_jobs,
        op_kwargs={"dataset_id": "electricity_fuel_type_data"},
    )
    claim = PythonOperator(
        task_id="claim_backfill_chunk",
        python_callable=claim_next_backfill_chunk,
        op_kwargs={"dataset_id": "electricity_fuel_type_data"},
    )
    has_work = ShortCircuitOperator(
        task_id="has_backfill_chunk",
        python_callable=has_backfill_chunk,
        op_args=[claim.output],
    )
    ingest = BashOperator(
        task_id="ingest_backfill_chunk",
        bash_command=build_fetch_command("electricity_fuel_type_data", chunk_cli_start_expr, chunk_cli_end_expr, max_pages=50),
    )
    bronze = BashOperator(
        task_id="spark_bronze_backfill_batch",
        bash_command=build_bronze_command(DATASET),
    )
    silver = BashOperator(
        task_id="spark_silver_backfill",
        bash_command=build_silver_command(DATASET, "electricity_fuel_type_data", chunk_start_expr, chunk_end_expr),
    )
    curated_gold = BashOperator(
        task_id="spark_curated_gold_backfill",
        bash_command=build_curated_gold_command("electricity_fuel_type_data", chunk_start_expr, chunk_end_expr),
    )
    mark_complete = PythonOperator(
        task_id="mark_backfill_complete",
        python_callable=mark_backfill_completed,
        op_kwargs={"job_id": "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['id'] }}"},
    )
    mark_failed = PythonOperator(
        task_id="mark_backfill_failed",
        python_callable=mark_backfill_failed,
        op_kwargs={
            "job_id": "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['id'] if ti.xcom_pull(task_ids='claim_backfill_chunk') else 0 }}",
            "error_message": "backfill chunk failed; inspect Airflow task logs",
        },
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    enqueue >> claim >> has_work >> ingest >> bronze >> silver >> curated_gold >> mark_complete
    [ingest, bronze, silver, curated_gold] >> mark_failed
