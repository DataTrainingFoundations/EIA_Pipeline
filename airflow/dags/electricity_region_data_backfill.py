from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

from pipeline_builders import (
    build_bronze_command,
    bronze_write_pool,
    build_curated_gold_command,
    build_fetch_command,
    build_merge_task,
    build_region_daily_platinum_command,
    build_silver_command,
    build_validate_bounds_task,
    build_validate_distinct_task,
    build_validate_rows_task,
)
from pipeline_constants import BACKFILL_SCHEDULE, REGION_DAILY_COLUMNS
from pipeline_support import (
    claim_next_backfill_chunk,
    enqueue_backfill_jobs,
    get_dataset,
    has_backfill_chunk,
    mark_backfill_completed,
    mark_backfill_failed,
)

DATASET = get_dataset("electricity_region_data")

with DAG(
    dag_id="electricity_region_data_backfill",
    start_date=datetime(2024, 1, 1),
    schedule=BACKFILL_SCHEDULE,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["eia", "pipeline", "electricity_region_data", "backfill"],
) as dag:
    chunk_start_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_start_utc'] }}"
    chunk_end_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_end_utc'] }}"
    chunk_cli_start_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_start_cli'] }}"
    chunk_cli_end_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_end_cli'] }}"
    where_clause = "source_window_start = %s and source_window_end = %s"
    window_params = [chunk_start_expr, chunk_end_expr]
    stage_table = "platinum.region_demand_daily_stage_backfill_{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['id'] }}"

    enqueue = PythonOperator(
        task_id="enqueue_backfill_jobs",
        python_callable=enqueue_backfill_jobs,
        op_kwargs={"dataset_id": "electricity_region_data"},
    )
    claim = PythonOperator(
        task_id="claim_backfill_chunk",
        python_callable=claim_next_backfill_chunk,
        op_kwargs={"dataset_id": "electricity_region_data"},
    )
    has_work = ShortCircuitOperator(
        task_id="has_backfill_chunk",
        python_callable=has_backfill_chunk,
        op_args=[claim.output],
    )
    ingest = BashOperator(
        task_id="ingest_backfill_chunk",
        bash_command=build_fetch_command("electricity_region_data", chunk_cli_start_expr, chunk_cli_end_expr, max_pages=50),
    )
    bronze = BashOperator(
        task_id="spark_bronze_backfill_batch",
        bash_command=build_bronze_command(DATASET),
        pool=bronze_write_pool("electricity_region_data"),
    )
    silver = BashOperator(
        task_id="spark_silver_backfill",
        bash_command=build_silver_command(DATASET, "electricity_region_data", chunk_start_expr, chunk_end_expr),
    )
    curated_gold = BashOperator(
        task_id="spark_curated_gold_backfill",
        bash_command=build_curated_gold_command("electricity_region_data", chunk_start_expr, chunk_end_expr),
    )
    platinum = BashOperator(
        task_id="spark_platinum_backfill_stage",
        bash_command=build_region_daily_platinum_command(stage_table, chunk_start_expr, chunk_end_expr),
    )
    validate_stage_rows = build_validate_rows_task(
        "validate_platinum_backfill_stage_rows",
        stage_table,
        description="region demand backfill stage rows",
        allow_missing_table=True,
    )
    merge = build_merge_task(
        "merge_backfill_stage",
        "platinum.region_demand_daily",
        stage_table,
        REGION_DAILY_COLUMNS,
        ["date", "respondent"],
    )
    validate_rows = build_validate_rows_task(
        "validate_backfill_rows",
        "platinum.region_demand_daily",
        where_clause=where_clause,
        params=window_params,
        description="region demand backfill current window",
        allow_empty_result=True,
    )
    validate_distinct_respondents = build_validate_distinct_task(
        "validate_backfill_distinct_respondents",
        "platinum.region_demand_daily",
        "respondent",
        where_clause=where_clause,
        params=window_params,
        description="region demand backfill respondents",
        allow_empty_result=True,
    )
    validate_positive_demand = build_validate_bounds_task(
        "validate_backfill_nonnegative_demand",
        "platinum.region_demand_daily",
        "daily_demand_mwh",
        min_value=0.0,
        where_clause=where_clause,
        params=window_params,
        description="region demand backfill nonnegative daily demand",
        allow_empty_result=True,
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

    enqueue >> claim >> has_work >> ingest >> bronze >> silver >> curated_gold >> platinum
    platinum >> validate_stage_rows >> merge >> validate_rows >> validate_distinct_respondents >> validate_positive_demand >> mark_complete
    [ingest, bronze, silver, curated_gold, platinum, validate_stage_rows, merge, validate_rows, validate_distinct_respondents, validate_positive_demand] >> mark_failed
