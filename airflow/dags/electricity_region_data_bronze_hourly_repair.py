from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

from pipeline_builders import (
    build_bronze_command,
    build_curated_gold_command,
    build_fetch_command,
    build_merge_task,
    build_region_daily_platinum_command,
    build_silver_command,
    build_validate_bounds_task,
    build_validate_distinct_task,
    build_validate_rows_task,
)
from pipeline_constants import BRONZE_REPAIR_SCHEDULE, REGION_DAILY_COLUMNS
from pipeline_support import (
    claim_next_bronze_repair_hour,
    enqueue_bronze_repair_jobs,
    get_dataset,
    has_repair_chunk,
    mark_bronze_repair_completed,
    mark_bronze_repair_failed,
    trigger_repair_dag_if_idle,
)

DATASET = get_dataset("electricity_region_data")

with DAG(
    dag_id="electricity_region_data_bronze_hourly_repair",
    start_date=datetime(2024, 1, 1),
    schedule=BRONZE_REPAIR_SCHEDULE,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["eia", "bronze", "repair", "electricity_region_data"],
) as dag:
    chunk_start_expr = "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['chunk_start_utc'] }}"
    chunk_end_expr = "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['chunk_end_utc'] }}"
    chunk_cli_start_expr = "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['chunk_start_cli'] }}"
    chunk_cli_end_expr = "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['chunk_end_cli'] }}"
    where_clause = "source_window_start = %s and source_window_end = %s"
    window_params = [chunk_start_expr, chunk_end_expr]
    stage_table = "platinum.region_demand_daily_stage_repair_{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['id'] }}"

    enqueue = PythonOperator(
        task_id="enqueue_bronze_repair_jobs",
        python_callable=enqueue_bronze_repair_jobs,
        op_kwargs={"dataset_id": "electricity_region_data"},
    )
    claim = PythonOperator(
        task_id="claim_bronze_repair_hour",
        python_callable=claim_next_bronze_repair_hour,
        op_kwargs={"dataset_id": "electricity_region_data"},
    )
    has_work = ShortCircuitOperator(
        task_id="has_bronze_repair_hour",
        python_callable=has_repair_chunk,
        op_args=[claim.output],
    )
    ingest = BashOperator(
        task_id="ingest_bronze_repair_hour",
        bash_command=build_fetch_command("electricity_region_data", chunk_cli_start_expr, chunk_cli_end_expr, max_pages=20),
    )
    bronze = BashOperator(
        task_id="spark_bronze_repair_batch",
        bash_command=build_bronze_command(DATASET),
    )
    silver = BashOperator(
        task_id="spark_silver_repair",
        bash_command=build_silver_command(DATASET, "electricity_region_data", chunk_start_expr, chunk_end_expr),
    )
    curated_gold = BashOperator(
        task_id="spark_curated_gold_repair",
        bash_command=build_curated_gold_command("electricity_region_data", chunk_start_expr, chunk_end_expr),
    )
    platinum = BashOperator(
        task_id="spark_platinum_repair_stage",
        bash_command=build_region_daily_platinum_command(stage_table, chunk_start_expr, chunk_end_expr),
    )
    validate_stage_rows = build_validate_rows_task(
        "validate_platinum_repair_stage_rows",
        stage_table,
        description="region demand repair stage rows",
        allow_missing_table=True,
    )
    merge = build_merge_task(
        "merge_repair_stage",
        "platinum.region_demand_daily",
        stage_table,
        REGION_DAILY_COLUMNS,
        ["date", "respondent"],
    )
    validate_rows = build_validate_rows_task(
        "validate_repair_rows",
        "platinum.region_demand_daily",
        where_clause=where_clause,
        params=window_params,
        description="region demand repair current window",
        allow_empty_result=True,
    )
    validate_distinct_respondents = build_validate_distinct_task(
        "validate_repair_distinct_respondents",
        "platinum.region_demand_daily",
        "respondent",
        where_clause=where_clause,
        params=window_params,
        description="region demand repair respondents",
        allow_empty_result=True,
    )
    validate_positive_demand = build_validate_bounds_task(
        "validate_repair_nonnegative_demand",
        "platinum.region_demand_daily",
        "daily_demand_mwh",
        min_value=0.0,
        where_clause=where_clause,
        params=window_params,
        description="region demand repair nonnegative daily demand",
        allow_empty_result=True,
    )
    mark_complete = PythonOperator(
        task_id="mark_bronze_repair_complete",
        python_callable=mark_bronze_repair_completed,
        op_kwargs={"job_id": "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['id'] }}"},
    )
    mark_failed = PythonOperator(
        task_id="mark_bronze_repair_failed",
        python_callable=mark_bronze_repair_failed,
        op_kwargs={
            "job_id": "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['id'] if ti.xcom_pull(task_ids='claim_bronze_repair_hour') else 0 }}",
            "error_message": "bronze repair hour failed; inspect Airflow task logs",
        },
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    trigger_next_after_complete = PythonOperator(
        task_id="trigger_next_bronze_repair_if_idle",
        python_callable=trigger_repair_dag_if_idle,
        op_kwargs={"dataset_id": "electricity_region_data", "ignore_run_id": "{{ run_id }}"},
    )
    trigger_next_after_failure = PythonOperator(
        task_id="trigger_next_bronze_repair_after_failure_if_idle",
        python_callable=trigger_repair_dag_if_idle,
        op_kwargs={"dataset_id": "electricity_region_data", "ignore_run_id": "{{ run_id }}"},
    )

    enqueue >> claim >> has_work >> ingest >> bronze >> silver >> curated_gold >> platinum
    platinum >> validate_stage_rows >> merge >> validate_rows >> validate_distinct_respondents >> validate_positive_demand >> mark_complete >> trigger_next_after_complete
    [ingest, bronze, silver, curated_gold, platinum, validate_stage_rows, merge, validate_rows, validate_distinct_respondents, validate_positive_demand] >> mark_failed >> trigger_next_after_failure
