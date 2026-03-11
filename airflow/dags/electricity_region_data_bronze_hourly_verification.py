from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from pipeline_builders import build_bronze_verification_command, build_merge_task, build_validate_bounds_task, build_validate_distinct_task, build_validate_rows_task
from pipeline_constants import BRONZE_HOURLY_COVERAGE_COLUMNS, BRONZE_VERIFICATION_SCHEDULE
from pipeline_support import enqueue_bronze_repair_jobs, get_dataset, trigger_repair_dag_if_idle

DATASET = get_dataset("electricity_region_data")

with DAG(
    dag_id="electricity_region_data_bronze_hourly_verification",
    start_date=datetime(2024, 1, 1),
    schedule=BRONZE_VERIFICATION_SCHEDULE,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["eia", "bronze", "verification", "electricity_region_data"],
) as dag:
    stage_table = "ops.bronze_hourly_coverage_stage_region_{{ ts_nodash | lower }}"

    verify = BashOperator(
        task_id="build_bronze_hourly_coverage_stage",
        bash_command=build_bronze_verification_command("electricity_region_data", DATASET, stage_table),
    )
    validate_stage_rows = build_validate_rows_task(
        "validate_bronze_hourly_coverage_stage_rows",
        stage_table,
        description="electricity_region_data bronze hourly coverage stage rows",
        allow_missing_table=True,
    )
    merge = build_merge_task(
        "merge_bronze_hourly_coverage_stage",
        "ops.bronze_hourly_coverage",
        stage_table,
        BRONZE_HOURLY_COVERAGE_COLUMNS,
        ["dataset_id", "hour_start_utc"],
    )
    validate_rows = build_validate_rows_task(
        "validate_bronze_hourly_coverage_rows",
        "ops.bronze_hourly_coverage",
        where_clause="dataset_id = %s",
        params=["electricity_region_data"],
        description="electricity_region_data bronze hourly coverage rows",
        allow_empty_result=True,
    )
    validate_distinct_hours = build_validate_distinct_task(
        "validate_bronze_hourly_coverage_distinct_hours",
        "ops.bronze_hourly_coverage",
        "hour_start_utc",
        where_clause="dataset_id = %s",
        params=["electricity_region_data"],
        description="electricity_region_data bronze hourly coverage distinct hours",
        allow_empty_result=True,
    )
    validate_non_negative_counts = build_validate_bounds_task(
        "validate_bronze_hourly_coverage_nonnegative_counts",
        "ops.bronze_hourly_coverage",
        "observed_row_count",
        min_value=0.0,
        where_clause="dataset_id = %s",
        params=["electricity_region_data"],
        description="electricity_region_data bronze hourly observed row counts",
        allow_empty_result=True,
    )
    enqueue_repairs = PythonOperator(
        task_id="enqueue_bronze_repair_candidates",
        python_callable=enqueue_bronze_repair_jobs,
        op_kwargs={"dataset_id": "electricity_region_data", "statuses": ("missing",), "max_pending_override": 0},
    )
    trigger_repairs = PythonOperator(
        task_id="trigger_bronze_repair_if_idle",
        python_callable=trigger_repair_dag_if_idle,
        op_kwargs={"dataset_id": "electricity_region_data"},
    )

    verify >> validate_stage_rows >> merge >> validate_rows >> validate_distinct_hours >> validate_non_negative_counts >> enqueue_repairs >> trigger_repairs
