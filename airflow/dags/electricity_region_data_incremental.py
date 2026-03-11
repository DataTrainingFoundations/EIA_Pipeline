from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
from pipeline_constants import REGION_DAILY_COLUMNS
from pipeline_support import get_dataset

DATASET = get_dataset("electricity_region_data")

with DAG(
    dag_id="electricity_region_data_incremental",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["eia", "pipeline", "electricity_region_data", "incremental"],
) as dag:
    start_expr = "{{ data_interval_start.in_timezone('UTC').isoformat() }}"
    end_expr = "{{ data_interval_end.in_timezone('UTC').isoformat() }}"
    cli_start_expr = "{{ data_interval_start.in_timezone('UTC').strftime('%Y-%m-%dT%H') }}"
    cli_end_expr = "{{ data_interval_end.in_timezone('UTC').strftime('%Y-%m-%dT%H') }}"
    where_clause = "source_window_start = %s and source_window_end = %s"
    window_params = [start_expr, end_expr]
    stage_table = "platinum.region_demand_daily_stage_{{ ts_nodash | lower }}"

    ingest = BashOperator(
        task_id="ingest_to_kafka",
        bash_command=build_fetch_command("electricity_region_data", cli_start_expr, cli_end_expr, max_pages=20),
    )
    bronze = BashOperator(
        task_id="spark_bronze_batch",
        bash_command=build_bronze_command(DATASET),
    )
    silver = BashOperator(
        task_id="spark_silver_batch",
        bash_command=build_silver_command(DATASET, "electricity_region_data", start_expr, end_expr),
    )
    curated_gold = BashOperator(
        task_id="spark_curated_gold_batch",
        bash_command=build_curated_gold_command("electricity_region_data", start_expr, end_expr),
    )
    platinum = BashOperator(
        task_id="spark_platinum_stage",
        bash_command=build_region_daily_platinum_command(stage_table, start_expr, end_expr),
    )
    validate_stage_rows = build_validate_rows_task(
        "validate_platinum_stage_rows",
        stage_table,
        description="region demand platinum stage rows",
        allow_missing_table=True,
    )
    merge = build_merge_task(
        "merge_platinum_stage",
        "platinum.region_demand_daily",
        stage_table,
        REGION_DAILY_COLUMNS,
        ["date", "respondent"],
    )
    validate_rows = build_validate_rows_task(
        "validate_platinum_rows",
        "platinum.region_demand_daily",
        where_clause=where_clause,
        params=window_params,
        description="region demand platinum current window",
        allow_empty_result=True,
    )
    validate_distinct_respondents = build_validate_distinct_task(
        "validate_platinum_distinct_respondents",
        "platinum.region_demand_daily",
        "respondent",
        where_clause=where_clause,
        params=window_params,
        description="region demand platinum current window respondents",
        allow_empty_result=True,
    )
    validate_positive_demand = build_validate_bounds_task(
        "validate_platinum_nonnegative_demand",
        "platinum.region_demand_daily",
        "daily_demand_mwh",
        min_value=0.0,
        where_clause=where_clause,
        params=window_params,
        description="region demand platinum nonnegative daily demand",
        allow_empty_result=True,
    )

    ingest >> bronze >> silver >> curated_gold >> platinum
    platinum >> validate_stage_rows >> merge >> validate_rows >> validate_distinct_respondents >> validate_positive_demand
