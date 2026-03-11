from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline_builders import (
    build_curated_gold_sensor,
    build_merge_task,
    build_validate_bounds_task,
    build_validate_distinct_task,
    build_validate_rows_task,
)
from pipeline_constants import (
    GOLD_FUEL_FACT_HOURLY_PATH,
    GOLD_FUEL_TYPE_DIM_PATH,
    GOLD_REGION_FACT_HOURLY_PATH,
    RESOURCE_PLANNING_COLUMNS,
    RESOURCE_PLANNING_DAILY_SCHEDULE,
    SPARK_PACKAGES_WITH_POSTGRES,
)
from pipeline_support import build_spark_submit_command

with DAG(
    dag_id="platinum_resource_planning_daily",
    start_date=datetime(2024, 1, 1),
    schedule=RESOURCE_PLANNING_DAILY_SCHEDULE,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["platinum", "resource-planning", "daily"],
) as dag:
    stage_table = "platinum.resource_planning_daily_stage_{{ ts_nodash | lower }}"
    lookback_start_expr = "{{ (data_interval_end.in_timezone('UTC') - macros.timedelta(days=35)).isoformat() }}"
    end_expr = "{{ data_interval_end.in_timezone('UTC').isoformat() }}"
    recent_where = "date >= current_date - interval '35 days'"

    wait_for_region_gold = build_curated_gold_sensor("wait_for_region_curated_gold", "electricity_region_data")
    wait_for_fuel_gold = build_curated_gold_sensor("wait_for_fuel_curated_gold", "electricity_fuel_type_data")
    build_stage = BashOperator(
        task_id="build_resource_planning_daily_stage",
        bash_command=build_spark_submit_command(
            "platinum_resource_planning_daily.py",
            packages=SPARK_PACKAGES_WITH_POSTGRES,
            job_name="platinum_resource_planning_daily",
            application_args=[
                "--region-fact-path",
                GOLD_REGION_FACT_HOURLY_PATH,
                "--fuel-fact-path",
                GOLD_FUEL_FACT_HOURLY_PATH,
                "--fuel-dim-path",
                GOLD_FUEL_TYPE_DIM_PATH,
                "--planning-stage-table",
                stage_table,
                "--start",
                lookback_start_expr,
                "--end",
                end_expr,
            ],
        ),
    )
    validate_stage_rows = build_validate_rows_task(
        "validate_resource_planning_stage_rows",
        stage_table,
        description="resource planning stage rows",
        allow_missing_table=True,
    )
    merge = build_merge_task(
        "merge_resource_planning_daily_stage",
        "platinum.resource_planning_daily",
        stage_table,
        RESOURCE_PLANNING_COLUMNS,
        ["date", "respondent"],
    )
    validate_rows = build_validate_rows_task(
        "validate_resource_planning_rows",
        "platinum.resource_planning_daily",
        where_clause=recent_where,
        description="resource planning rows",
        allow_empty_result=True,
    )
    validate_respondents = build_validate_distinct_task(
        "validate_resource_planning_respondents",
        "platinum.resource_planning_daily",
        "respondent",
        where_clause=recent_where,
        description="resource planning respondents",
        allow_empty_result=True,
    )
    validate_renewable_share = build_validate_bounds_task(
        "validate_resource_planning_renewable_share",
        "platinum.resource_planning_daily",
        "renewable_share_pct",
        min_value=0.0,
        max_value=100.0,
        where_clause=recent_where,
        description="resource planning renewable share",
        allow_empty_result=True,
    )
    validate_carbon_intensity = build_validate_bounds_task(
        "validate_resource_planning_carbon_intensity",
        "platinum.resource_planning_daily",
        "carbon_intensity_kg_per_mwh",
        min_value=0.0,
        where_clause=recent_where,
        description="resource planning carbon intensity",
        allow_empty_result=True,
    )

    [wait_for_region_gold, wait_for_fuel_gold] >> build_stage
    build_stage >> validate_stage_rows >> merge >> validate_rows >> validate_respondents >> validate_renewable_share >> validate_carbon_intensity
