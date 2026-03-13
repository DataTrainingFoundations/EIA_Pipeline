"""Business-serving Airflow DAG builders for platinum marts."""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline_builders import (
    build_first_backfill_sensor,
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
    GRID_OPERATIONS_ALERT_COLUMNS,
    GRID_OPERATIONS_COLUMNS,
    GRID_OPERATIONS_SCHEDULE,
    RESOURCE_PLANNING_COLUMNS,
    RESOURCE_PLANNING_DAILY_SCHEDULE,
    SPARK_PACKAGES_WITH_POSTGRES,
)
from pipeline_support import build_spark_submit_command

PLATINUM_DEFAULT_ARGS = {"retries": 1, "retry_delay": timedelta(minutes=5)}


def build_grid_operations_dag() -> DAG:
    """Build the hourly grid-operations platinum DAG."""

    with DAG(
        dag_id="platinum_grid_operations_hourly",
        start_date=datetime(2024, 1, 1),
        schedule=GRID_OPERATIONS_SCHEDULE,
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        default_args=PLATINUM_DEFAULT_ARGS,
        tags=["platinum", "grid-operations", "hourly"],
    ) as dag:
        stage_table = "platinum.grid_operations_hourly_stage_{{ ts_nodash | lower }}"
        alert_stage_table = "platinum.grid_operations_alert_hourly_stage_{{ ts_nodash | lower }}"
        lookback_start_expr = "{{ (data_interval_end.in_timezone('UTC') - macros.timedelta(days=14)).isoformat() }}"
        end_expr = "{{ data_interval_end.in_timezone('UTC').isoformat() }}"
        wait_for_region_first_backfill = build_first_backfill_sensor("wait_for_region_first_backfill", "electricity_region_data")
        wait_for_fuel_first_backfill = build_first_backfill_sensor("wait_for_fuel_first_backfill", "electricity_fuel_type_data")
        wait_for_region_gold = build_curated_gold_sensor("wait_for_region_curated_gold", "electricity_region_data")
        wait_for_fuel_gold = build_curated_gold_sensor("wait_for_fuel_curated_gold", "electricity_fuel_type_data")

        build_stage = BashOperator(
            task_id="build_grid_operations_hourly_stage",
            bash_command=build_spark_submit_command(
                "platinum_grid_operations_hourly.py",
                packages=SPARK_PACKAGES_WITH_POSTGRES,
                job_name="platinum_grid_operations_hourly",
                application_args=[
                    "--region-fact-path",
                    GOLD_REGION_FACT_HOURLY_PATH,
                    "--fuel-fact-path",
                    GOLD_FUEL_FACT_HOURLY_PATH,
                    "--ops-stage-table",
                    stage_table,
                    "--alerts-stage-table",
                    alert_stage_table,
                    "--start",
                    lookback_start_expr,
                    "--end",
                    end_expr,
                ],
            ),
        )
        validate_stage_rows = build_validate_rows_task("validate_grid_operations_stage_rows", stage_table, description="grid operations stage rows", allow_missing_table=True)
        merge_status = build_merge_task(
            "merge_grid_operations_hourly_stage",
            "platinum.grid_operations_hourly",
            stage_table,
            GRID_OPERATIONS_COLUMNS,
            ["period", "respondent"],
        )
        merge_alerts = build_merge_task(
            "merge_grid_operations_alert_stage",
            "platinum.grid_operations_alert_hourly",
            alert_stage_table,
            GRID_OPERATIONS_ALERT_COLUMNS,
            ["period", "respondent", "alert_type"],
        )
        recent_where = "period >= now() at time zone 'utc' - interval '14 days'"
        validate_rows = build_validate_rows_task(
            "validate_grid_operations_rows",
            "platinum.grid_operations_hourly",
            where_clause=recent_where,
            description="grid operations recent rows",
            allow_empty_result=True,
        )
        validate_respondents = build_validate_distinct_task(
            "validate_grid_operations_respondents",
            "platinum.grid_operations_hourly",
            "respondent",
            where_clause=recent_where,
            description="grid operations recent respondents",
            allow_empty_result=True,
        )
        validate_coverage_ratio = build_validate_bounds_task(
            "validate_grid_operations_coverage_ratio",
            "platinum.grid_operations_hourly",
            "coverage_ratio",
            min_value=0.0,
            where_clause=recent_where,
            description="grid operations coverage ratio",
            allow_empty_result=True,
        )
        validate_renewable_share = build_validate_bounds_task(
            "validate_grid_operations_renewable_share",
            "platinum.grid_operations_hourly",
            "renewable_share_pct",
            min_value=0.0,
            max_value=100.0,
            where_clause=recent_where,
            description="grid operations renewable share",
            allow_empty_result=True,
        )

        wait_for_region_first_backfill >> wait_for_fuel_first_backfill >> wait_for_region_gold
        wait_for_fuel_first_backfill >> wait_for_fuel_gold
        [wait_for_region_gold, wait_for_fuel_gold] >> build_stage
        build_stage >> validate_stage_rows >> merge_status >> merge_alerts >> validate_rows >> validate_respondents >> validate_coverage_ratio >> validate_renewable_share

    return dag


def build_resource_planning_dag() -> DAG:
    """Build the daily resource-planning platinum DAG."""

    with DAG(
        dag_id="platinum_resource_planning_daily",
        start_date=datetime(2024, 1, 1),
        schedule=RESOURCE_PLANNING_DAILY_SCHEDULE,
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        default_args=PLATINUM_DEFAULT_ARGS,
        tags=["platinum", "resource-planning", "daily"],
    ) as dag:
        stage_table = "platinum.resource_planning_daily_stage_{{ ts_nodash | lower }}"
        lookback_start_expr = "{{ (data_interval_end.in_timezone('UTC') - macros.timedelta(days=35)).isoformat() }}"
        end_expr = "{{ data_interval_end.in_timezone('UTC').isoformat() }}"
        recent_where = "date >= current_date - interval '35 days'"
        wait_for_region_first_backfill = build_first_backfill_sensor("wait_for_region_first_backfill", "electricity_region_data")
        wait_for_fuel_first_backfill = build_first_backfill_sensor("wait_for_fuel_first_backfill", "electricity_fuel_type_data")
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
        merge = build_merge_task(
            "merge_resource_planning_daily_stage",
            "platinum.resource_planning_daily",
            stage_table,
            RESOURCE_PLANNING_COLUMNS,
            ["date", "respondent"],
        )
        validate_stage_rows = build_validate_rows_task("validate_resource_planning_stage_rows", stage_table, description="resource planning stage rows", allow_missing_table=True)
        validate_rows = build_validate_rows_task(
            "validate_resource_planning_rows",
            "platinum.resource_planning_daily",
            where_clause=recent_where,
            description="resource planning recent rows",
            allow_empty_result=True,
        )
        validate_respondents = build_validate_distinct_task(
            "validate_resource_planning_respondents",
            "platinum.resource_planning_daily",
            "respondent",
            where_clause=recent_where,
            description="resource planning recent respondents",
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

        wait_for_region_first_backfill >> wait_for_fuel_first_backfill >> wait_for_region_gold
        wait_for_fuel_first_backfill >> wait_for_fuel_gold
        [wait_for_region_gold, wait_for_fuel_gold] >> build_stage
        build_stage >> validate_stage_rows >> merge >> validate_rows >> validate_respondents >> validate_renewable_share >> validate_carbon_intensity

    return dag
