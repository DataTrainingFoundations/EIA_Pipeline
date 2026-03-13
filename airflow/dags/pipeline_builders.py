from __future__ import annotations

from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.external_task import ExternalTaskSensor

from pipeline_constants import (
    GOLD_FUEL_FACT_HOURLY_PATH,
    GOLD_FUEL_TYPE_DIM_PATH,
    GOLD_REGION_FACT_HOURLY_PATH,
    GLOBAL_BACKFILL_POOL,
    GOLD_RESPONDENT_DIM_PATH,
    SPARK_PACKAGES,
    SPARK_PACKAGES_WITH_POSTGRES,
)
from pipeline_support import (
    build_spark_submit_command,
    has_completed_backfill,
    merge_stage_into_target,
    validate_distinct_values,
    validate_numeric_bounds,
    validate_table_has_rows,
)


def bronze_write_pool(dataset_id: str) -> str:
    return f"{dataset_id}_bronze_write"


def global_backfill_pool() -> str:
    return GLOBAL_BACKFILL_POOL


def build_fetch_command(dataset_id: str, start_expression: str, end_expression: str, max_pages: int) -> str:
    return (
        "PYTHONPATH=/workspace/ingestion "
        f"python -m src.fetch_eia --dataset {dataset_id} --start {start_expression} --end {end_expression} "
        f"--page-size 5000 --max-pages {max_pages}"
    )


def build_bronze_command(dataset: dict[str, str]) -> str:
    return (
        f"export KAFKA_STARTING_OFFSETS=earliest; "
        f"export BRONZE_OUTPUT_PATH={dataset['bronze_output_path']}; "
        f"export BRONZE_CHECKPOINT_PATH={dataset['bronze_checkpoint_path']}; "
        + build_spark_submit_command(
            "bronze_kafka_to_minio.py",
            packages=SPARK_PACKAGES,
            job_name="bronze_kafka_to_minio_batch",
            application_args=["--topic", dataset["topic"], "--trigger-available-now"],
        )
    )


def build_silver_command(
    dataset: dict[str, str],
    dataset_id: str,
    start_expression: str,
    end_expression: str,
    *,
    validation_only: bool = False,
) -> str:
    application_args = [
        "--bronze-path",
        dataset["bronze_output_path"],
        "--silver-base-path",
        "s3a://silver",
        "--dataset",
        dataset_id,
        "--start",
        start_expression,
        "--end",
        end_expression,
    ]
    if validation_only:
        application_args.append("--validation-only")
    return build_spark_submit_command(
        "silver_clean_transform.py",
        packages=SPARK_PACKAGES,
        job_name="silver_clean_transform",
        application_args=application_args,
    )


def build_curated_gold_command(dataset_id: str, start_expression: str, end_expression: str) -> str:
    return build_spark_submit_command(
        "gold_region_fuel_serving_hourly.py",
        packages=SPARK_PACKAGES,
        job_name="gold_region_fuel_serving_hourly",
        application_args=[
            "--silver-base-path",
            "s3a://silver",
            "--dataset",
            dataset_id,
            "--region-fact-path",
            GOLD_REGION_FACT_HOURLY_PATH,
            "--fuel-fact-path",
            GOLD_FUEL_FACT_HOURLY_PATH,
            "--respondent-dim-path",
            GOLD_RESPONDENT_DIM_PATH,
            "--fuel-dim-path",
            GOLD_FUEL_TYPE_DIM_PATH,
            "--start",
            start_expression,
            "--end",
            end_expression,
        ],
    )


def build_region_daily_platinum_command(stage_table: str, start_expression: str, end_expression: str) -> str:
    return build_spark_submit_command(
        "platinum_region_demand_daily.py",
        packages=SPARK_PACKAGES_WITH_POSTGRES,
        job_name="platinum_region_demand_daily",
        application_args=[
            "--gold-input-path",
            GOLD_REGION_FACT_HOURLY_PATH,
            "--platinum-table",
            "platinum.region_demand_daily",
            "--stage-table",
            stage_table,
            "--start",
            start_expression,
            "--end",
            end_expression,
        ],
    )


def build_bronze_verification_command(dataset_id: str, dataset: dict[str, str], stage_table: str) -> str:
    return build_spark_submit_command(
        "bronze_hourly_coverage_verify.py",
        packages=SPARK_PACKAGES_WITH_POSTGRES,
        job_name=f"bronze_hourly_coverage_verify_{dataset_id}",
        application_args=[
            "--dataset-id",
            dataset_id,
            "--bronze-path",
            dataset["bronze_output_path"],
            "--verification-stage-table",
            stage_table,
        ],
    )


def build_merge_task(task_id: str, target_table: str, stage_table: str, insert_columns: list[str], conflict_columns: list[str]) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=merge_stage_into_target,
        op_kwargs={
            "target_table": target_table,
            "stage_table": stage_table,
            "insert_columns": insert_columns,
            "conflict_columns": conflict_columns,
            "allow_missing_stage": True,
        },
    )


def build_validate_rows_task(
    task_id: str,
    table_name: str,
    *,
    description: str,
    where_clause: str | None = None,
    params: list[str] | None = None,
    min_rows: int = 1,
    allow_missing_table: bool = False,
    allow_empty_result: bool = False,
) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=validate_table_has_rows,
        op_kwargs={
            "table_name": table_name,
            "min_rows": min_rows,
            "where_clause": where_clause,
            "params": params or [],
            "description": description,
            "allow_missing_table": allow_missing_table,
            "allow_empty_result": allow_empty_result,
        },
    )


def build_validate_distinct_task(
    task_id: str,
    table_name: str,
    column_name: str,
    *,
    description: str,
    where_clause: str | None = None,
    params: list[str] | None = None,
    min_distinct: int = 1,
    allow_empty_result: bool = False,
) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=validate_distinct_values,
        op_kwargs={
            "table_name": table_name,
            "column_name": column_name,
            "min_distinct": min_distinct,
            "where_clause": where_clause,
            "params": params or [],
            "description": description,
            "allow_empty_result": allow_empty_result,
        },
    )


def build_validate_bounds_task(
    task_id: str,
    table_name: str,
    column_name: str,
    *,
    description: str,
    min_value: float | None = None,
    max_value: float | None = None,
    where_clause: str | None = None,
    params: list[str] | None = None,
    allow_empty_result: bool = False,
) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=validate_numeric_bounds,
        op_kwargs={
            "table_name": table_name,
            "column_name": column_name,
            "min_value": min_value,
            "max_value": max_value,
            "where_clause": where_clause,
            "params": params or [],
            "description": description,
            "allow_empty_result": allow_empty_result,
        },
    )


def map_to_hourly_incremental_run(logical_date, **_: object):  # noqa: ANN001
    return logical_date.replace(minute=0, second=0, microsecond=0)


def build_curated_gold_sensor(task_id: str, upstream_dataset_id: str) -> ExternalTaskSensor:
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=f"{upstream_dataset_id}_incremental",
        external_task_id="spark_curated_gold_batch",
        execution_date_fn=map_to_hourly_incremental_run,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        # Deferrable mode avoids the BaseSensorOperator super().execute path that
        # logs misleading "cannot be called outside TaskInstance" warnings in
        # Airflow 2.9 for ExternalTaskSensor.
        deferrable=True,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )


def build_first_backfill_sensor(task_id: str, dataset_id: str) -> PythonSensor:
    return PythonSensor(
        task_id=task_id,
        python_callable=has_completed_backfill,
        op_kwargs={"dataset_id": dataset_id},
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
    )
