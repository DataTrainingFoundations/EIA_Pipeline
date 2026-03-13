"""Dataset-focused Airflow DAG builders.

These builders create the ingestion, Bronze, Silver, Gold, backfill,
verification, and repair DAGs for each dataset in the registry.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

from pipeline_builders import (
    build_bronze_command,
    build_bronze_verification_command,
    bronze_write_pool,
    global_backfill_pool,
    build_curated_gold_command,
    build_fetch_command,
    build_merge_task,
    build_region_daily_platinum_command,
    build_silver_command,
    build_validate_bounds_task,
    build_validate_distinct_task,
    build_validate_rows_task,
)
from pipeline_constants import (
    BACKFILL_SCHEDULE,
    BRONZE_HOURLY_COVERAGE_COLUMNS,
    BRONZE_REPAIR_SCHEDULE,
    BRONZE_VERIFICATION_SCHEDULE,
    REGION_DAILY_COLUMNS,
)
from pipeline_support import (
    claim_next_backfill_chunk,
    claim_next_bronze_repair_hour,
    enqueue_backfill_jobs,
    enqueue_bronze_repair_jobs,
    has_backfill_chunk,
    has_repair_chunk,
    mark_backfill_completed,
    mark_backfill_failed,
    mark_bronze_repair_completed,
    mark_bronze_repair_failed,
    trigger_backfill_dag_if_idle,
    trigger_repair_dag_if_idle,
)

DATASET_DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=5)}
BACKFILL_DEFAULT_ARGS = {"retries": 1, "retry_delay": timedelta(minutes=5)}


def build_incremental_dag(dataset_id: str, dataset: dict[str, str]) -> DAG:
    """Build the hourly dataset DAG from ingestion through optional serving."""

    dag_id = f"{dataset_id}_incremental"
    has_serving = dataset_id == "electricity_region_data" and bool(dataset.get("platinum_table"))
    has_curated_gold = dataset_id in {"electricity_region_data", "electricity_fuel_type_data"}

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule="@hourly",
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        default_args=DATASET_DEFAULT_ARGS,
        tags=["eia", "pipeline", dataset_id, "incremental"],
    ) as dag:
        start_expr = "{{ data_interval_start.in_timezone('UTC').isoformat() }}"
        end_expr = "{{ data_interval_end.in_timezone('UTC').isoformat() }}"
        cli_start_expr = "{{ data_interval_start.in_timezone('UTC').strftime('%Y-%m-%dT%H') }}"
        cli_end_expr = "{{ data_interval_end.in_timezone('UTC').strftime('%Y-%m-%dT%H') }}"

        ingest = BashOperator(
            task_id="ingest_to_kafka",
            bash_command=build_fetch_command(dataset_id, cli_start_expr, cli_end_expr, max_pages=20),
        )
        bronze = BashOperator(
            task_id="spark_bronze_batch",
            bash_command=build_bronze_command(dataset),
            pool=bronze_write_pool(dataset_id),
        )
        silver = BashOperator(
            task_id="spark_silver_batch",
            bash_command=build_silver_command(dataset, dataset_id, start_expr, end_expr),
        )

        ingest >> bronze >> silver
        upstream = silver

        if has_curated_gold:
            curated_gold = BashOperator(
                task_id="spark_curated_gold_batch",
                bash_command=build_curated_gold_command(dataset_id, start_expr, end_expr),
            )
            upstream >> curated_gold
            upstream = curated_gold

        if has_serving:
            stage_table = "platinum.region_demand_daily_stage_{{ ts_nodash | lower }}"
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
            where_clause = "source_window_start = %s and source_window_end = %s"
            window_params = [start_expr, end_expr]
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
            trigger_backfill = PythonOperator(
                task_id="trigger_backfill_if_idle",
                python_callable=trigger_backfill_dag_if_idle,
                op_kwargs={"dataset_id": dataset_id},
            )
            upstream >> platinum >> validate_stage_rows >> merge >> validate_rows >> validate_distinct_respondents >> validate_positive_demand >> trigger_backfill
        else:
            trigger_backfill = PythonOperator(
                task_id="trigger_backfill_if_idle",
                python_callable=trigger_backfill_dag_if_idle,
                op_kwargs={"dataset_id": dataset_id},
            )
            upstream >> trigger_backfill

    return dag


def build_backfill_dag(dataset_id: str, dataset: dict[str, str]) -> DAG:
    """Build the newest-first historical backfill DAG for one dataset."""

    dag_id = f"{dataset_id}_backfill"
    has_serving = dataset_id == "electricity_region_data" and bool(dataset.get("platinum_table"))
    has_curated_gold = dataset_id in {"electricity_region_data", "electricity_fuel_type_data"}

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=BACKFILL_SCHEDULE,
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        default_args=BACKFILL_DEFAULT_ARGS,
        tags=["eia", "pipeline", dataset_id, "backfill"],
    ) as dag:
        claim_key = "claim_backfill_chunk"
        chunk_start_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_start_utc'] }}"
        chunk_end_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_end_utc'] }}"
        chunk_cli_start_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_start_cli'] }}"
        chunk_cli_end_expr = "{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['chunk_end_cli'] }}"

        enqueue = PythonOperator(task_id="enqueue_backfill_jobs", python_callable=enqueue_backfill_jobs, op_kwargs={"dataset_id": dataset_id})
        claim = PythonOperator(task_id=claim_key, python_callable=claim_next_backfill_chunk, op_kwargs={"dataset_id": dataset_id})
        has_work = ShortCircuitOperator(task_id="has_backfill_chunk", python_callable=has_backfill_chunk, op_args=[claim.output])
        ingest = BashOperator(
            task_id="ingest_backfill_chunk",
            bash_command=build_fetch_command(dataset_id, chunk_cli_start_expr, chunk_cli_end_expr, max_pages=50),
        )
        bronze = BashOperator(
            task_id="spark_bronze_backfill_batch",
            bash_command=build_bronze_command(dataset),
            pool=bronze_write_pool(dataset_id),
        )
        silver = BashOperator(
            task_id="spark_silver_backfill",
            bash_command=build_silver_command(dataset, dataset_id, chunk_start_expr, chunk_end_expr),
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
        trigger_next_after_complete = PythonOperator(
            task_id="trigger_next_backfill_if_idle",
            python_callable=trigger_backfill_dag_if_idle,
            op_kwargs={"dataset_id": dataset_id, "ignore_run_id": "{{ run_id }}"},
        )
        trigger_next_after_failure = PythonOperator(
            task_id="trigger_next_backfill_after_failure_if_idle",
            python_callable=trigger_backfill_dag_if_idle,
            op_kwargs={"dataset_id": dataset_id, "ignore_run_id": "{{ run_id }}"},
        )

        enqueue >> claim >> has_work >> ingest >> bronze >> silver
        downstream_failures = [ingest, bronze, silver]
        completion_anchor = silver
        backfill_pool = global_backfill_pool()
        for task in (ingest, bronze, silver):
            task.pool = backfill_pool

        if has_curated_gold:
            curated_gold = BashOperator(
                task_id="spark_curated_gold_backfill",
                bash_command=build_curated_gold_command(dataset_id, chunk_start_expr, chunk_end_expr),
                pool=backfill_pool,
            )
            silver >> curated_gold
            completion_anchor = curated_gold
            downstream_failures.append(curated_gold)

        if has_serving:
            stage_table = "platinum.region_demand_daily_stage_backfill_{{ ti.xcom_pull(task_ids='claim_backfill_chunk')['id'] }}"
            platinum = BashOperator(
                task_id="spark_platinum_backfill_stage",
                bash_command=build_region_daily_platinum_command(stage_table, chunk_start_expr, chunk_end_expr),
                pool=backfill_pool,
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
            where_clause = "source_window_start = %s and source_window_end = %s"
            window_params = [chunk_start_expr, chunk_end_expr]
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
            completion_anchor >> platinum >> validate_stage_rows >> merge >> validate_rows >> validate_distinct_respondents >> validate_positive_demand >> mark_complete >> trigger_next_after_complete
            downstream_failures.extend([platinum, validate_stage_rows, merge, validate_rows, validate_distinct_respondents, validate_positive_demand])
        else:
            completion_anchor >> mark_complete >> trigger_next_after_complete

        downstream_failures >> mark_failed >> trigger_next_after_failure

    return dag


def build_bronze_verification_dag(dataset_id: str, dataset: dict[str, str]) -> DAG:
    """Build the hourly Bronze coverage verification DAG for one dataset."""

    stage_suffix = "region" if dataset_id == "electricity_region_data" else "fuel"
    dag_id = f"{dataset_id}_bronze_hourly_verification"

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=BRONZE_VERIFICATION_SCHEDULE,
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        default_args=DATASET_DEFAULT_ARGS,
        tags=["eia", "bronze", "verification", dataset_id],
    ) as dag:
        stage_table = f"ops.bronze_hourly_coverage_stage_{stage_suffix}_{{{{ ts_nodash | lower }}}}"
        verify = BashOperator(
            task_id="build_bronze_hourly_coverage_stage",
            bash_command=build_bronze_verification_command(dataset_id, dataset, stage_table),
        )
        validate_stage_rows = build_validate_rows_task(
            "validate_bronze_hourly_coverage_stage_rows",
            stage_table,
            description=f"{dataset_id} bronze hourly coverage stage rows",
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
            params=[dataset_id],
            description=f"{dataset_id} bronze hourly coverage rows",
            allow_empty_result=True,
        )
        validate_distinct_hours = build_validate_distinct_task(
            "validate_bronze_hourly_coverage_distinct_hours",
            "ops.bronze_hourly_coverage",
            "hour_start_utc",
            where_clause="dataset_id = %s",
            params=[dataset_id],
            description=f"{dataset_id} bronze hourly coverage distinct hours",
            allow_empty_result=True,
        )
        validate_non_negative_counts = build_validate_bounds_task(
            "validate_bronze_hourly_coverage_nonnegative_counts",
            "ops.bronze_hourly_coverage",
            "observed_row_count",
            min_value=0.0,
            where_clause="dataset_id = %s",
            params=[dataset_id],
            description=f"{dataset_id} bronze hourly observed row counts",
            allow_empty_result=True,
        )
        enqueue_repairs = PythonOperator(
            task_id="enqueue_bronze_repair_candidates",
            python_callable=enqueue_bronze_repair_jobs,
            op_kwargs={"dataset_id": dataset_id, "statuses": ("missing",), "max_pending_override": 0},
        )
        trigger_repairs = PythonOperator(
            task_id="trigger_bronze_repair_if_idle",
            python_callable=trigger_repair_dag_if_idle,
            op_kwargs={"dataset_id": dataset_id},
        )

        verify >> validate_stage_rows >> merge >> validate_rows >> validate_distinct_hours >> validate_non_negative_counts >> enqueue_repairs >> trigger_repairs

    return dag


def build_bronze_repair_dag(dataset_id: str, dataset: dict[str, str]) -> DAG:
    """Build the chained Bronze repair DAG for one dataset."""

    dag_id = f"{dataset_id}_bronze_hourly_repair"
    has_serving = dataset_id == "electricity_region_data" and bool(dataset.get("platinum_table"))
    has_curated_gold = dataset_id in {"electricity_region_data", "electricity_fuel_type_data"}

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=BRONZE_REPAIR_SCHEDULE,
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        default_args=BACKFILL_DEFAULT_ARGS,
        tags=["eia", "bronze", "repair", dataset_id],
    ) as dag:
        claim_key = "claim_bronze_repair_hour"
        chunk_start_expr = "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['chunk_start_utc'] }}"
        chunk_end_expr = "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['chunk_end_utc'] }}"
        chunk_cli_start_expr = "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['chunk_start_cli'] }}"
        chunk_cli_end_expr = "{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['chunk_end_cli'] }}"

        enqueue = PythonOperator(task_id="enqueue_bronze_repair_jobs", python_callable=enqueue_bronze_repair_jobs, op_kwargs={"dataset_id": dataset_id})
        claim = PythonOperator(task_id=claim_key, python_callable=claim_next_bronze_repair_hour, op_kwargs={"dataset_id": dataset_id})
        has_work = ShortCircuitOperator(task_id="has_bronze_repair_hour", python_callable=has_repair_chunk, op_args=[claim.output])
        ingest = BashOperator(
            task_id="ingest_bronze_repair_hour",
            bash_command=build_fetch_command(dataset_id, chunk_cli_start_expr, chunk_cli_end_expr, max_pages=20),
        )
        bronze = BashOperator(
            task_id="spark_bronze_repair_batch",
            bash_command=build_bronze_command(dataset),
            pool=bronze_write_pool(dataset_id),
        )
        silver = BashOperator(
            task_id="spark_silver_repair",
            bash_command=build_silver_command(dataset, dataset_id, chunk_start_expr, chunk_end_expr),
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
            op_kwargs={"dataset_id": dataset_id, "ignore_run_id": "{{ run_id }}"},
        )
        trigger_next_after_failure = PythonOperator(
            task_id="trigger_next_bronze_repair_after_failure_if_idle",
            python_callable=trigger_repair_dag_if_idle,
            op_kwargs={"dataset_id": dataset_id, "ignore_run_id": "{{ run_id }}"},
        )

        enqueue >> claim >> has_work >> ingest >> bronze >> silver
        downstream_failures = [ingest, bronze, silver]
        completion_anchor = silver

        if has_curated_gold:
            curated_gold = BashOperator(
                task_id="spark_curated_gold_repair",
                bash_command=build_curated_gold_command(dataset_id, chunk_start_expr, chunk_end_expr),
            )
            silver >> curated_gold
            completion_anchor = curated_gold
            downstream_failures.append(curated_gold)

        if has_serving:
            stage_table = "platinum.region_demand_daily_stage_repair_{{ ti.xcom_pull(task_ids='claim_bronze_repair_hour')['id'] }}"
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
            where_clause = "source_window_start = %s and source_window_end = %s"
            window_params = [chunk_start_expr, chunk_end_expr]
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
            completion_anchor >> platinum >> validate_stage_rows >> merge >> validate_rows >> validate_distinct_respondents >> validate_positive_demand >> mark_complete >> trigger_next_after_complete
            downstream_failures.extend([platinum, validate_stage_rows, merge, validate_rows, validate_distinct_respondents, validate_positive_demand])
        else:
            completion_anchor >> mark_complete >> trigger_next_after_complete

        downstream_failures >> mark_failed >> trigger_next_after_failure

    return dag
