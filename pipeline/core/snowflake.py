from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from snowflake.snowpark import Session
from snowflake.snowpark.types import DoubleType, LongType, StringType, StructField, StructType

from pipeline.core.settings import SnowflakeSettings
from pipeline.core.windowing import current_partition_date, month_anchor_date

META_SCHEMA = "META"
PIPELINE_STATE_TABLE = "PIPELINE_RUN_STATE"


def get_snowpark_session(settings: SnowflakeSettings):
    return Session.builder.configs(
        {
            "account": settings.account,
            "user": settings.user,
            "password": settings.password,
            "role": settings.role,
            "warehouse": settings.warehouse,
            "database": settings.database,
            "schema": settings.schema,
        }
    ).create()


def close_session(session) -> None:
    session.close()


def normalize_record_keys(record: dict[str, Any]) -> dict[str, Any]:
    return {key.upper().replace("-", "_"): value for key, value in record.items()}


def _snowpark_type(type_name: str):
    normalized = (type_name or "string").strip().lower()
    if normalized in {"float", "double", "number"}:
        return DoubleType()
    if normalized in {"integer", "int", "long"}:
        return LongType()
    return StringType()


def _coerce_raw_value(value: Any, declared_type: str) -> Any:
    if value is None:
        return None
    normalized = (declared_type or "string").strip().lower()
    if normalized in {"float", "double", "number"}:
        return float(value)
    if normalized in {"integer", "int", "long"}:
        return int(value)
    return str(value) if normalized == "string" else value


def _build_raw_struct_type(column_names: list[str], declared_schema: dict[str, Any]) -> StructType:
    normalized_schema = {
        key.upper().replace("-", "_"): value for key, value in (declared_schema or {}).items()
    }
    fields = []
    for column_name in column_names:
        declared_type = normalized_schema.get(column_name, "string")
        fields.append(StructField(column_name, _snowpark_type(str(declared_type)), nullable=True))
    return StructType(fields)


def write_raw_records(
    session,
    table_name: str,
    records: list[dict[str, Any]],
    declared_schema: dict[str, Any] | None = None,
) -> int:
    if not records:
        return 0
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    normalized = []
    for record in records:
        row = normalize_record_keys(record)
        row["_INGESTED_AT"] = ingested_at
        normalized.append(row)
    column_names: list[str] = []
    seen = set()
    for row in normalized:
        for key in row:
            if key not in seen:
                seen.add(key)
                column_names.append(key)
    normalized_schema = {
        key.upper().replace("-", "_"): str(value) for key, value in (declared_schema or {}).items()
    }
    rows = [
        [
            _coerce_raw_value(row.get(column_name), normalized_schema.get(column_name, "string"))
            for column_name in column_names
        ]
        for row in normalized
    ]
    struct_type = _build_raw_struct_type(column_names, declared_schema or {})
    df = session.create_dataframe(rows, schema=struct_type)
    df.write.mode("append").save_as_table(table_name, table_type="transient", column_order="name")
    return len(normalized)


def rows_exist_for_business_date(session, table_name: str, target_date: str, *, frequency: str) -> bool:
    try:
        if frequency == "monthly":
            target_month = month_anchor_date(target_date)[:7]
            query = f"SELECT COUNT(*) AS n FROM {table_name} WHERE PERIOD = '{target_month}'"
        else:
            query = (
                "SELECT COUNT(*) AS n "
                f"FROM {table_name} "
                f"WHERE TRY_TO_DATE(SUBSTR(PERIOD, 1, 10)) = '{target_date}'"
            )
        result = session.sql(query).collect()
        return bool(result and result[0]["N"] > 0)
    except Exception:
        return False


def table_has_rows_for_partition_date(session, table_name: str, target_date: str) -> bool:
    try:
        result = session.sql(
            f"SELECT COUNT(*) AS n FROM {table_name} WHERE partition_date = '{target_date}'"
        ).collect()
        return bool(result and result[0]["N"] > 0)
    except Exception:
        return False


def table_exists(session, table_name: str) -> bool:
    try:
        session.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False


def _sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def ensure_pipeline_state_table(session, database: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {database}.{META_SCHEMA}").collect()
    table_name = f"{database}.{META_SCHEMA}.{PIPELINE_STATE_TABLE}"
    session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            dataset_id STRING,
            frequency STRING,
            bootstrap_complete BOOLEAN,
            last_raw_partition DATE,
            last_silver_partition DATE,
            last_gold_partition DATE,
            last_run_started_at TIMESTAMP_NTZ,
            last_run_succeeded_at TIMESTAMP_NTZ,
            last_error_message STRING,
            updated_at TIMESTAMP_NTZ
        )
        """
    ).collect()
    return table_name


def get_pipeline_state(session, database: str, dataset_id: str) -> dict[str, Any]:
    table_name = ensure_pipeline_state_table(session, database)
    rows = session.sql(
        f"SELECT * FROM {table_name} WHERE dataset_id = {_sql_literal(dataset_id)}"
    ).collect()
    if not rows:
        return {
            "dataset_id": dataset_id,
            "frequency": "",
            "bootstrap_complete": False,
            "last_raw_partition": None,
            "last_silver_partition": None,
            "last_gold_partition": None,
            "last_run_started_at": None,
            "last_run_succeeded_at": None,
            "last_error_message": None,
            "updated_at": None,
        }
    row = rows[0].as_dict()
    return {key.lower(): value for key, value in row.items()}


def upsert_pipeline_state(
    session,
    database: str,
    dataset_id: str,
    *,
    frequency: str,
    bootstrap_complete: bool,
    last_raw_partition: str | None,
    last_silver_partition: str | None,
    last_gold_partition: str | None,
    last_run_started_at: str | None,
    last_run_succeeded_at: str | None,
    last_error_message: str | None,
) -> None:
    table_name = ensure_pipeline_state_table(session, database)
    session.sql(
        f"""
        MERGE INTO {table_name} target
        USING (
            SELECT
                {_sql_literal(dataset_id)} AS dataset_id,
                {_sql_literal(frequency)} AS frequency,
                {str(bool(bootstrap_complete)).upper()} AS bootstrap_complete,
                TO_DATE({_sql_literal(last_raw_partition)}) AS last_raw_partition,
                TO_DATE({_sql_literal(last_silver_partition)}) AS last_silver_partition,
                TO_DATE({_sql_literal(last_gold_partition)}) AS last_gold_partition,
                TO_TIMESTAMP_NTZ({_sql_literal(last_run_started_at)}) AS last_run_started_at,
                TO_TIMESTAMP_NTZ({_sql_literal(last_run_succeeded_at)}) AS last_run_succeeded_at,
                {_sql_literal(last_error_message)} AS last_error_message,
                CURRENT_TIMESTAMP() AS updated_at
        ) source
        ON target.dataset_id = source.dataset_id
        WHEN MATCHED THEN UPDATE SET
            frequency = source.frequency,
            bootstrap_complete = source.bootstrap_complete,
            last_raw_partition = source.last_raw_partition,
            last_silver_partition = source.last_silver_partition,
            last_gold_partition = source.last_gold_partition,
            last_run_started_at = source.last_run_started_at,
            last_run_succeeded_at = source.last_run_succeeded_at,
            last_error_message = source.last_error_message,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            dataset_id,
            frequency,
            bootstrap_complete,
            last_raw_partition,
            last_silver_partition,
            last_gold_partition,
            last_run_started_at,
            last_run_succeeded_at,
            last_error_message,
            updated_at
        ) VALUES (
            source.dataset_id,
            source.frequency,
            source.bootstrap_complete,
            source.last_raw_partition,
            source.last_silver_partition,
            source.last_gold_partition,
            source.last_run_started_at,
            source.last_run_succeeded_at,
            source.last_error_message,
            source.updated_at
        )
        """
    ).collect()


def raw_partition_expression(frequency: str) -> str:
    if frequency == "monthly":
        return "TO_DATE(PERIOD || '-01')"
    return "TRY_TO_DATE(SUBSTR(PERIOD, 1, 10))"


def list_partition_status(
    session,
    table_name: str,
    *,
    frequency: str,
    processed_column: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    partition_expr = "partition_date" if processed_column != "_INGESTED_AT" else raw_partition_expression(frequency)
    clauses = [f"{partition_expr} IS NOT NULL"]
    if start_date:
        clauses.append(f"{partition_expr} >= TO_DATE({_sql_literal(month_anchor_date(start_date) if frequency == 'monthly' else start_date)})")
    if end_date:
        clauses.append(f"{partition_expr} <= TO_DATE({_sql_literal(month_anchor_date(end_date) if frequency == 'monthly' else end_date)})")
    where_sql = " AND ".join(clauses)
    rows = session.sql(
        f"""
        SELECT
            {partition_expr} AS partition_date,
            MAX({processed_column}) AS processed_at,
            COUNT(*) AS row_count
        FROM {table_name}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
        """
    ).collect()
    return [
        {
            "partition_date": _normalize_result_value(row["PARTITION_DATE"]),
            "processed_at": _normalize_result_value(row["PROCESSED_AT"]),
            "row_count": int(row["ROW_COUNT"]),
        }
        for row in rows
    ]


def raw_partitions_for_dataset(
    session,
    table_name: str,
    *,
    frequency: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    return list_partition_status(
        session,
        table_name,
        frequency=frequency,
        processed_column="_INGESTED_AT",
        start_date=start_date,
        end_date=end_date,
    )


def pipeline_partitions_for_table(
    session,
    table_name: str,
    *,
    frequency: str,
    processed_column: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, dict[str, Any]]:
    if not table_exists(session, table_name):
        return {}
    return {
        item["partition_date"]: item
        for item in list_partition_status(
            session,
            table_name,
            frequency=frequency,
            processed_column=processed_column,
            start_date=start_date,
            end_date=end_date,
        )
        if item["partition_date"]
    }


def current_partition_for_frequency(frequency: str) -> str:
    return current_partition_date(frequency)


def _normalize_result_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
