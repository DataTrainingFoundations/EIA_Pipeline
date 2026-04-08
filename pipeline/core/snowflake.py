from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from snowflake.snowpark import Session
from snowflake.snowpark.types import DoubleType, LongType, StringType, StructField, StructType

from pipeline.core.settings import SnowflakeSettings
from pipeline.core.windowing import current_partition_date, month_anchor_date

META_SCHEMA = "META"
PIPELINE_STATE_TABLE = "PIPELINE_RUN_STATE"

_PIPELINE_STATE_COLUMNS = {
    "dataset_id": "STRING",
    "frequency": "STRING",
    "bootstrap_ingest_complete": "BOOLEAN",
    "bootstrap_transform_complete": "BOOLEAN",
    "last_raw_partition": "DATE",
    "last_silver_partition": "DATE",
    "last_gold_partition": "DATE",
    "last_ingest_started_at": "TIMESTAMP_NTZ",
    "last_ingest_succeeded_at": "TIMESTAMP_NTZ",
    "last_transform_started_at": "TIMESTAMP_NTZ",
    "last_transform_succeeded_at": "TIMESTAMP_NTZ",
    "last_ingest_error_message": "STRING",
    "last_transform_error_message": "STRING",
    "updated_at": "TIMESTAMP_NTZ",
}


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


def _state_table_name(database: str) -> str:
    return f"{database}.{META_SCHEMA}.{PIPELINE_STATE_TABLE}"


def ensure_pipeline_state_table(session, database: str) -> str:
    table_name = _state_table_name(database)
    session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            dataset_id STRING,
            frequency STRING,
            bootstrap_ingest_complete BOOLEAN,
            bootstrap_transform_complete BOOLEAN,
            last_raw_partition DATE,
            last_silver_partition DATE,
            last_gold_partition DATE,
            last_ingest_started_at TIMESTAMP_NTZ,
            last_ingest_succeeded_at TIMESTAMP_NTZ,
            last_transform_started_at TIMESTAMP_NTZ,
            last_transform_succeeded_at TIMESTAMP_NTZ,
            last_ingest_error_message STRING,
            last_transform_error_message STRING,
            updated_at TIMESTAMP_NTZ
        )
        """
    ).collect()
    for column_name, column_type in _PIPELINE_STATE_COLUMNS.items():
        session.sql(
            f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        ).collect()
    return table_name


def _default_pipeline_state(dataset_id: str) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "frequency": "",
        "bootstrap_ingest_complete": False,
        "bootstrap_transform_complete": False,
        "last_raw_partition": None,
        "last_silver_partition": None,
        "last_gold_partition": None,
        "last_ingest_started_at": None,
        "last_ingest_succeeded_at": None,
        "last_transform_started_at": None,
        "last_transform_succeeded_at": None,
        "last_ingest_error_message": None,
        "last_transform_error_message": None,
        "updated_at": None,
    }


def get_pipeline_state(session, database: str, dataset_id: str) -> dict[str, Any]:
    table_name = ensure_pipeline_state_table(session, database)
    rows = session.sql(
        f"SELECT * FROM {table_name} WHERE dataset_id = {_sql_literal(dataset_id)}"
    ).collect()
    if not rows:
        return _default_pipeline_state(dataset_id)
    row = rows[0].as_dict()
    state = _default_pipeline_state(dataset_id)
    state.update({key.lower(): value for key, value in row.items()})
    return state


def upsert_pipeline_state(
    session,
    database: str,
    dataset_id: str,
    *,
    frequency: str,
    bootstrap_ingest_complete: bool,
    bootstrap_transform_complete: bool,
    last_raw_partition: str | None,
    last_silver_partition: str | None,
    last_gold_partition: str | None,
    last_ingest_started_at: str | None,
    last_ingest_succeeded_at: str | None,
    last_transform_started_at: str | None,
    last_transform_succeeded_at: str | None,
    last_ingest_error_message: str | None,
    last_transform_error_message: str | None,
) -> None:
    table_name = ensure_pipeline_state_table(session, database)
    session.sql(
        f"""
        MERGE INTO {table_name} target
        USING (
            SELECT
                {_sql_literal(dataset_id)} AS dataset_id,
                {_sql_literal(frequency)} AS frequency,
                {str(bool(bootstrap_ingest_complete)).upper()} AS bootstrap_ingest_complete,
                {str(bool(bootstrap_transform_complete)).upper()} AS bootstrap_transform_complete,
                TO_DATE({_sql_literal(last_raw_partition)}) AS last_raw_partition,
                TO_DATE({_sql_literal(last_silver_partition)}) AS last_silver_partition,
                TO_DATE({_sql_literal(last_gold_partition)}) AS last_gold_partition,
                TO_TIMESTAMP_NTZ({_sql_literal(last_ingest_started_at)}) AS last_ingest_started_at,
                TO_TIMESTAMP_NTZ({_sql_literal(last_ingest_succeeded_at)}) AS last_ingest_succeeded_at,
                TO_TIMESTAMP_NTZ({_sql_literal(last_transform_started_at)}) AS last_transform_started_at,
                TO_TIMESTAMP_NTZ({_sql_literal(last_transform_succeeded_at)}) AS last_transform_succeeded_at,
                {_sql_literal(last_ingest_error_message)} AS last_ingest_error_message,
                {_sql_literal(last_transform_error_message)} AS last_transform_error_message,
                CURRENT_TIMESTAMP() AS updated_at
        ) source
        ON target.dataset_id = source.dataset_id
        WHEN MATCHED THEN UPDATE SET
            frequency = source.frequency,
            bootstrap_ingest_complete = source.bootstrap_ingest_complete,
            bootstrap_transform_complete = source.bootstrap_transform_complete,
            last_raw_partition = source.last_raw_partition,
            last_silver_partition = source.last_silver_partition,
            last_gold_partition = source.last_gold_partition,
            last_ingest_started_at = source.last_ingest_started_at,
            last_ingest_succeeded_at = source.last_ingest_succeeded_at,
            last_transform_started_at = source.last_transform_started_at,
            last_transform_succeeded_at = source.last_transform_succeeded_at,
            last_ingest_error_message = source.last_ingest_error_message,
            last_transform_error_message = source.last_transform_error_message,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            dataset_id,
            frequency,
            bootstrap_ingest_complete,
            bootstrap_transform_complete,
            last_raw_partition,
            last_silver_partition,
            last_gold_partition,
            last_ingest_started_at,
            last_ingest_succeeded_at,
            last_transform_started_at,
            last_transform_succeeded_at,
            last_ingest_error_message,
            last_transform_error_message,
            updated_at
        ) VALUES (
            source.dataset_id,
            source.frequency,
            source.bootstrap_ingest_complete,
            source.bootstrap_transform_complete,
            source.last_raw_partition,
            source.last_silver_partition,
            source.last_gold_partition,
            source.last_ingest_started_at,
            source.last_ingest_succeeded_at,
            source.last_transform_started_at,
            source.last_transform_succeeded_at,
            source.last_ingest_error_message,
            source.last_transform_error_message,
            source.updated_at
        )
        """
    ).collect()


def update_ingest_state(
    session,
    database: str,
    dataset_id: str,
    *,
    frequency: str,
    bootstrap_ingest_complete: bool,
    last_raw_partition: str | None,
    last_ingest_started_at: str | None,
    last_ingest_succeeded_at: str | None,
    last_ingest_error_message: str | None,
) -> None:
    prior_state = get_pipeline_state(session, database, dataset_id)
    upsert_pipeline_state(
        session,
        database,
        dataset_id,
        frequency=frequency,
        bootstrap_ingest_complete=bootstrap_ingest_complete,
        bootstrap_transform_complete=bool(prior_state.get("bootstrap_transform_complete")),
        last_raw_partition=last_raw_partition or _normalize_result_value(prior_state.get("last_raw_partition")),
        last_silver_partition=_normalize_result_value(prior_state.get("last_silver_partition")),
        last_gold_partition=_normalize_result_value(prior_state.get("last_gold_partition")),
        last_ingest_started_at=last_ingest_started_at,
        last_ingest_succeeded_at=last_ingest_succeeded_at,
        last_transform_started_at=_normalize_result_value(prior_state.get("last_transform_started_at")),
        last_transform_succeeded_at=_normalize_result_value(prior_state.get("last_transform_succeeded_at")),
        last_ingest_error_message=last_ingest_error_message,
        last_transform_error_message=prior_state.get("last_transform_error_message"),
    )


def update_transform_state(
    session,
    database: str,
    dataset_id: str,
    *,
    frequency: str,
    bootstrap_transform_complete: bool,
    last_silver_partition: str | None,
    last_gold_partition: str | None,
    last_transform_started_at: str | None,
    last_transform_succeeded_at: str | None,
    last_transform_error_message: str | None,
) -> None:
    prior_state = get_pipeline_state(session, database, dataset_id)
    upsert_pipeline_state(
        session,
        database,
        dataset_id,
        frequency=frequency,
        bootstrap_ingest_complete=bool(prior_state.get("bootstrap_ingest_complete")),
        bootstrap_transform_complete=bootstrap_transform_complete,
        last_raw_partition=_normalize_result_value(prior_state.get("last_raw_partition")),
        last_silver_partition=last_silver_partition or _normalize_result_value(prior_state.get("last_silver_partition")),
        last_gold_partition=last_gold_partition or _normalize_result_value(prior_state.get("last_gold_partition")),
        last_ingest_started_at=_normalize_result_value(prior_state.get("last_ingest_started_at")),
        last_ingest_succeeded_at=_normalize_result_value(prior_state.get("last_ingest_succeeded_at")),
        last_transform_started_at=last_transform_started_at,
        last_transform_succeeded_at=last_transform_succeeded_at,
        last_ingest_error_message=prior_state.get("last_ingest_error_message"),
        last_transform_error_message=last_transform_error_message,
    )


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
        normalized_start = month_anchor_date(start_date) if frequency == "monthly" else start_date
        clauses.append(f"{partition_expr} >= TO_DATE({_sql_literal(normalized_start)})")
    if end_date:
        normalized_end = month_anchor_date(end_date) if frequency == "monthly" else end_date
        clauses.append(f"{partition_expr} <= TO_DATE({_sql_literal(normalized_end)})")
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
    if not table_exists(session, table_name):
        return []
    if frequency != "hourly":
        return list_partition_status(
            session,
            table_name,
            frequency=frequency,
            processed_column="_INGESTED_AT",
            start_date=start_date,
            end_date=end_date,
        )

    partition_expr = raw_partition_expression(frequency)
    clauses = [f"{partition_expr} IS NOT NULL"]
    if start_date:
        clauses.append(f"{partition_expr} >= TO_DATE({_sql_literal(start_date)})")
    if end_date:
        clauses.append(f"{partition_expr} <= TO_DATE({_sql_literal(end_date)})")
    where_sql = " AND ".join(clauses)
    current_partition = current_partition_for_frequency(frequency)
    rows = session.sql(
        f"""
        SELECT
            {partition_expr} AS partition_date,
            MAX(_INGESTED_AT) AS processed_at,
            COUNT(*) AS row_count,
            COUNT(DISTINCT PERIOD) AS distinct_period_count
        FROM {table_name}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
        """
    ).collect()
    results: list[dict[str, Any]] = []
    for row in rows:
        partition_date = _normalize_result_value(row["PARTITION_DATE"])
        if not partition_date:
            continue
        distinct_period_count = int(row["DISTINCT_PERIOD_COUNT"] or 0)
        results.append(
            {
                "partition_date": partition_date,
                "processed_at": _normalize_result_value(row["PROCESSED_AT"]),
                "row_count": int(row["ROW_COUNT"]),
                "distinct_period_count": distinct_period_count,
                # Historical hourly dates are only considered publishable when all 24 hours are present.
                "is_complete": partition_date >= current_partition or distinct_period_count >= 24,
            }
        )
    return results


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
