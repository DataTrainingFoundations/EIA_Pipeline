from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from snowflake.snowpark import Session
from snowflake.snowpark.types import DoubleType, LongType, StringType, StructField, StructType

from pipeline.core.settings import SnowflakeSettings
from pipeline.core.windowing import month_anchor_date


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
