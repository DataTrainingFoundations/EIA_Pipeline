from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from snowflake.snowpark import Session

from pipeline.core.settings import SnowflakeSettings


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


def write_raw_records(session, table_name: str, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    normalized = []
    for record in records:
        row = normalize_record_keys(record)
        row["_INGESTED_AT"] = ingested_at
        normalized.append(row)
    df = session.create_dataframe(normalized)
    df.write.mode("append").save_as_table(table_name, table_type="transient", column_order="name")
    return len(normalized)


def rows_exist_for_date(session, table_name: str, target_date: str) -> bool:
    try:
        result = session.sql(
            f"SELECT COUNT(*) AS n FROM {table_name} WHERE TRY_TO_DATE(_FETCHED_AT) = '{target_date}'"
        ).collect()
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
