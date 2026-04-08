"""Shared Snowflake access helpers for the EIA Streamlit app."""

from __future__ import annotations

import pandas as pd
import snowflake.connector

from pipeline.core.settings import load_app_snowflake_settings

FACT_GENERATION_HOURLY = "FACT_GENERATION_HOURLY"
FACT_DEMAND_HOURLY = "FACT_DEMAND_HOURLY"
AGG_DAILY_GENERATION = "AGG_DAILY_GENERATION"
AGG_DAILY_DEMAND_PEAK = "AGG_DAILY_DEMAND_PEAK"
DIM_BALANCING_AUTHORITY = "DIM_BALANCING_AUTHORITY"
DIM_FUEL_TYPE = "DIM_FUEL_TYPE"
SILVER_ELECTRICITY_RETAIL_SALES = "SILVER_ELECTRICITY_RETAIL_SALES"
SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA = "SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA"
FACT_SALES_MONTHLY = "FACT_SALES_MONTHLY"


def _connection_kwargs() -> dict[str, object]:
    settings = load_app_snowflake_settings()
    return {
        "account": settings.account,
        "user": settings.user,
        "password": settings.password,
        "role": settings.role,
        "warehouse": settings.warehouse,
        "database": settings.database,
        "schema": settings.schema,
    }


def get_connection():
    return snowflake.connector.connect(**_connection_kwargs())


def _safe_read_sql(query: str) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            frame = cur.fetch_pandas_all()
            frame.columns = [column.lower() for column in frame.columns]
            return frame


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_in_list(values: list[str]) -> str:
    return ", ".join(sql_literal(value) for value in values)


def qualified_table(schema: str, table_name: str) -> str:
    settings = load_app_snowflake_settings()
    return f"{settings.database}.{schema}.{table_name}"
