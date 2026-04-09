"""Shared Snowflake access helpers for the EIA Streamlit app."""

from __future__ import annotations

import pandas as pd
import snowflake.connector
import streamlit as st

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
FAST_CACHE_TTL = 60
SLOW_CACHE_TTL = 300


@st.cache_resource
def get_app_snowflake_settings():
    return load_app_snowflake_settings()


def _connection_kwargs() -> dict[str, object]:
    settings = get_app_snowflake_settings()
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


def _run_sql(query: str) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            frame = cur.fetch_pandas_all()
            frame.columns = [column.lower() for column in frame.columns]
            return frame


@st.cache_data(ttl=FAST_CACHE_TTL, show_spinner=False)
def _safe_read_sql_fast_cached(query: str) -> pd.DataFrame:
    return _run_sql(query)


@st.cache_data(ttl=SLOW_CACHE_TTL, show_spinner=False)
def _safe_read_sql_slow_cached(query: str) -> pd.DataFrame:
    return _run_sql(query)


def _safe_read_sql(query: str, ttl: int = FAST_CACHE_TTL) -> pd.DataFrame:
    if ttl >= SLOW_CACHE_TTL:
        return _safe_read_sql_slow_cached(query)
    return _safe_read_sql_fast_cached(query)


@st.cache_data(ttl=FAST_CACHE_TTL, show_spinner=False)
def get_connection_status() -> tuple[bool, object]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select current_timestamp()")
            return True, cur.fetchone()[0]


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_in_list(values: list[str]) -> str:
    return ", ".join(sql_literal(value) for value in values)


def qualified_table(schema: str, table_name: str) -> str:
    settings = load_app_snowflake_settings()
    return f"{settings.database}.{schema}.{table_name}"
