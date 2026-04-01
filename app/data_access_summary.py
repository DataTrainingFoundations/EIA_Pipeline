"""Coverage and summary queries for the EIA Streamlit app."""

from __future__ import annotations
from typing import Any

import pandas as pd
import streamlit as st

from data_access_shared import (
    FACT_HOURLY,
    AGG_DAILY_GENERATION,
    AGG_DAILY_DEMAND_PEAK,
    DIM_BALANCING_AUTHORITY,
    DIM_FUEL_TYPE,
    _safe_read_sql,
    get_session,
)


@st.cache_data(ttl=60)
def table_has_rows(table_name: str = FACT_HOURLY) -> bool:
    """Return True if the given Snowflake table exists and contains at least one row."""
    try:
        df = _safe_read_sql(f"SELECT 1 AS n FROM {table_name} LIMIT 1")
        return len(df) > 0
    except Exception:
        return False


@st.cache_data(ttl=60)
def get_generation_coverage() -> dict[str, Any]:
    """Min/max period, row count, BA count and fuel type count from fact_hourly."""
    query = f"""
    SELECT
        MIN(period_ts)            AS min_period,
        MAX(period_ts)            AS max_period,
        COUNT(*)                  AS row_count,
        COUNT(DISTINCT ba_code)   AS ba_count,
        COUNT(DISTINCT fuel_code) AS fuel_count
    FROM {FACT_HOURLY}
    WHERE generation_gwh IS NOT NULL
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def get_demand_coverage() -> dict[str, Any]:
    """Min/max period, row count and BA count for demand rows in fact_hourly."""
    query = f"""
    SELECT
        MIN(period_ts)          AS min_period,
        MAX(period_ts)          AS max_period,
        COUNT(*)                AS row_count,
        COUNT(DISTINCT ba_code) AS ba_count
    FROM {FACT_HOURLY}
    WHERE demand_gwh IS NOT NULL
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def get_coverage() -> dict[str, Any]:
    """
    Combined coverage across the whole fact_hourly table — used by the home page.
    Returns min/max period, total rows, BA count and fuel type count.
    """
    query = f"""
    SELECT
        MIN(period_ts)            AS min_period,
        MAX(period_ts)            AS max_period,
        COUNT(*)                  AS row_count,
        COUNT(DISTINCT ba_code)   AS ba_count,
        COUNT(DISTINCT fuel_code) AS fuel_count
    FROM {FACT_HOURLY}
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def get_daily_generation_coverage() -> dict[str, Any]:
    query = f"""
    SELECT
        MIN(report_date) AS min_date,
        MAX(report_date) AS max_date,
        COUNT(*)         AS row_count
    FROM {AGG_DAILY_GENERATION}
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def list_ba_codes(table_name: str = FACT_HOURLY) -> list[str]:
    df = _safe_read_sql(
        f"SELECT DISTINCT ba_code FROM {table_name} ORDER BY ba_code"
    )
    return df["ba_code"].dropna().tolist()


@st.cache_data(ttl=60)
def list_fuel_codes() -> list[str]:
    df = _safe_read_sql(
        f"SELECT fuel_code FROM {DIM_FUEL_TYPE} ORDER BY fuel_code"
    )
    return df["fuel_code"].dropna().tolist()