"""Coverage and summary queries for the EIA Streamlit app."""

from __future__ import annotations

from contextlib import closing
from typing import Any

import streamlit as st

from data_access_shared import (
    AGG_DAILY_GENERATION,
    DIM_FUEL_TYPE,
    FACT_DEMAND_HOURLY,
    FACT_GENERATION_HOURLY,
    SILVER_ELECTRICITY_RETAIL_SALES,
    _safe_read_sql,
    qualified_table,
    get_connection,
)

MONTHLY_SALES_TABLE = qualified_table("SILVER", SILVER_ELECTRICITY_RETAIL_SALES)


@st.cache_data(ttl=60)
def table_has_rows(table_name: str = FACT_GENERATION_HOURLY) -> bool:
    query = f"select exists (select 1 from {table_name} limit 1)"
    try:
        with get_connection() as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(query)
                return bool(cur.fetchone()[0])
    except Exception:
        return False


@st.cache_data(ttl=60)
def get_generation_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(period_ts) as min_period,
        max(period_ts) as max_period,
        count(*) as row_count,
        count(distinct ba_code) as ba_count,
        count(distinct fuel_code) as fuel_count
    from {FACT_GENERATION_HOURLY}
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def get_demand_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(period_ts) as min_period,
        max(period_ts) as max_period,
        count(*) as row_count,
        count(distinct ba_code) as ba_count
    from {FACT_DEMAND_HOURLY}
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def get_daily_generation_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(report_date) as min_date,
        max(report_date) as max_date,
        count(*) as row_count
    from {AGG_DAILY_GENERATION}
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def get_monthly_sales_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(period) as min_period,
        max(period) as max_period,
        count(*) as row_count,
        count(distinct stateid) as state_count,
        count(distinct sectorid) as sector_count
    from {MONTHLY_SALES_TABLE}
    where sectorid != 'ALL'
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def list_ba_codes(table_name: str = FACT_GENERATION_HOURLY) -> list[str]:
    query = f"select distinct ba_code from {table_name} order by ba_code"
    df = _safe_read_sql(query)
    return df["ba_code"].dropna().tolist()


@st.cache_data(ttl=60)
def list_fuel_codes() -> list[str]:
    query = f"select fuel_code, fuel_name from {DIM_FUEL_TYPE} order by fuel_code"
    df = _safe_read_sql(query)
    return df["fuel_code"].dropna().tolist()
