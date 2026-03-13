"""Generation query helpers for the EIA Streamlit app."""

from __future__ import annotations
from typing import Any

import pandas as pd
import streamlit as st

from data_access_shared import (
    FACT_GENERATION_HOURLY,
    AGG_DAILY_GENERATION,
    _safe_read_sql,
)


@st.cache_data(ttl=60)
def load_generation_hourly(
    start_ts: str | None = None,
    end_ts: str | None = None,
    ba_codes: list[str] | None = None,
    fuel_codes: list[str] | None = None,
) -> pd.DataFrame:
    query = f"select * from {FACT_GENERATION_HOURLY} where 1=1"
    params: list[Any] = []
    if start_ts:
        query += " and period_ts >= %s"
        params.append(start_ts)
    if end_ts:
        query += " and period_ts <= %s"
        params.append(end_ts)
    if ba_codes:
        query += " and ba_code = any(%s)"
        params.append(ba_codes)
    if fuel_codes:
        query += " and fuel_code = any(%s)"
        params.append(fuel_codes)
    query += " order by period_ts, ba_code, fuel_code"
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_daily_generation(
    start_date: str | None = None,
    end_date: str | None = None,
    fuel_codes: list[str] | None = None,
) -> pd.DataFrame:
    query = f"select * from {AGG_DAILY_GENERATION} where 1=1"
    params: list[Any] = []
    if start_date:
        query += " and report_date >= %s"
        params.append(start_date)
    if end_date:
        query += " and report_date <= %s"
        params.append(end_date)
    if fuel_codes:
        query += " and fuel_code = any(%s)"
        params.append(fuel_codes)
    query += " order by report_date, fuel_code"
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_latest_generation_snapshot(
    start_ts: str | None = None,
    end_ts: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    """Latest hourly period's generation totalled by BA."""
    query = f"""
    with filtered as (
        select *
        from {FACT_GENERATION_HOURLY}
        where 1=1
    """
    params: list[Any] = []
    if start_ts:
        query += " and period_ts >= %s"
        params.append(start_ts)
    if end_ts:
        query += " and period_ts <= %s"
        params.append(end_ts)
    if ba_codes:
        query += " and ba_code = any(%s)"
        params.append(ba_codes)
    query += """
    ),
    latest as (select max(period_ts) as period_ts from filtered)
    select
        f.ba_code,
        f.ba_name,
        f.period_ts,
        sum(f.generation_gwh)                                               as total_gwh,
        sum(case when f.fuel_code in ('WND','SUN','WAT') then f.generation_gwh else 0 end)
            / nullif(sum(f.generation_gwh), 0) * 100                        as renewable_pct,
        sum(case when f.fuel_code in ('NG','COL') then f.generation_gwh else 0 end)
            / nullif(sum(f.generation_gwh), 0) * 100                        as fossil_pct,
        sum(case when f.fuel_code = 'NG' then f.generation_gwh else 0 end)
            / nullif(sum(f.generation_gwh), 0) * 100                        as gas_pct
    from filtered f
    join latest on f.period_ts = latest.period_ts
    group by f.ba_code, f.ba_name, f.period_ts
    order by total_gwh desc
    """
    return _safe_read_sql(query, params)
