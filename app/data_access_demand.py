"""Demand query helpers for the EIA Streamlit app."""

from __future__ import annotations
from typing import Any

import pandas as pd
import streamlit as st

from data_access_shared import (
    FACT_DEMAND_HOURLY,
    AGG_DAILY_DEMAND_PEAK,
    _safe_read_sql,
)


@st.cache_data(ttl=60)
def load_demand_hourly(
    start_ts: str | None = None,
    end_ts: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    query = f"select * from {FACT_DEMAND_HOURLY} where 1=1"
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
    query += " order by period_ts, ba_code"
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_daily_demand_peak(
    start_date: str | None = None,
    end_date: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    query = f"select * from {AGG_DAILY_DEMAND_PEAK} where 1=1"
    params: list[Any] = []
    if start_date:
        query += " and report_date >= %s"
        params.append(start_date)
    if end_date:
        query += " and report_date <= %s"
        params.append(end_date)
    if ba_codes:
        query += " and ba_code = any(%s)"
        params.append(ba_codes)
    query += " order by report_date, ba_code"
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_latest_demand_snapshot(
    start_ts: str | None = None,
    end_ts: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    """Latest hourly period demand vs forecast per BA."""
    query = f"""
    with filtered as (
        select * from {FACT_DEMAND_HOURLY} where 1=1
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
        f.*,
        (f.demand_gwh - f.forecast_gwh)                         as forecast_error_gwh,
        case
            when f.forecast_gwh > 0
            then (f.demand_gwh - f.forecast_gwh) / f.forecast_gwh * 100
        end                                                      as forecast_error_pct
    from filtered f
    join latest on f.period_ts = latest.period_ts
    order by f.ba_code
    """
    return _safe_read_sql(query, params)
