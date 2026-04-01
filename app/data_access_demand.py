"""Demand query helpers for the EIA Streamlit app."""

from __future__ import annotations
from typing import Any

import pandas as pd
import streamlit as st

from data_access_shared import (
    FACT_HOURLY,
    AGG_DAILY_DEMAND_PEAK,
    _safe_read_sql,
)


@st.cache_data(ttl=60)
def load_demand_hourly(
    start_ts: str | None = None,
    end_ts: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    """
    Hourly demand rows from fact_hourly, collapsed to one row per
    (period_ts, ba_code) by summing generation across fuel types.
    Demand and forecast are BA-level figures so they repeat across fuel rows —
    we take MAX to deduplicate without losing the value.
    """
    query = f"""
    SELECT
        period_ts,
        ba_code,

        MAX(demand_gwh)   AS demand_gwh,
        MAX(forecast_gwh) AS forecast_gwh
    FROM {FACT_HOURLY}
    WHERE demand_gwh IS NOT NULL
    """
    params: list[Any] = []
    if start_ts:
        query += " AND period_ts >= ?"
        params.append(start_ts)
    if end_ts:
        query += " AND period_ts <= ?"
        params.append(end_ts)
    if ba_codes:
        placeholders = ", ".join("?" * len(ba_codes))
        query += f" AND ba_code IN ({placeholders})"
        params.extend(ba_codes)
    query += " GROUP BY period_ts, ba_code ORDER BY period_ts, ba_code"
    return _safe_read_sql(query, params or None)


@st.cache_data(ttl=60)
def load_daily_demand_peak(
    start_date: str | None = None,
    end_date: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    query = f"""
    SELECT report_date, ba_code, peak_gwh, partition_date
    FROM {AGG_DAILY_DEMAND_PEAK}
    WHERE 1=1
    """
    params: list[Any] = []
    if start_date:
        query += " AND report_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND report_date <= ?"
        params.append(end_date)
    if ba_codes:
        placeholders = ", ".join("?" * len(ba_codes))
        query += f" AND ba_code IN ({placeholders})"
        params.extend(ba_codes)
    query += " ORDER BY report_date, ba_code"
    return _safe_read_sql(query, params or None)


@st.cache_data(ttl=60)
def load_latest_demand_snapshot(
    start_ts: str | None = None,
    end_ts: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    """
    Latest hourly period demand vs forecast per BA, with forecast error columns.
    Collapses fuel rows with MAX since demand/forecast are BA-level values.
    """
    query = f"""
    WITH filtered AS (
        SELECT
            period_ts,
            ba_code,

            MAX(demand_gwh)   AS demand_gwh,
            MAX(forecast_gwh) AS forecast_gwh
        FROM {FACT_HOURLY}
        WHERE demand_gwh IS NOT NULL
    """
    params: list[Any] = []
    if start_ts:
        query += " AND period_ts >= ?"
        params.append(start_ts)
    if end_ts:
        query += " AND period_ts <= ?"
        params.append(end_ts)
    if ba_codes:
        placeholders = ", ".join("?" * len(ba_codes))
        query += f" AND ba_code IN ({placeholders})"
        params.extend(ba_codes)
    query += """
        GROUP BY period_ts, ba_code
    ),
    latest AS (SELECT MAX(period_ts) AS period_ts FROM filtered)
    SELECT
        f.period_ts,
        f.ba_code,

        f.demand_gwh,
        f.forecast_gwh,
        (f.demand_gwh - f.forecast_gwh)                              AS forecast_error_gwh,
        CASE
            WHEN f.forecast_gwh > 0
            THEN (f.demand_gwh - f.forecast_gwh) / f.forecast_gwh * 100
        END                                                          AS forecast_error_pct
    FROM filtered f
    JOIN latest ON f.period_ts = latest.period_ts
    ORDER BY f.ba_code
    """
    return _safe_read_sql(query, params or None)