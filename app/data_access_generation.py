"""Generation query helpers for the EIA Streamlit app."""

from __future__ import annotations
from typing import Any

import pandas as pd
import streamlit as st

from data_access_shared import (
    FACT_HOURLY,
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
    """
    Hourly generation rows from fact_hourly.
    Selects only the generation-relevant columns so the result shape matches
    what the generation dashboard expects.
    """
    query = f"""
    SELECT

        period_ts,
        ba_code,

        fuel_code,
        fuel_name,
        generation_gwh,
        partition_date
    FROM {FACT_HOURLY}
    WHERE generation_gwh IS NOT NULL
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
    if fuel_codes:
        placeholders = ", ".join("?" * len(fuel_codes))
        query += f" AND fuel_code IN ({placeholders})"
        params.extend(fuel_codes)
    query += " ORDER BY period_ts, ba_code, fuel_code"
    return _safe_read_sql(query, params or None)


@st.cache_data(ttl=60)
def load_daily_generation(
    start_date: str | None = None,
    end_date: str | None = None,
    fuel_codes: list[str] | None = None,
) -> pd.DataFrame:
    query = f"""
    SELECT report_date, fuel_code, fuel_name, total_gwh, partition_date
    FROM {AGG_DAILY_GENERATION}
    WHERE 1=1
    """
    params: list[Any] = []
    if start_date:
        query += " AND report_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND report_date <= ?"
        params.append(end_date)
    if fuel_codes:
        placeholders = ", ".join("?" * len(fuel_codes))
        query += f" AND fuel_code IN ({placeholders})"
        params.extend(fuel_codes)
    query += " ORDER BY report_date, fuel_code"
    return _safe_read_sql(query, params or None)


@st.cache_data(ttl=60)
def load_latest_generation_snapshot(
    start_ts: str | None = None,
    end_ts: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    """
    Latest hourly period's generation totalled by BA — renewable/fossil shares.
    Reads from fact_hourly, computing share percentages in SQL.
    """
    query = f"""
    WITH filtered AS (
        SELECT *
        FROM {FACT_HOURLY}
        WHERE generation_gwh IS NOT NULL
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
    ),
    latest AS (SELECT MAX(period_ts) AS period_ts FROM filtered)
    SELECT
        f.ba_code,

        f.period_ts,
        SUM(f.generation_gwh)                                                AS total_gwh,
        SUM(CASE WHEN f.fuel_code IN ('WND','SUN','WAT') THEN f.generation_gwh ELSE 0 END)
            / NULLIF(SUM(f.generation_gwh), 0) * 100                         AS renewable_pct,
        SUM(CASE WHEN f.fuel_code IN ('NG','COL') THEN f.generation_gwh ELSE 0 END)
            / NULLIF(SUM(f.generation_gwh), 0) * 100                         AS fossil_pct,
        SUM(CASE WHEN f.fuel_code = 'NG' THEN f.generation_gwh ELSE 0 END)
            / NULLIF(SUM(f.generation_gwh), 0) * 100                         AS gas_pct
    FROM filtered f
    JOIN latest ON f.period_ts = latest.period_ts
    GROUP BY f.ba_code, f.period_ts
    ORDER BY total_gwh DESC
    """
    return _safe_read_sql(query, params or None)