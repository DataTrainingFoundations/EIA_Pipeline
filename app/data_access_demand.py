"""Demand query helpers for the EIA Streamlit app."""

from __future__ import annotations

import pandas as pd

from data_access_shared import (
    FAST_CACHE_TTL,
    AGG_DAILY_DEMAND_PEAK,
    FACT_DEMAND_HOURLY,
    _safe_read_sql,
    sql_in_list,
    sql_literal,
)


def load_demand_hourly(
    start_ts: str | None = None,
    end_ts: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    query = f"select * from {FACT_DEMAND_HOURLY} where 1=1"
    if start_ts:
        query += f" and period_ts >= {sql_literal(start_ts)}"
    if end_ts:
        query += f" and period_ts <= {sql_literal(end_ts)}"
    if ba_codes:
        query += f" and ba_code in ({sql_in_list(ba_codes)})"
    query += " order by period_ts, ba_code"
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL)


def load_daily_demand_peak(
    start_date: str | None = None,
    end_date: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    query = f"select * from {AGG_DAILY_DEMAND_PEAK} where 1=1"
    if start_date:
        query += f" and report_date >= {sql_literal(start_date)}"
    if end_date:
        query += f" and report_date <= {sql_literal(end_date)}"
    if ba_codes:
        query += f" and ba_code in ({sql_in_list(ba_codes)})"
    query += " order by report_date, ba_code"
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL)


def load_latest_demand_snapshot(
    start_ts: str | None = None,
    end_ts: str | None = None,
    ba_codes: list[str] | None = None,
) -> pd.DataFrame:
    query = f"""
    with filtered as (
        select * from {FACT_DEMAND_HOURLY} where 1=1
    """
    if start_ts:
        query += f" and period_ts >= {sql_literal(start_ts)}"
    if end_ts:
        query += f" and period_ts <= {sql_literal(end_ts)}"
    if ba_codes:
        query += f" and ba_code in ({sql_in_list(ba_codes)})"
    query += """
    ),
    latest as (select max(period_ts) as period_ts from filtered)
    select
        f.*,
        (f.demand_gwh - f.forecast_gwh) as forecast_error_gwh,
        case
            when f.forecast_gwh > 0
            then (f.demand_gwh - f.forecast_gwh) / f.forecast_gwh * 100
        end as forecast_error_pct
    from filtered f
    join latest on f.period_ts = latest.period_ts
    order by f.ba_code
    """
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL)
