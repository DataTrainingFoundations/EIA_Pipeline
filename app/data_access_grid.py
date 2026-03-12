"""Grid-operations query helpers for the business Streamlit app."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from data_access_shared import GRID_OPERATIONS_ALERT_TABLE, GRID_OPERATIONS_TABLE, _safe_read_sql


@st.cache_data(ttl=60)
def load_grid_operations_hourly(start_ts: str | None = None, end_ts: str | None = None, respondents: list[str] | None = None) -> pd.DataFrame:
    """Load hourly grid-operations rows for the selected time range and respondents."""

    query = f"select * from {GRID_OPERATIONS_TABLE} where 1 = 1"
    params: list[Any] = []
    if start_ts:
        query += " and period >= %s"
        params.append(start_ts)
    if end_ts:
        query += " and period <= %s"
        params.append(end_ts)
    if respondents:
        query += " and respondent = any(%s)"
        params.append(respondents)
    query += " order by period, respondent"
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_grid_operations_alerts(start_ts: str | None = None, end_ts: str | None = None, respondents: list[str] | None = None) -> pd.DataFrame:
    """Load persisted grid-operation alerts for the selected filters."""

    query = f"select * from {GRID_OPERATIONS_ALERT_TABLE} where 1 = 1"
    params: list[Any] = []
    if start_ts:
        query += " and period >= %s"
        params.append(start_ts)
    if end_ts:
        query += " and period <= %s"
        params.append(end_ts)
    if respondents:
        query += " and respondent = any(%s)"
        params.append(respondents)
    query += " order by period desc, respondent, alert_type"
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_latest_grid_operations_snapshot(
    start_ts: str | None = None,
    end_ts: str | None = None,
    respondents: list[str] | None = None,
) -> pd.DataFrame:
    """Load the latest available hourly snapshot for grid operations."""

    query = f"""
    with filtered as (
        select *
        from {GRID_OPERATIONS_TABLE}
        where 1 = 1
    """
    params: list[Any] = []
    if start_ts:
        query += " and period >= %s"
        params.append(start_ts)
    if end_ts:
        query += " and period <= %s"
        params.append(end_ts)
    if respondents:
        query += " and respondent = any(%s)"
        params.append(respondents)
    query += """
    ),
    latest_period as (
        select max(period) as period
        from filtered
    )
    select filtered.*
    from filtered
    join latest_period on filtered.period = latest_period.period
    order by filtered.respondent
    """
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_latest_grid_alerts(
    start_ts: str | None = None,
    end_ts: str | None = None,
    respondents: list[str] | None = None,
) -> pd.DataFrame:
    """Load the latest available alert snapshot for grid operations."""

    query = f"""
    with filtered as (
        select *
        from {GRID_OPERATIONS_ALERT_TABLE}
        where 1 = 1
    """
    params: list[Any] = []
    if start_ts:
        query += " and period >= %s"
        params.append(start_ts)
    if end_ts:
        query += " and period <= %s"
        params.append(end_ts)
    if respondents:
        query += " and respondent = any(%s)"
        params.append(respondents)
    query += """
    ),
    latest_period as (
        select max(period) as period
        from filtered
    )
    select filtered.*
    from filtered
    join latest_period on filtered.period = latest_period.period
    order by filtered.respondent, filtered.alert_type
    """
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_grid_watchlist(
    start_ts: str | None = None,
    end_ts: str | None = None,
    respondents: list[str] | None = None,
) -> pd.DataFrame:
    """Load the latest watchlist table that combines ops rows and alert rollups."""

    query = f"""
    with filtered_ops as (
        select *
        from {GRID_OPERATIONS_TABLE}
        where 1 = 1
    """
    params: list[Any] = []
    if start_ts:
        query += " and period >= %s"
        params.append(start_ts)
    if end_ts:
        query += " and period <= %s"
        params.append(end_ts)
    if respondents:
        query += " and respondent = any(%s)"
        params.append(respondents)
    query += f"""
    ),
    filtered_alerts as (
        select *
        from {GRID_OPERATIONS_ALERT_TABLE}
        where 1 = 1
    """
    if start_ts:
        query += " and period >= %s"
        params.append(start_ts)
    if end_ts:
        query += " and period <= %s"
        params.append(end_ts)
    if respondents:
        query += " and respondent = any(%s)"
        params.append(respondents)
    query += """
    ),
    latest_period as (
        select max(period) as period
        from filtered_ops
    ),
    latest_ops as (
        select filtered_ops.*
        from filtered_ops
        join latest_period on filtered_ops.period = latest_period.period
    ),
    latest_alerts as (
        select filtered_alerts.*
        from filtered_alerts
        join latest_period on filtered_alerts.period = latest_period.period
    ),
    alert_rollup as (
        select
            respondent,
            count(*) as active_alert_count,
            bool_or(lower(severity) = 'high') as has_high_alert,
            bool_or(lower(severity) = 'medium') as has_medium_alert
        from latest_alerts
        group by respondent
    )
    select
        latest_ops.period,
        latest_ops.respondent,
        latest_ops.respondent_name,
        latest_ops.actual_demand_mwh,
        latest_ops.forecast_error_mwh,
        latest_ops.forecast_error_zscore,
        latest_ops.demand_ramp_zscore,
        latest_ops.coverage_ratio,
        coalesce(alert_rollup.active_alert_count, 0) as active_alert_count,
        coalesce(alert_rollup.has_high_alert, false) as has_high_alert,
        coalesce(alert_rollup.has_medium_alert, false) as has_medium_alert
    from latest_ops
    left join alert_rollup on latest_ops.respondent = alert_rollup.respondent
    order by latest_ops.respondent
    """
    return _safe_read_sql(query, params)
