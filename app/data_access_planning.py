"""Resource-planning query helpers for the business Streamlit app."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st
from data_access_shared import RESOURCE_PLANNING_TABLE, _safe_read_sql


@st.cache_data(ttl=60)
def load_resource_planning_daily(
    start_date: str | None = None,
    end_date: str | None = None,
    respondents: list[str] | None = None,
) -> pd.DataFrame:
    """Load planning rows for the selected date range and respondents."""

    query = f"select * from {RESOURCE_PLANNING_TABLE} where 1 = 1"
    params: list[Any] = []
    if start_date:
        query += " and date >= %s"
        params.append(start_date)
    if end_date:
        query += " and date <= %s"
        params.append(end_date)
    if respondents:
        query += " and respondent = any(%s)"
        params.append(respondents)
    query += " order by date, respondent"
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_latest_planning_snapshot(
    start_date: str | None = None,
    end_date: str | None = None,
    respondents: list[str] | None = None,
) -> pd.DataFrame:
    """Load the latest available planning snapshot for the selected filters."""

    query = f"""
    with filtered as (
        select *
        from {RESOURCE_PLANNING_TABLE}
        where 1 = 1
    """
    params: list[Any] = []
    if start_date:
        query += " and date >= %s"
        params.append(start_date)
    if end_date:
        query += " and date <= %s"
        params.append(end_date)
    if respondents:
        query += " and respondent = any(%s)"
        params.append(respondents)
    query += """
    ),
    latest_date as (
        select max(date) as date
        from filtered
    )
    select filtered.*
    from filtered
    join latest_date on filtered.date = latest_date.date
    order by filtered.respondent
    """
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_planning_watchlist(
    start_date: str | None = None,
    end_date: str | None = None,
    respondents: list[str] | None = None,
) -> pd.DataFrame:
    """Load the latest planning watchlist rows for the selected filters."""

    query = f"""
    with filtered as (
        select *
        from {RESOURCE_PLANNING_TABLE}
        where 1 = 1
    """
    params: list[Any] = []
    if start_date:
        query += " and date >= %s"
        params.append(start_date)
    if end_date:
        query += " and date <= %s"
        params.append(end_date)
    if respondents:
        query += " and respondent = any(%s)"
        params.append(respondents)
    query += """
    ),
    latest_date as (
        select max(date) as date
        from filtered
    )
    select filtered.*
    from filtered
    join latest_date on filtered.date = latest_date.date
    order by filtered.respondent
    """
    return _safe_read_sql(query, params)
