"""Coverage and shared summary queries for the business Streamlit app."""

from __future__ import annotations

from contextlib import closing
from typing import Any

import pandas as pd
import psycopg2
import streamlit as st
from data_access_shared import (
    GRID_OPERATIONS_TABLE,
    PLATINUM_TABLE,
    RESOURCE_PLANNING_TABLE,
    _safe_read_sql,
    get_connection,
)


@st.cache_data(ttl=60)
def table_has_rows(table_name: str = PLATINUM_TABLE) -> bool:
    """Return whether a serving table exists and contains at least one row."""

    query = f"select exists (select 1 from {table_name} limit 1)"
    try:
        with closing(get_connection()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(query)
                return bool(cur.fetchone()[0])
    except psycopg2.Error:
        return False


@st.cache_data(ttl=60)
def get_summary_coverage() -> dict[str, Any]:
    """Return the date range and row counts for the core daily mart."""

    query = f"""
    select
        min(date) as min_date,
        max(date) as max_date,
        count(*) as row_count,
        count(distinct respondent) as respondent_count
    from {PLATINUM_TABLE}
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def get_grid_operations_coverage() -> dict[str, Any]:
    """Return the period range and row counts for the grid operations mart."""

    query = f"""
    select
        min(period) as min_period,
        max(period) as max_period,
        count(*) as row_count,
        count(distinct respondent) as respondent_count
    from {GRID_OPERATIONS_TABLE}
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def get_planning_coverage() -> dict[str, Any]:
    """Return the date range and row counts for the resource planning mart."""

    query = f"""
    select
        min(date) as min_date,
        max(date) as max_date,
        count(*) as row_count,
        count(distinct respondent) as respondent_count
    from {RESOURCE_PLANNING_TABLE}
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def list_respondents(table_name: str = PLATINUM_TABLE) -> list[str]:
    """Return the sorted set of respondents available in one serving table."""

    query = f"select distinct respondent from {table_name} order by respondent"
    df = _safe_read_sql(query)
    return df["respondent"].dropna().tolist()


@st.cache_data(ttl=60)
def get_backfill_status() -> pd.DataFrame:
    """Return aggregated backfill job status for the home page."""

    query = """
    select
        dataset_id,
        status,
        job_count,
        min_chunk_start_utc,
        max_chunk_end_utc,
        last_updated_at
    from ops.backfill_job_summary
    order by dataset_id, status
    """
    try:
        return _safe_read_sql(query)
    except psycopg2.Error:
        return pd.DataFrame(
            columns=[
                "dataset_id",
                "status",
                "job_count",
                "min_chunk_start_utc",
                "max_chunk_end_utc",
                "last_updated_at",
            ]
        )
