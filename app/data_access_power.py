"""Power-operations query helpers for the business Streamlit app."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st
from data_access_shared import POWER_OPERATIONS_MONTHLY_TABLE, _safe_read_sql


@st.cache_data(ttl=60)
def load_power_operations_monthly(
    start_period: str | None = None,
    end_period: str | None = None,
    locations: list[str] | None = None,
    sector_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Load monthly power-operations rows for the selected filters."""

    query = f"select * from {POWER_OPERATIONS_MONTHLY_TABLE} where 1 = 1"
    params: list[Any] = []
    if start_period:
        query += " and period >= %s"
        params.append(start_period)
    if end_period:
        query += " and period <= %s"
        params.append(end_period)
    if locations:
        query += " and location = any(%s)"
        params.append(locations)
    if sector_ids:
        query += " and sector_id = any(%s)"
        params.append(sector_ids)
    query += " order by period, location, sector_id, fueltype_id"
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def load_latest_power_operations_snapshot(
    start_period: str | None = None,
    end_period: str | None = None,
    locations: list[str] | None = None,
    sector_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Load the latest available power-operations snapshot for the selected filters."""

    query = f"""
    with filtered as (
        select *
        from {POWER_OPERATIONS_MONTHLY_TABLE}
        where 1 = 1
    """
    params: list[Any] = []
    if start_period:
        query += " and period >= %s"
        params.append(start_period)
    if end_period:
        query += " and period <= %s"
        params.append(end_period)
    if locations:
        query += " and location = any(%s)"
        params.append(locations)
    if sector_ids:
        query += " and sector_id = any(%s)"
        params.append(sector_ids)
    query += """
    ),
    latest_period as (
        select max(period) as period
        from filtered
    )
    select filtered.*
    from filtered
    join latest_period on filtered.period = latest_period.period
    order by filtered.location, filtered.sector_id, filtered.fueltype_id
    """
    return _safe_read_sql(query, params)


@st.cache_data(ttl=60)
def get_power_operations_coverage() -> dict[str, Any]:
    """Return the period range and row counts for the power-operations mart."""

    query = f"""
    select
        min(period) as min_period,
        max(period) as max_period,
        count(*) as row_count,
        count(distinct location) as location_count,
        count(distinct sector_id) as sector_count
    from {POWER_OPERATIONS_MONTHLY_TABLE}
    """
    return _safe_read_sql(query).iloc[0].to_dict()


@st.cache_data(ttl=60)
def list_power_locations() -> list[str]:
    """Return the sorted set of power locations available in the mart."""

    query = f"""
    select distinct location
    from {POWER_OPERATIONS_MONTHLY_TABLE}
    where location is not null
    order by location
    """
    df = _safe_read_sql(query)
    return df["location"].dropna().tolist()


@st.cache_data(ttl=60)
def list_power_sectors() -> pd.DataFrame:
    """Return the distinct sector ids and names available in the power mart."""

    query = f"""
    select distinct sector_id, sector_name
    from {POWER_OPERATIONS_MONTHLY_TABLE}
    where sector_id is not null
      and sector_name is not null
    order by sector_name, sector_id
    """
    df = _safe_read_sql(query)
    if not df.empty:
        df["sector_id"] = df["sector_id"].astype(str)
        df["sector_name"] = df["sector_name"].astype(str)
    return df
