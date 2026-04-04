"""Monthly sales query helpers for the EIA Streamlit app."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from data_access_shared import SILVER_ELECTRICITY_RETAIL_SALES, _safe_read_sql, qualified_table, sql_in_list, sql_literal
from data_access_summary import table_has_rows

SALES_TABLE = qualified_table("SILVER", SILVER_ELECTRICITY_RETAIL_SALES)


def _coerce_sales_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    numeric_cols = ["customers", "price", "revenue", "sales"]
    for column in numeric_cols:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"])
    return df


@st.cache_data(ttl=300)
def load_sales_data(
    start_date: str | None = None,
    end_date: str | None = None,
    sectors: list[str] | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    query = f"""
    select
        period,
        stateid,
        state_description,
        sectorid,
        sector_name,
        customers,
        price,
        revenue,
        sales
    from {SALES_TABLE}
    where sectorid != 'ALL'
    """
    if start_date:
        query += f" and period >= {sql_literal(start_date)}"
    if end_date:
        query += f" and period <= {sql_literal(end_date)}"
    if sectors:
        query += f" and sector_name in ({sql_in_list(sectors)})"
    if states:
        query += f" and stateid in ({sql_in_list(states)})"
    query += " order by period, stateid, sector_name"
    return _coerce_sales_frame(_safe_read_sql(query))


@st.cache_data(ttl=300)
def load_sales_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(period) as min_period,
        max(period) as max_period,
        count(*) as row_count,
        count(distinct stateid) as state_count,
        count(distinct sectorid) as sector_count
    from {SALES_TABLE}
    where sectorid != 'ALL'
    """
    row = _safe_read_sql(query).iloc[0].to_dict()
    if row.get("min_period") is not None:
        row["min_period"] = pd.to_datetime(row["min_period"])
    if row.get("max_period") is not None:
        row["max_period"] = pd.to_datetime(row["max_period"])
    return row


@st.cache_data(ttl=300)
def list_sales_states() -> list[str]:
    query = f"select distinct stateid from {SALES_TABLE} where sectorid != 'ALL' order by stateid"
    df = _safe_read_sql(query)
    return df["stateid"].dropna().tolist()


@st.cache_data(ttl=300)
def list_sales_sectors() -> list[str]:
    query = f"select distinct sector_name from {SALES_TABLE} where sectorid != 'ALL' order by sector_name"
    df = _safe_read_sql(query)
    return df["sector_name"].dropna().tolist()


@st.cache_data(ttl=300)
def sales_table_has_rows() -> bool:
    return table_has_rows(SALES_TABLE)
