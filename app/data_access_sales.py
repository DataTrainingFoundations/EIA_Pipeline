"""Monthly sales query helpers for the EIA Streamlit app."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_access_shared import (
    FACT_SALES_MONTHLY,
    SLOW_CACHE_TTL,
    SILVER_ELECTRICITY_RETAIL_SALES,
    _safe_read_sql,
    qualified_table,
    sql_in_list,
    sql_literal,
)
from data_access_summary import table_has_rows

SALES_TABLE = qualified_table("SILVER", SILVER_ELECTRICITY_RETAIL_SALES)
MONTHLY_GOLD_TABLE = qualified_table("GOLD", FACT_SALES_MONTHLY)


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


def _coerce_gold_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    numeric_cols = ["customers", "price", "revenue", "sales", "generation", "fossil_pct", "renewable_pct", "nuclear_pct"]
    for column in numeric_cols:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"])
    return df


def load_sales_data(
    start_date: str | None = None,
    end_date: str | None = None,
    sectors: list[str] | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    query = f"""
    select
        period,
        state_id,
        state_description,
        sector_abbr,
        sector_name,
        customers,
        price,
        revenue,
        sales
    from {SALES_TABLE}
    where sector_abbr != 'ALL'
    """
    if start_date:
        query += f" and period >= {sql_literal(start_date)}"
    if end_date:
        query += f" and period <= {sql_literal(end_date)}"
    if sectors:
        query += f" and sector_name in ({sql_in_list(sectors)})"
    if states:
        query += f" and state_id in ({sql_in_list(states)})"
    query += " order by period, state_id, sector_name"
    return _coerce_sales_frame(_safe_read_sql(query, ttl=SLOW_CACHE_TTL))


def load_sales_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(period) as min_period,
        max(period) as max_period,
        count(*) as row_count,
        count(distinct state_id) as state_count,
        count(distinct sector_abbr) as sector_count
    from {SALES_TABLE}
    where sector_abbr != 'ALL'
    """
    row = _safe_read_sql(query, ttl=SLOW_CACHE_TTL).iloc[0].to_dict()
    if row.get("min_period") is not None:
        row["min_period"] = pd.to_datetime(row["min_period"])
    if row.get("max_period") is not None:
        row["max_period"] = pd.to_datetime(row["max_period"])
    return row


def load_sales_gold(
    start_date: str | None = None,
    end_date: str | None = None,
    sectors: list[str] | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    query = f"select * from {MONTHLY_GOLD_TABLE} where 1=1"
    if start_date:
        query += f" and period >= {sql_literal(start_date)}"
    if end_date:
        query += f" and period <= {sql_literal(end_date)}"
    if sectors:
        query += f" and sector_name in ({sql_in_list(sectors)})"
    if states:
        query += f" and state_id in ({sql_in_list(states)})"
    query += " order by period, state_id, sector_name"
    return _coerce_gold_frame(_safe_read_sql(query, ttl=SLOW_CACHE_TTL))


def list_sales_states() -> list[str]:
    query = f"select distinct state_id from {SALES_TABLE} where sector_abbr != 'ALL' order by state_id"
    df = _safe_read_sql(query, ttl=SLOW_CACHE_TTL)
    return df["state_id"].dropna().tolist()


def list_sales_sectors() -> list[str]:
    query = f"select distinct sector_name from {SALES_TABLE} where sector_abbr != 'ALL' order by sector_name"
    df = _safe_read_sql(query, ttl=SLOW_CACHE_TTL)
    return df["sector_name"].dropna().tolist()


def sales_table_has_rows() -> bool:
    return table_has_rows(SALES_TABLE)


def gold_table_has_rows() -> bool:
    return table_has_rows(MONTHLY_GOLD_TABLE)
