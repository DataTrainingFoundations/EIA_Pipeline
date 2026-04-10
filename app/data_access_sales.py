"""
Gold electricity operational + sales helpers for the EIA Streamlit app.
"""

from __future__ import annotations
from typing import Any

import pandas as pd
import streamlit as st

from data_access_shared import (
    FACT_SALES_MONTHLY,
    SLOW_CACHE_TTL,
    _safe_read_sql,
    qualified_table,
    sql_literal,
)

# Gold fact table reference
FACT_TABLE = qualified_table("GOLD", FACT_SALES_MONTHLY)


def _coerce_sales_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert key numeric columns to float and parse period as datetime.
    Maintains robustness from main branch.
    """
    if df.empty:
        return df
    numeric_cols = ["customers", "price", "revenue", "sales"]
    for col_name in numeric_cols:
        if col_name in df.columns:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"])
    return df


@st.cache_data(ttl=SLOW_CACHE_TTL)
def load_sales_gold(
    start_date: str | None = None,
    end_date: str | None = None,
    sectors: list[str] | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """
    Load gold sales data with optional filtering.
    Combines feature branch simplicity with main branch caching and coercion.
    """
    query = f"SELECT * FROM {FACT_TABLE} WHERE 1=1"

    if start_date:
        query += f" AND period >= {sql_literal(start_date)}"
    if end_date:
        query += f" AND period <= {sql_literal(end_date)}"
    if sectors:
        query += f" AND sector_name IN ({', '.join(map(sql_literal, sectors))})"
    if states:
        query += f" AND state_id IN ({', '.join(map(sql_literal, states))})"

    df = _safe_read_sql(query, ttl=SLOW_CACHE_TTL)
    df = _coerce_sales_frame(df)

    # Standardize column names like feature branch
    df = df.rename(columns={
        "state_id": "statedescription",
        "sector_abbr": "sectorname",
        "retail_sales_mwh": "sales",
        "avg_price": "price",
    })
    df.columns = [c.upper() for c in df.columns]
    return df.reset_index(drop=True)


@st.cache_data(ttl=SLOW_CACHE_TTL)
def load_gold_coverage() -> dict[str, Any]:
    """
    Returns min/max period and row count for gold sales table.
    Combines caching from feature branch with main's robustness.
    """
    query = f"""
        SELECT 
            MIN(period) AS min_period, 
            MAX(period) AS max_period, 
            COUNT(*) AS row_count 
        FROM {FACT_TABLE}
    """
    row = _safe_read_sql(query, ttl=SLOW_CACHE_TTL).iloc[0].to_dict()
    if row.get("min_period") is not None:
        row["min_period"] = pd.to_datetime(row["min_period"])
    if row.get("max_period") is not None:
        row["max_period"] = pd.to_datetime(row["max_period"])
    return row


def gold_table_has_rows() -> bool:
    """
    Efficient row check for gold table.
    """
    query = f"SELECT COUNT(*) AS cnt FROM {FACT_TABLE}"
    result = _safe_read_sql(query, ttl=SLOW_CACHE_TTL)
    return result.iloc[0]["cnt"] > 0