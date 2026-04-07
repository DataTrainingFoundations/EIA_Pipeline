"""
data_access_sales.py
=================================
Gold electricity operational + sales helpers for the EIA Streamlit app.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from data_access_shared import (
    FACT_SALES_MONTHLY,
    _safe_read_sql,
    qualified_table,
)

FACT_TABLE = qualified_table("GOLD", FACT_SALES_MONTHLY)


@st.cache_data(ttl=3600)
def load_sales_gold() -> pd.DataFrame:
    df = _safe_read_sql(f"SELECT * FROM {FACT_TABLE}")

    df = df.rename(columns={
        "state_id":         "statedescription",
        "sector_abbr":      "sectorname",
        "retail_sales_mwh": "sales",
        "avg_price":        "price",
    })

    for c in ["sales", "revenue", "price", "customers"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["period"] = pd.to_datetime(df["period"])
    df.columns = [c.upper() for c in df.columns]
    return df.reset_index(drop=True)


@st.cache_data(ttl=3600)
def load_gold_coverage() -> dict:
    query = f"SELECT MIN(PERIOD) as min_period, MAX(PERIOD) as max_period FROM {FACT_TABLE}"
    row = _safe_read_sql(query).iloc[0]
    return {
    "min_period": pd.Timestamp(row["min_period"]),
    "max_period": pd.Timestamp(row["max_period"]),
    }


def gold_table_has_rows() -> bool:
    result = _safe_read_sql(f"SELECT COUNT(*) as cnt FROM {FACT_TABLE}")
    return result.iloc[0]["cnt"] > 0