"""Gold monthly sales and operational mart helpers."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from data_access_shared import GOLD_ELECTRICITY_OPERATIONAL_SALES, _safe_read_sql, qualified_table, sql_in_list, sql_literal
from data_access_summary import table_has_rows

MONTHLY_GOLD_TABLE = qualified_table("GOLD", GOLD_ELECTRICITY_OPERATIONAL_SALES)


def _coerce_gold_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"])
    for column in ["customers", "price", "revenue", "sales", "generation", "fossil_pct", "renewable_pct", "nuclear_pct"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


@st.cache_data(ttl=300)
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
        query += f" and stateid in ({sql_in_list(states)})"
    query += " order by period, stateid, sector_name"
    return _coerce_gold_frame(_safe_read_sql(query))


@st.cache_data(ttl=300)
def gold_table_has_rows() -> bool:
    return table_has_rows(MONTHLY_GOLD_TABLE)
