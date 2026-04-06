"""Monthly operational fuel-mix helpers for the EIA Streamlit app."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from data_access_shared import SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA, _safe_read_sql, qualified_table, sql_in_list, sql_literal
from data_access_summary import table_has_rows
from data_access_sales import load_sales_data

OPERATIONAL_TABLE = qualified_table("SILVER", SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA)
FOSSIL_FUELS = {"NG", "COL"}
RENEWABLE_FUELS = {"SUN", "WND", "WAT"}
NUCLEAR_FUELS = {"NUC"}


def _coerce_operational_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"])
    if "generation" in df.columns:
        df["generation"] = pd.to_numeric(df["generation"], errors="coerce")
    return df


@st.cache_data(ttl=300)
def load_operational_data(
    start_date: str | None = None,
    end_date: str | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    query = f"""
    select
        period,
        state_id,
        state_description,
        sector_id,
        fuel_type_id,
        generation
    from {OPERATIONAL_TABLE}
    where generation is not null
    """
    if start_date:
        query += f" and period >= {sql_literal(start_date)}"
    if end_date:
        query += f" and period <= {sql_literal(end_date)}"
    if states:
        query += f" and state_id in ({sql_in_list(states)})"
    query += " order by period, state_id, fuel_type_id"
    return _coerce_operational_frame(_safe_read_sql(query))


@st.cache_data(ttl=300)
def load_fuel_mix_by_state(
    start_date: str | None = None,
    end_date: str | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    df = load_operational_data(start_date=start_date, end_date=end_date, states=states)
    if df.empty:
        return df
    grouped = (
        df.groupby(["period", "state_id", "state_description"], as_index=False)
        .agg(total_generation=("generation", "sum"))
    )
    fossil = (
        df[df["fuel_type_id"].isin(FOSSIL_FUELS)]
        .groupby(["period", "state_id"], as_index=False)
        .agg(fossil_generation=("generation", "sum"))
    )
    renewable = (
        df[df["fuel_type_id"].isin(RENEWABLE_FUELS)]
        .groupby(["period", "state_id"], as_index=False)
        .agg(renewable_generation=("generation", "sum"))
    )
    nuclear = (
        df[df["fuel_type_id"].isin(NUCLEAR_FUELS)]
        .groupby(["period", "state_id"], as_index=False)
        .agg(nuclear_generation=("generation", "sum"))
    )
    grouped = grouped.merge(fossil, on=["period", "state_id"], how="left")
    grouped = grouped.merge(renewable, on=["period", "state_id"], how="left")
    grouped = grouped.merge(nuclear, on=["period", "state_id"], how="left")
    grouped[["fossil_generation", "renewable_generation", "nuclear_generation"]] = grouped[
        ["fossil_generation", "renewable_generation", "nuclear_generation"]
    ].fillna(0.0)
    grouped["fossil_pct"] = grouped["fossil_generation"] / grouped["total_generation"].replace(0, pd.NA) * 100
    grouped["renewable_pct"] = grouped["renewable_generation"] / grouped["total_generation"].replace(0, pd.NA) * 100
    grouped["nuclear_pct"] = grouped["nuclear_generation"] / grouped["total_generation"].replace(0, pd.NA) * 100
    return grouped


@st.cache_data(ttl=300)
def load_fuel_mix_price_joined(
    start_date: str | None = None,
    end_date: str | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    mix_df = load_fuel_mix_by_state(start_date=start_date, end_date=end_date, states=states)
    sales_df = load_sales_data(start_date=start_date, end_date=end_date, states=states)
    if mix_df.empty or sales_df.empty:
        return pd.DataFrame()

    sales_prices = (
        sales_df.groupby(["period", "state_id", "state_description"], as_index=False)
        .agg(avg_price=("price", "mean"))
    )
    return mix_df.merge(
        sales_prices,
        on=["period", "state_id", "state_description"],
        how="inner",
    )


@st.cache_data(ttl=300)
def operational_table_has_rows() -> bool:
    return table_has_rows(OPERATIONAL_TABLE)
