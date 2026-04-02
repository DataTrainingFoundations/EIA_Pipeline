"""Power operations query helpers for the EIA Streamlit app.

Connects to Snowflake via Snowpark, mirroring the pattern in data_access_sales.py.
All public functions are re-exported through data_access.py via `from data_access_operational import *`.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

load_dotenv()

# ── Table name constant ────────────────────────────────────────────────────────
ELECTRICITY_POWER_OPERATIONAL_RAW = "ELECTRICITY_POWER_OPERATIONAL_RAW"

# State-level IDs only — excludes census regions (ENC, WSC, etc.) and US total
VALID_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO",
    "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

# Fuel type groupings
FOSSIL_FUELS = {"NG", "COL", "COW", "DFO", "RFO", "PC", "OOG", "OIL"}
RENEWABLE_FUELS = {"SUN", "WND", "WAT", "GEO", "AOR"}
NUCLEAR_FUELS = {"NUC"}

NUMERIC_COLS_OPS = ["GENERATION", "CONSUMPTION_FOR_EG", "HEAT_CONTENT", "ASH_CONTENT"]


# ── Snowpark session factory ───────────────────────────────────────────────────
def _get_session() -> Session:
    return Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "database":  os.getenv("SNOWFLAKE_DATABASE"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
    }).create()


# ── Shared post-processing ─────────────────────────────────────────────────────
def _clean_ops_df(df: pd.DataFrame) -> pd.DataFrame:
    df[NUMERIC_COLS_OPS] = df[NUMERIC_COLS_OPS].apply(pd.to_numeric, errors="coerce")
    df["PERIOD"] = pd.to_datetime(df["PERIOD"])
    df["FUELTYPEID"] = df["FUELTYPEID"].str.upper().str.strip()
    df["SECTORID"] = df["SECTORID"].astype(str).str.strip()
    return df


# ── Public query functions ─────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_operational_data(
    start_date: str | None = None,
    end_date: str | None = None,
    states: list[str] | None = None,
    fuel_types: list[str] | None = None,
    sector_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Full power operations dataset with optional filters.

    Parameters
    ----------
    start_date : ISO date string ``"YYYY-MM"``. Inclusive lower bound on PERIOD.
    end_date   : ISO date string ``"YYYY-MM"``. Inclusive upper bound on PERIOD.
    states     : List of STATEID values. ``None`` = all valid states.
    fuel_types : List of FUELTYPEID values (e.g. ``["NG", "SUN"]``). ``None`` = all.
    sector_ids : List of SECTORID values (e.g. ``["1", "2"]``). ``None`` = all.
                 Sector 1 = Electric Utility, 2 = IPP Non-CHP, 3 = IPP CHP,
                 4 = Commercial CHP, 5 = Industrial CHP, 6 = All sectors.
    """
    session = _get_session()
    try:
        state_filter = list(states) if states else list(VALID_STATES)

        query = (
            session.table(ELECTRICITY_POWER_OPERATIONAL_RAW)
            .filter(col("LOCATION").isin(state_filter))
            # Exclude aggregate fuel rows — use granular fuel types only
            .filter(~col("FUELTYPEID").isin(["ALL", "AOR", "COW", "OOG"]))
            .select(
                "PERIOD", "LOCATION", "STATEDESCRIPTION",
                "SECTORID", "SECTORDESCRIPTION",
                "FUELTYPEID", "FUELTYPEDESCRIPTION",
                "GENERATION", "GENERATION_UNITS",
                "CONSUMPTION_FOR_EG", "CONSUMPTION_FOR_EG_UNITS",
                "HEAT_CONTENT", "HEAT_CONTENT_UNITS", "ASH_CONTENT"
            )
        )

        if sector_ids:
            query = query.filter(col("SECTORID").isin([str(s) for s in sector_ids]))
        if fuel_types:
            query = query.filter(col("FUELTYPEID").isin([f.upper() for f in fuel_types]))

        df = query.to_pandas()
    finally:
        session.close()

    df = _clean_ops_df(df)

    if start_date:
        df = df[df["PERIOD"] >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df["PERIOD"] <= pd.Timestamp(end_date)]

    return df.reset_index(drop=True)


@st.cache_data(ttl=3600)
def load_fuel_mix_by_state(
    start_date: str | None = None,
    end_date: str | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """Monthly fuel mix per state — generation GWh by fuel group (fossil/renewable/nuclear/other).

    Returns columns: PERIOD, LOCATION, STATEDESCRIPTION,
                     TOTAL_GWH, FOSSIL_GWH, RENEWABLE_GWH, NUCLEAR_GWH,
                     FOSSIL_PCT, RENEWABLE_PCT, NUCLEAR_PCT
    """
    df = load_operational_data(start_date=start_date, end_date=end_date, states=states)
    if df.empty:
        return df

    df["FUEL_GROUP"] = "other"
    df.loc[df["FUELTYPEID"].isin(FOSSIL_FUELS), "FUEL_GROUP"] = "fossil"
    df.loc[df["FUELTYPEID"].isin(RENEWABLE_FUELS), "FUEL_GROUP"] = "renewable"
    df.loc[df["FUELTYPEID"].isin(NUCLEAR_FUELS), "FUEL_GROUP"] = "nuclear"

    # Aggregate all sectors together for state-level view
    agg = (
        df.groupby(["PERIOD", "LOCATION", "STATEDESCRIPTION", "FUEL_GROUP"])["GENERATION"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
    )

    # Ensure all columns exist even if a fuel group is absent in data
    for col_name in ["fossil", "renewable", "nuclear", "other"]:
        if col_name not in agg.columns:
            agg[col_name] = 0.0

    agg.rename(columns={
        "fossil":    "FOSSIL_GWH",
        "renewable": "RENEWABLE_GWH",
        "nuclear":   "NUCLEAR_GWH",
        "other":     "OTHER_GWH",
    }, inplace=True)

    agg["TOTAL_GWH"] = agg[["FOSSIL_GWH", "RENEWABLE_GWH", "NUCLEAR_GWH", "OTHER_GWH"]].sum(axis=1)
    agg["FOSSIL_PCT"]    = agg["FOSSIL_GWH"]    / agg["TOTAL_GWH"].replace(0, float("nan")) * 100
    agg["RENEWABLE_PCT"] = agg["RENEWABLE_GWH"] / agg["TOTAL_GWH"].replace(0, float("nan")) * 100
    agg["NUCLEAR_PCT"]   = agg["NUCLEAR_GWH"]   / agg["TOTAL_GWH"].replace(0, float("nan")) * 100

    return agg.sort_values(["PERIOD", "LOCATION"]).reset_index(drop=True)


@st.cache_data(ttl=3600)
def load_fuel_mix_price_joined(
    start_date: str | None = None,
    end_date: str | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """Join fuel mix (operational) with avg retail price (sales) on PERIOD + state.

    Returns one row per state per month with fuel mix percentages and avg retail price.
    Useful for scatter/correlation charts.

    Returns columns: PERIOD, LOCATION, STATEDESCRIPTION,
                     FOSSIL_PCT, RENEWABLE_PCT, NUCLEAR_PCT,
                     TOTAL_GWH, AVG_PRICE
    """
    from data_access_sales import load_sales_data  # avoid circular import at module level

    fuel_mix = load_fuel_mix_by_state(start_date=start_date, end_date=end_date, states=states)
    sales = load_sales_data(start_date=start_date, end_date=end_date, states=states)

    if fuel_mix.empty or sales.empty:
        return pd.DataFrame()

    # Aggregate sales to state + period level (all sectors avg price)
    sales_agg = (
        sales[sales["SECTORID"] == "ALL"]
        .groupby(["PERIOD", "STATEID"])
        .agg(AVG_PRICE=("PRICE", "mean"))
        .reset_index()
        .rename(columns={"STATEID": "LOCATION"})
    )

    joined = fuel_mix.merge(sales_agg, on=["PERIOD", "LOCATION"], how="inner")
    return joined.sort_values(["PERIOD", "LOCATION"]).reset_index(drop=True)


@st.cache_data(ttl=3600)
def operational_table_has_rows() -> bool:
    """Quick existence check for the operational table."""
    session = _get_session()
    try:
        count = (
            session.table(ELECTRICITY_POWER_OPERATIONAL_RAW)
            .filter(col("LOCATION").isin(list(VALID_STATES)))
            .count()
        )
    finally:
        session.close()
    return count > 0