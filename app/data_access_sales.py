"""Sales query helpers for the EIA Streamlit app.

Connects to Snowflake via Snowpark (mirrors the pattern in monthly_sales.py).
All public functions are re-exported through data_access.py via `from data_access_sales import *`.
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

# ── Table name constants ───────────────────────────────────────────────────────
ELECTRICITY_RETAIL_SALES_RAW = "ELECTRICITY_RETAIL_SALES_RAW"

VALID_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO",
    "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

NUMERIC_COLS_SALES = ["CUSTOMERS", "PRICE", "REVENUE", "SALES"]


# ── Snowpark session factory ───────────────────────────────────────────────────
def _get_session() -> Session:
    """Create and return a new Snowpark session from environment variables."""
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
def _clean_sales_df(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce types and normalise columns — mirrors monthly_sales.py logic."""
    df[NUMERIC_COLS_SALES] = df[NUMERIC_COLS_SALES].apply(pd.to_numeric, errors="coerce")
    df["SECTORNAME"] = df["SECTORNAME"].str.lower()
    df["PERIOD"] = pd.to_datetime(df["PERIOD"])
    return df


# ── Public query functions ─────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_sales_data(
    start_date: str | None = None,
    end_date: str | None = None,
    sectors: list[str] | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """Full retail sales dataset with optional filters.

    Parameters
    ----------
    start_date:
        ISO date string (``"YYYY-MM-DD"``). Inclusive lower bound on PERIOD.
    end_date:
        ISO date string (``"YYYY-MM-DD"``). Inclusive upper bound on PERIOD.
    sectors:
        List of SECTORNAME values to include (case-insensitive). ``None`` = all.
    states:
        List of STATEID values to include (e.g. ``["TX", "CA"]``). ``None`` = all valid states.
    """
    session = _get_session()
    try:
        state_filter = list(states) if states else list(VALID_STATES)

        df_snow = (
            session.table(ELECTRICITY_RETAIL_SALES_RAW)
            .filter(col("SECTORID") != "ALL")
            .filter(col("STATEID").isin(state_filter))
            .select(
                "PERIOD", "STATEID", "STATEDESCRIPTION", "SECTORID", "SECTORNAME",
                "CUSTOMERS", "PRICE", "REVENUE", "SALES",
            )
            .to_pandas()
        )
    finally:
        session.close()

    df_snow = _clean_sales_df(df_snow)

    # Apply remaining filters in-memory (Snowpark string functions add overhead)
    if start_date:
        df_snow = df_snow[df_snow["PERIOD"] >= pd.Timestamp(start_date)]
    if end_date:
        df_snow = df_snow[df_snow["PERIOD"] <= pd.Timestamp(end_date)]
    if sectors:
        sectors_lower = [s.lower() for s in sectors]
        df_snow = df_snow[df_snow["SECTORNAME"].isin(sectors_lower)]

    return df_snow.reset_index(drop=True)


@st.cache_data(ttl=3600)
def load_sales_coverage() -> dict[str, Any]:
    """Return the min/max PERIOD available in the raw sales table."""
    session = _get_session()
    try:
        row = (
            session.table(ELECTRICITY_RETAIL_SALES_RAW)
            .filter(col("STATEID").isin(list(VALID_STATES)))
            .filter(col("SECTORID") != "ALL")
            .select(
                col("PERIOD").cast("string").alias("PERIOD")
            )
            .to_pandas()
        )
    finally:
        session.close()

    if row.empty:
        return {"min_period": None, "max_period": None}

    periods = pd.to_datetime(row["PERIOD"])
    return {
        "min_period": periods.min(),
        "max_period": periods.max(),
    }


@st.cache_data(ttl=3600)
def load_sales_monthly_by_sector(
    start_date: str | None = None,
    end_date: str | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """Monthly sales aggregated by sector across all (or selected) states.

    Returns columns: PERIOD, SECTORNAME, SALES, REVENUE, PRICE, CUSTOMERS.
    PRICE is averaged; all other metrics are summed.
    """
    df = load_sales_data(start_date=start_date, end_date=end_date, states=states)
    if df.empty:
        return df

    monthly = (
        df.groupby(["PERIOD", "SECTORNAME"])
        .agg(
            SALES=("SALES", "sum"),
            REVENUE=("REVENUE", "sum"),
            PRICE=("PRICE", "mean"),
            CUSTOMERS=("CUSTOMERS", "sum"),
        )
        .reset_index()
        .sort_values(["PERIOD", "SECTORNAME"])
    )
    return monthly


@st.cache_data(ttl=3600)
def load_sales_by_state(
    start_date: str | None = None,
    end_date: str | None = None,
    sectors: list[str] | None = None,
) -> pd.DataFrame:
    """Sales aggregated by state over the selected window.

    Returns columns: STATEID, STATEDESCRIPTION, SALES, REVENUE, PRICE, CUSTOMERS.
    """
    df = load_sales_data(start_date=start_date, end_date=end_date, sectors=sectors)
    if df.empty:
        return df

    by_state = (
        df.groupby(["STATEID", "STATEDESCRIPTION"])
        .agg(
            SALES=("SALES", "sum"),
            REVENUE=("REVENUE", "sum"),
            PRICE=("PRICE", "mean"),
            CUSTOMERS=("CUSTOMERS", "sum"),
        )
        .reset_index()
        .sort_values("STATEDESCRIPTION")
    )
    return by_state


@st.cache_data(ttl=3600)
def list_sales_sectors() -> list[str]:
    """Sorted list of unique sector names available in the raw table."""
    session = _get_session()
    try:
        rows = (
            session.table(ELECTRICITY_RETAIL_SALES_RAW)
            .filter(col("SECTORID") != "ALL")
            .select("SECTORNAME")
            .distinct()
            .to_pandas()
        )
    finally:
        session.close()

    return sorted(rows["SECTORNAME"].str.lower().dropna().unique().tolist())


@st.cache_data(ttl=3600)
def list_sales_states() -> list[str]:
    """Sorted list of valid STATEID values present in the raw table."""
    session = _get_session()
    try:
        rows = (
            session.table(ELECTRICITY_RETAIL_SALES_RAW)
            .filter(col("STATEID").isin(list(VALID_STATES)))
            .select("STATEID")
            .distinct()
            .to_pandas()
        )
    finally:
        session.close()

    return sorted(rows["STATEID"].dropna().unique().tolist())


@st.cache_data(ttl=3600)
def sales_table_has_rows() -> bool:
    """Quick existence check — returns True if the raw sales table is non-empty."""
    session = _get_session()
    try:
        count = (
            session.table(ELECTRICITY_RETAIL_SALES_RAW)
            .filter(col("STATEID").isin(list(VALID_STATES)))
            .count()
        )
    finally:
        session.close()

    return count > 0