# data_access_gold.py
"""
Gold electricity operational + sales helpers for the EIA Streamlit app.

Connects via Snowpark to the GOLD table and exposes cached query functions.
"""

from __future__ import annotations
import os
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# ── Snowpark session factory ──────────────────────────────────────────────
def _get_session() -> Session:
    """Create a Snowpark session from environment variables."""
    return Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "database":  os.getenv("SNOWFLAKE_DATABASE"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
    }).create()


# ── Table constant ────────────────────────────────────────────────────────
GOLD_OPS_SALES_TABLE = "GOLD.GOLD_ELECTRICITY_OPERATIONAL_SALES"

NUMERIC_COLS = [
    "SALES", "REVENUE", "PRICE", "CUSTOMERS",
    "GENERATION", "GENERATION_UNITS",
    "FOSSIL_PCT", "RENEWABLE_PCT", "NUCLEAR_PCT"
]

VALID_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO",
    "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}


# ── Post-processing ───────────────────────────────────────────────────────
def _clean_gold_df(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce types and normalize columns."""
    df[NUMERIC_COLS] = df[NUMERIC_COLS].apply(pd.to_numeric, errors="coerce")
    df["SECTORNAME"] = df["SECTORNAME"].str.lower()
    df["PERIOD"] = pd.to_datetime(df["PERIOD"])
    return df


# ── Public query functions ────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_gold_data(
    start_date: str | None = None,
    end_date: str | None = None,
    sectors: list[str] | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """
    Load GOLD_ELECTRICITY_OPERATIONAL_SALES with optional filters.

    Parameters
    ----------
    start_date: ISO date string "YYYY-MM-DD" inclusive lower bound.
    end_date  : ISO date string "YYYY-MM-DD" inclusive upper bound.
    sectors   : List of SECTORNAME values (lowercase). None = all.
    states    : List of STATEID values. None = all valid states.
    """
    session = _get_session()
    try:
        state_filter = list(states) if states else list(VALID_STATES)
        df_snow = (
            session.table(GOLD_OPS_SALES_TABLE)
            .filter(col("STATEID").isin(state_filter))
            .to_pandas()
        )
    finally:
        session.close()

    df_snow = _clean_gold_df(df_snow)

    if start_date:
        df_snow = df_snow[df_snow["PERIOD"] >= pd.Timestamp(start_date)]
    if end_date:
        df_snow = df_snow[df_snow["PERIOD"] <= pd.Timestamp(end_date)]
    if sectors:
        sectors_lower = [s.lower() for s in sectors]
        df_snow = df_snow[df_snow["SECTORNAME"].isin(sectors_lower)]

    return df_snow.reset_index(drop=True)


@st.cache_data(ttl=3600)
def list_gold_sectors() -> list[str]:
    """Return sorted unique sector names in the gold table."""
    session = _get_session()
    try:
        rows = session.table(GOLD_OPS_SALES_TABLE).select("SECTORNAME").distinct().to_pandas()
    finally:
        session.close()

    return sorted(rows["SECTORNAME"].str.lower().dropna().unique().tolist())


@st.cache_data(ttl=3600)
def list_gold_states() -> list[str]:
    """Return sorted STATEID values in the gold table."""
    session = _get_session()
    try:
        rows = session.table(GOLD_OPS_SALES_TABLE).select("STATEID").distinct().to_pandas()
    finally:
        session.close()

    return sorted(rows["STATEID"].dropna().unique().tolist())


@st.cache_data(ttl=3600)
def gold_table_has_rows() -> bool:
    """Quick existence check for the gold table."""
    session = _get_session()
    try:
        count = session.table(GOLD_OPS_SALES_TABLE).count()
    finally:
        session.close()
    return count > 0