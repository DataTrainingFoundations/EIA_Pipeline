"""Shared database access helpers for the EIA Streamlit app."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
# ── Table names ───────────────────────────────────────────────────────────────
# fact_hourly is the single combined fact table — one row per
# (period_ts, ba_code, fuel_code) containing both generation and demand columns.
FACT_HOURLY             = "FACT_HOURLY"
AGG_DAILY_GENERATION    = "AGG_DAILY_GENERATION"
AGG_DAILY_DEMAND_PEAK   = "AGG_DAILY_DEMAND_PEAK"
DIM_BALANCING_AUTHORITY = "DIM_BALANCING_AUTHORITY"
DIM_FUEL_TYPE           = "DIM_FUEL_TYPE"


# ── Snowflake connection ───────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _snowflake_connection_params() -> dict[str, str]:
    """
    Build Snowflake connection params from environment variables.
    Cached so the dict is only constructed once per process.
    """
    return {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE",  "EIA"),
        "schema":    "GOLD",
    }


def get_session():
    """
    Open and return a Snowpark Session.
    A new session is created on each call — Streamlit caches results at the
    query level so sessions are short-lived and don't need pooling here.
    """
    
    connection_params = {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE",  "EIA"),
        "schema":    "GOLD"
    }
    session = Session.builder.configs(connection_params).create()
    return session


def _safe_read_sql(query: str, params: list[Any] | None = None) -> pd.DataFrame:
    """
    Execute a SQL query against Snowflake and return a pandas DataFrame.
    Uses Snowpark's session.sql() which accepts positional ? placeholders.
    Converts column names to lowercase for consistency with the rest of the app.
    """
    session = get_session()
    try:
        # Snowpark uses ? positional params, not %s
        if params:
            # Replace %s placeholders (psycopg2 style) with ? (Snowflake style)
            sf_query = query.replace("%s", "?")
            df = session.sql(sf_query, params=params).to_pandas()
        else:
            df = session.sql(query).to_pandas()
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        session.close()