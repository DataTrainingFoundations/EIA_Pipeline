"""
data_access_operational_sales.py
=================================
Gold electricity operational + sales helpers for the EIA Streamlit app.
Connects via Snowpark to FACT_ELECTRICITY_OPERATIONAL_SALES.
"""

from __future__ import annotations
import os
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

FACT_TABLE = "EIA_DB.GOLD.FACT_ELECTRICITY_OPERATIONAL_SALES"

def _get_session() -> Session:
    return Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "database":  os.getenv("SNOWFLAKE_DATABASE"),
        "schema":    "GOLD",
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
    }).create()

@st.cache_data(ttl=3600)
@st.cache_data(ttl=3600)
def load_sales_gold() -> pd.DataFrame:
    session = _get_session()
    try:
        df = session.table(FACT_TABLE).to_pandas()
    finally:
        session.close()

    # No duplicate PERIOD columns anymore — just rename and parse directly
    df = df.rename(columns={
        "STATE_ID":         "STATEDESCRIPTION",
        "SECTOR_ABBR":      "SECTORNAME",
        "RETAIL_SALES_MWH": "SALES",
        "AVG_PRICE":        "PRICE",
        "TOTAL_GEN":        "GENERATION",
    })

    for c in ["SALES", "REVENUE", "PRICE", "CUSTOMERS", "GENERATION"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # PERIOD is YYYY-MM — parse as first of month
    df["PERIOD"] = pd.to_datetime(df["PERIOD"] + "-01")
    return df.reset_index(drop=True)

@st.cache_data(ttl=3600)
def load_gold_coverage():
    session = _get_session()
    try:
        row = session.sql(f"SELECT MIN(PERIOD) as min_period, MAX(PERIOD) as max_period FROM {FACT_TABLE}").collect()[0]
    finally:
        session.close()
    return {
        "min_period": pd.Timestamp(row["MIN_PERIOD"] + "-01"),
        "max_period": pd.Timestamp(row["MAX_PERIOD"] + "-01"),
    }

def gold_table_has_rows() -> bool:
    session = _get_session()
    try:
        count = session.table(FACT_TABLE).count()
    finally:
        session.close()
    return count > 0