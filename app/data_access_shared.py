"""Shared database access helpers for the EIA Streamlit app."""

from __future__ import annotations

import os
from contextlib import closing
from typing import Any

import pandas as pd
import psycopg2

# ── Table names ──────────────────────────────────────────────────────────────
FACT_GENERATION_HOURLY   = "fact_generation_hourly"
FACT_DEMAND_HOURLY       = "fact_demand_hourly"
AGG_DAILY_GENERATION     = "agg_daily_generation"
AGG_DAILY_DEMAND_PEAK    = "agg_daily_demand_peak"
DIM_BALANCING_AUTHORITY  = "dim_balancing_authority"
DIM_FUEL_TYPE            = "dim_fuel_type"


def _connection_kwargs() -> dict[str, object]:
    return {
        "host":     os.getenv("POSTGRES_HOST", "postgres"),
        "port":     int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname":   os.getenv("POSTGRES_DB", "platform"),
        "user":     os.getenv("POSTGRES_USER", "platform"),
        "password": os.getenv("POSTGRES_PASSWORD", "platform"),
    }


def get_connection():
    return psycopg2.connect(**_connection_kwargs())


def _safe_read_sql(query: str, params: list[Any] | None = None) -> pd.DataFrame:
    with closing(get_connection()) as conn:
        return pd.read_sql_query(query, conn, params=params)
