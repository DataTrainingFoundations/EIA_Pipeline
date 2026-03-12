"""Shared database access helpers for the business Streamlit app.

This module owns the Postgres connection settings and the low-level SQL helper
used by the page-specific data access modules.
"""

from __future__ import annotations

import os
from contextlib import closing
from typing import Any

import pandas as pd
import psycopg2

PLATINUM_TABLE = "platinum.region_demand_daily"
GRID_OPERATIONS_TABLE = "platinum.grid_operations_hourly"
GRID_OPERATIONS_ALERT_TABLE = "platinum.grid_operations_alert_hourly"
RESOURCE_PLANNING_TABLE = "platinum.resource_planning_daily"


def _connection_kwargs() -> dict[str, object]:
    """Return the configured Postgres connection settings for the app."""

    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "platform"),
        "user": os.getenv("POSTGRES_USER", "platform"),
        "password": os.getenv("POSTGRES_PASSWORD", "platform"),
    }


def get_connection():
    """Open a Postgres connection for the Streamlit app."""

    return psycopg2.connect(**_connection_kwargs())


def _safe_read_sql(query: str, params: list[Any] | None = None) -> pd.DataFrame:
    """Execute a SQL query and return the result as a DataFrame."""

    with closing(get_connection()) as conn:
        return pd.read_sql_query(query, conn, params=params)
