"""UI utility helpers for the EIA Streamlit app."""

from __future__ import annotations

import os
from datetime import timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd

DEFAULT_APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/New_York")


def build_default_date_range(
    min_value: pd.Timestamp,
    max_value: pd.Timestamp,
    lookback_days: int,
) -> tuple:
    min_date = pd.to_datetime(min_value).date()
    max_date = pd.to_datetime(max_value).date()
    start_date = max(max_date - timedelta(days=lookback_days), min_date)
    return start_date, max_date


def get_timezone_options() -> list[str]:
    return list(dict.fromkeys([DEFAULT_APP_TIMEZONE, "UTC"]))


def resolve_timezone_name(timezone_name: str | None = None) -> str:
    candidate = timezone_name or DEFAULT_APP_TIMEZONE
    try:
        ZoneInfo(candidate)
        return candidate
    except ZoneInfoNotFoundError:
        return "UTC"


def convert_timestamp_series(values: pd.Series, timezone_name: str) -> pd.Series:
    tz = resolve_timezone_name(timezone_name)
    return pd.to_datetime(values, utc=True, errors="coerce").dt.tz_convert(tz)


def format_timestamp(value: pd.Timestamp | None, timezone_name: str) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    tz = resolve_timezone_name(timezone_name)
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return "n/a"
    return ts.tz_convert(tz).strftime(f"%Y-%m-%d %H:%M {tz}")


def coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def safe_quantile(series: pd.Series, quantile: float) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.quantile(quantile))
