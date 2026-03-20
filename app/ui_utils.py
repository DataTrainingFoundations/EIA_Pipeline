from __future__ import annotations

import os
from datetime import timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd

DEFAULT_APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/New_York")


def build_default_date_range(
    min_value: pd.Timestamp, max_value: pd.Timestamp, lookback_days: int
) -> tuple:
    min_date = pd.to_datetime(min_value).date()
    max_date = pd.to_datetime(max_value).date()
    start_date = max(max_date - timedelta(days=lookback_days), min_date)
    return start_date, max_date


def get_timezone_options() -> list[str]:
    options = [DEFAULT_APP_TIMEZONE, "UTC"]
    return list(dict.fromkeys(options))


def resolve_timezone_name(timezone_name: str | None = None) -> str:
    candidate = timezone_name or DEFAULT_APP_TIMEZONE
    try:
        ZoneInfo(candidate)
        return candidate
    except ZoneInfoNotFoundError:
        return "UTC"


def convert_timestamp_series(values: pd.Series, timezone_name: str) -> pd.Series:
    target_timezone = resolve_timezone_name(timezone_name)
    timestamp_series = pd.to_datetime(values, utc=True, errors="coerce")
    return timestamp_series.dt.tz_convert(target_timezone)


def format_timestamp(value: pd.Timestamp | None, timezone_name: str) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    target_timezone = resolve_timezone_name(timezone_name)
    timestamp_value = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp_value):
        return "n/a"
    return timestamp_value.tz_convert(target_timezone).strftime(
        f"%Y-%m-%d %H:%M {target_timezone}"
    )


def coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    cleaned_df = df.copy()
    for column in columns:
        if column in cleaned_df.columns:
            cleaned_df[column] = pd.to_numeric(cleaned_df[column], errors="coerce")
    return cleaned_df


def latest_non_null_value(
    df: pd.DataFrame, value_column: str, required_columns: list[str]
) -> pd.Timestamp | None:
    valid_df = df.dropna(subset=required_columns, how="all")
    if valid_df.empty:
        return None
    return valid_df[value_column].max()


def safe_quantile(series: pd.Series, quantile: float) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.quantile(quantile))


def rank_priority_labels(
    df: pd.DataFrame, label_column: str, order: list[str]
) -> pd.DataFrame:
    ranked_df = df.copy()
    ranked_df[label_column] = pd.Categorical(
        ranked_df[label_column], categories=order, ordered=True
    )
    return ranked_df.sort_values(
        [label_column, "respondent"], ascending=[True, True]
    ).reset_index(drop=True)
