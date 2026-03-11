from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_time_window(df: DataFrame, column_name: str, start: str | None, end: str | None) -> DataFrame:
    filtered_df = df
    if start:
        filtered_df = filtered_df.filter(F.col(column_name) >= F.to_timestamp(F.lit(start)))
    if end:
        filtered_df = filtered_df.filter(F.col(column_name) < F.to_timestamp(F.lit(end)))
    return filtered_df
