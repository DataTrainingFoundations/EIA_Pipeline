from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_non_empty(df: DataFrame, label: str) -> None:
    if df.isEmpty():
        raise ValueError(f"{label} produced no rows")


def assert_no_nulls(df: DataFrame, columns: list[str], label: str) -> None:
    null_filter = None
    for column_name in columns:
        condition = F.col(column_name).isNull()
        null_filter = condition if null_filter is None else (null_filter | condition)

    if null_filter is not None and not df.filter(null_filter).isEmpty():
        joined_columns = ", ".join(columns)
        raise ValueError(f"{label} contains null values in required columns: {joined_columns}")


def assert_unique_keys(df: DataFrame, keys: list[str], label: str) -> None:
    duplicates_df = df.groupBy(*keys).count().filter(F.col("count") > 1)
    if not duplicates_df.isEmpty():
        joined_keys = ", ".join(keys)
        raise ValueError(f"{label} contains duplicate keys: {joined_keys}")


def assert_allowed_values(df: DataFrame, column_name: str, allowed_values: list[str], label: str) -> None:
    invalid_df = df.filter(F.col(column_name).isNotNull() & (~F.col(column_name).isin(allowed_values)))
    if not invalid_df.isEmpty():
        joined_values = ", ".join(allowed_values)
        raise ValueError(f"{label} contains unsupported values for {column_name}. Allowed values: {joined_values}")


def assert_no_conflicting_records(df: DataFrame, keys: list[str], value_columns: list[str], label: str) -> None:
    comparison_struct = F.struct(*[F.col(column_name) for column_name in value_columns])
    conflicts_df = (
        df.groupBy(*keys)
        .agg(F.countDistinct(comparison_struct).alias("distinct_versions"))
        .filter(F.col("distinct_versions") > 1)
    )
    if not conflicts_df.isEmpty():
        joined_keys = ", ".join(keys)
        joined_values = ", ".join(value_columns)
        raise ValueError(f"{label} contains conflicting records for keys [{joined_keys}] across values [{joined_values}]")


def count_duplicate_keys(df: DataFrame, keys: list[str]) -> int:
    duplicate_rows = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicate_count"))
        .collect()
    )
    if not duplicate_rows:
        return 0
    duplicate_count = duplicate_rows[0]["duplicate_count"]
    return int(duplicate_count or 0)


def assert_non_negative(df: DataFrame, columns: list[str], label: str) -> None:
    invalid_filter = None
    for column_name in columns:
        condition = F.col(column_name).isNotNull() & (F.col(column_name) < 0)
        invalid_filter = condition if invalid_filter is None else (invalid_filter | condition)

    if invalid_filter is not None and not df.filter(invalid_filter).isEmpty():
        joined_columns = ", ".join(columns)
        raise ValueError(f"{label} contains negative values in columns: {joined_columns}")


def assert_value_bounds(df: DataFrame, column_name: str, *, min_value: float | None = None, max_value: float | None = None, label: str) -> None:
    invalid_filter = F.lit(False)
    if min_value is not None:
        invalid_filter = invalid_filter | (F.col(column_name).isNotNull() & (F.col(column_name) < F.lit(min_value)))
    if max_value is not None:
        invalid_filter = invalid_filter | (F.col(column_name).isNotNull() & (F.col(column_name) > F.lit(max_value)))

    if not df.filter(invalid_filter).isEmpty():
        bounds: list[str] = []
        if min_value is not None:
            bounds.append(f">= {min_value}")
        if max_value is not None:
            bounds.append(f"<= {max_value}")
        raise ValueError(f"{label} contains values outside bounds {' and '.join(bounds)} for column {column_name}")
