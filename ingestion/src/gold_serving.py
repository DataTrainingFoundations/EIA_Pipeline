"""
gold_serving.py
===============

ADDING NEW SERVING TABLES
--------------------------
Add a new _build_<table>() function and call it from run().
The function receives a SparkSession and the date string and should call
write_gold() with the output table name.
"""

from __future__ import annotations

import argparse
import logging
import os

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import (
    col,
    concat,
    current_timestamp,
    lit,
    max as spark_max,
    md5,
    round as spark_round,
    sum as spark_sum,
    to_date,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Config ─────────────────────────────────────────────────────────────────────
# MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_USER     = os.environ.get("MINIO_ROOT_USER", "minioadmin")
# MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

# GOLD_BASE = "s3a://gold/eia"
SF_DB = os.environ.get("SNOWFLAKE_DATABASE", "EIA")

# ── Spark session ──────────────────────────────────────────────────────────────

def _build_snowpark():
    connection_params = {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE",  "EIA"),
        "schema":    "SILVER"
    }
    session = Session.builder.configs(connection_params).create()
    logger.info(
        "Snowpark session opened → %s.%s",
        connection_params["database"],
        connection_params["schema"],
    )
    return session


# ── Writer ─────────────────────────────────────────────────────────────────────

def _write(df, table_name: str):
    """Write to Snowflake GOLD schema"""
    out = df.withColumn("gold_processed_at", current_timestamp())

    full_table = f"{SF_DB}.GOLD.{table_name.upper()}"

    logger.info("Writing → %s", full_table)

    (
        out.write
        .mode("overwrite")
        .save_as_table(full_table)
    )

    logger.info("Done → %s", full_table)


# ── Serving table builders ─────────────────────────────────────────────────────

def _build_dim_balancing_authority(
    gen_df: DataFrame,
    dem_df: DataFrame,
    date: str,
) -> None:
    """dim_balancing_authority — unique BA codes + names from both datasets."""

    gen_bas = gen_df.select(
        col("respondent").alias("ba_code"),
        col("respondent_name").alias("ba_name"),
    )
    dem_bas = dem_df.select(
        col("respondent").alias("ba_code"),
        col("respondent_name").alias("ba_name"),
    )
    dim = (
        gen_bas.union(dem_bas)
        .dropDuplicates(["ba_code"])
        .orderBy("ba_code")
    )
    _write(dim, "dim_balancing_authority")


def _build_dim_fuel_type(gen_df: DataFrame, date: str) -> None:
    """dim_fuel_type — unique fuel codes + names."""
    dim = (
        gen_df.select(
            col("fueltype").alias("fuel_code"),
            col("fuel_type_name").alias("fuel_name"),
        )
        .dropDuplicates(["fuel_code"])
        .orderBy("fuel_code")
    )
    _write(dim, "dim_fuel_type")


def _build_fact_generation_hourly(gen_df: DataFrame, date: str) -> None:
    """
    fact_generation_hourly — one row per (period_ts, ba_code, fuel_code).
    record_id = MD5(period_ts | ba_code | fuel_code) for upsert safety.
    """
    # Aggregate within the date partition first (silver may have duplicates
    # across overlapping ingest windows).
    agg = (
        gen_df.groupBy("period_ts", "respondent", "respondent_name", "fueltype", "fuel_type_name")
        .agg(spark_round(spark_sum("value_gwh"), 4).alias("generation_gwh"))
    )
    fact = agg.select(
        md5(
            concat(
                col("PERIOD_TS"),
                lit("|"),
                col("RESPONDENT"),
                lit("|"),
                col("FUELTYPE")
            )
        ).alias("record_id"),
        col("PERIOD_TS"),
        col("RESPONDENT").alias("ba_code"),
        col("RESPONDENT_NAME").alias("ba_name"),
        col("FUELTYPE").alias("fuel_code"),
        col("FUEL_TYPE_NAME").alias("fuel_name"),
        col("GENERATION_GWH"),
        lit(date).alias("partition_date"),
    )
    _write(fact, "fact_generation_hourly")


def _build_fact_demand_hourly(dem_df: DataFrame, date: str) -> None:
    """
    fact_demand_hourly — one row per (period_ts, ba_code).
    Pivots demand (D) and forecast (DF) type rows into two columns.
    record_id = MD5(period_ts | ba_code).
    """
    demand = (
        dem_df.filter(col("type") == "D")
        .groupBy("PERIOD_TS", "RESPONDENT", "RESPONDENT_NAME")
        .agg(spark_round(spark_sum("VALUE_GWH"), 4).alias("demand_gwh"))
    )
    forecast = (
        dem_df.filter(col("type") == "DF")
        .groupBy("PERIOD_TS", "RESPONDENT")
        .agg(spark_round(spark_sum("VALUE_GWH"), 4).alias("forecast_gwh"))
    )
    fact = (
        demand.join(forecast, on=["PERIOD_TS", "RESPONDENT"], how="left")
        .select(
            md5(
                concat(
                    col("PERIOD_TS"),
                    lit("|"),
                    col("RESPONDENT"),
                )
            ).alias("record_id"),
            col("PERIOD_TS"),
            col("RESPONDENT").alias("ba_code"),
            col("RESPONDENT_NAME").alias("ba_name"),
            col("demand_gwh"),
            col("forecast_gwh"),
            lit(date).alias("partition_date"),
        )
    )
    _write(fact, "fact_demand_hourly")


def _build_agg_daily_generation(gen_df: DataFrame, date: str) -> None:
    """
    agg_daily_generation — daily total GWh per fuel type across all BAs.
    Used by the Streamlit trend charts.
    """
    agg = (
        gen_df.withColumn("report_date", to_date("period_ts"))
        .groupBy("report_date", "fueltype", "fuel_type_name")
        .agg(spark_round(spark_sum("value_gwh"), 4).alias("total_gwh"))
        .select(
            col("report_date"),
            col("fueltype").alias("fuel_code"),
            col("fuel_type_name").alias("fuel_name"),
            col("total_gwh"),
            lit(date).alias("partition_date"),
        )
        .orderBy("report_date", "fuel_code")
    )
    _write(agg, "agg_daily_generation")


def _build_agg_daily_demand_peak(dem_df: DataFrame, date: str) -> None:
    """
    agg_daily_demand_peak — daily peak demand GWh per balancing authority.
    Used by the Streamlit KPI tiles.
    """
    agg = (
        dem_df.filter(col("type") == "D")
        .withColumn("report_date", to_date("period_ts"))
        .groupBy("report_date", "respondent", "respondent_name")
        .agg(spark_round(spark_max("value_gwh"), 4).alias("peak_gwh"))
        .select(
            col("report_date"),
            col("respondent").alias("ba_code"),
            col("respondent_name").alias("ba_name"),
            col("peak_gwh"),
            lit(date).alias("partition_date"),
        )
        .orderBy("report_date", "ba_code")
    )
    _write(agg, "agg_daily_demand_peak")


# ── Entry point ────────────────────────────────────────────────────────────────

def run(date: str) -> None:
    session = _build_snowpark()
    logger.info("Building gold serving tables for date: %s", date)

    # ── Read silver inputs ────────────────────────────────────────────────────
    gen_df = session.table("SILVER_ELECTRICITY_GENERATION")
    dem_df = session.table("SILVER_ELECTRICITY_DEMAND")

    gen_df = gen_df.filter(to_date(col("period_ts")) == date)
    dem_df = dem_df.filter(to_date(col("period_ts")) == date)

    # Cache — each DataFrame is read by multiple builders

    # ── Build all serving tables ──────────────────────────────────────────────
    _build_dim_balancing_authority(gen_df, dem_df, date)
    _build_dim_fuel_type(gen_df, date)
    _build_fact_generation_hourly(gen_df, date)
    _build_fact_demand_hourly(dem_df, date)
    _build_agg_daily_generation(gen_df, date)
    _build_agg_daily_demand_peak(dem_df, date)

    logger.info("All gold serving tables complete for %s", date)
    session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Serving: silver → gold")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.date)
