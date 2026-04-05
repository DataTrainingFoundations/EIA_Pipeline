"""
gold_electricity_operational_sales.py
======================================
Reads from EIA_DB.SILVER silver tables and writes gold dimension and fact
tables to EIA_DB.GOLD.

Silver columns used:
    SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA:
        period, state_id, sector_id, fuel_type_id, state_description,
        sector_description, fuel_type_description, generation, generation_units,
        consumption_for_eg, ash_content, heat_content, period_ts

    SILVER_ELECTRICITY_RETAIL_SALES:
        period, state_id, sector_abbr, state_description, sector_name,
        customers, price, revenue, sales, period_ts
"""

import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, year, month, substr, round as sf_round,
    current_timestamp, sum as sf_sum, when
)


# ── 1. Connect ────────────────────────────────────────────
def _get_session() -> Session:
    return Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database":  os.getenv("SNOWFLAKE_DATABASE"),
        "schema":    "SILVER",
    }).create()


# ── 2. Load silver tables ─────────────────────────────────
def load_silver_tables(session):
    ops_df   = session.table("EIA_DB.SILVER.SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA")
    sales_df = session.table("EIA_DB.SILVER.SILVER_ELECTRICITY_RETAIL_SALES")
    return ops_df, sales_df


# ── 3a. Dimension tables ──────────────────────────────────
def create_dim_tables(session, ops_df, sales_df):

    # DIM_LOCATION — from sales (has state_description + state_id)
    dim_location = (
        sales_df
        .select(col("state_id"), col("state_description"))
        .distinct()
    )
    dim_location.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_LOCATION")
    print("✓ DIM_LOCATION")

    # DIM_SECTOR — from sales (has sector_abbr + sector_name)
    dim_sector = (
        sales_df
        .select(col("sector_abbr"), col("sector_name"))
        .distinct()
    )
    dim_sector.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_SECTOR")
    print("✓ DIM_SECTOR")

    # DIM_FUEL_TYPE — from ops
    dim_fuel = (
        ops_df
        .select(col("fuel_type_id"), col("fuel_type_description"))
        .distinct()
    )
    dim_fuel.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_FUEL_TYPE")
    print("✓ DIM_FUEL_TYPE")

    # DIM_TIME — from ops period (YYYY-MM)
    dim_time = (
        ops_df
        .select(
            col("period"),
            col("period_ts"),
            year(col("period_ts")).alias("year"),
            month(col("period_ts")).alias("month"),
        )
        .distinct()
    )
    dim_time.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_TIME")
    print("✓ DIM_TIME")


# ── 3b. Fact table ────────────────────────────────────────
def create_fact_table(session, ops_df, sales_df):
    """
    Join ops and sales on period + state_id.
    Note: ops uses sector_id (int), sales uses sector_abbr (text).
    We join on period + state_id only and carry both sector keys.
    """

    # Aggregate ops to period + state_id level (sum across fuel types)
    ops_agg = (
        ops_df
        .group_by("period", "state_id", "sector_id", "state_description", "sector_description")
        .agg(
            sf_sum(col("generation").cast("float")).alias("total_generation"),
            sf_sum(col("consumption_for_eg").cast("float")).alias("total_consumption"),
        )
    )

    # Join to sales on period + state_id
    fact = (
        ops_agg.join(
            sales_df,
            on=["period", "state_id"],
            how="inner",
        )
        .select(
            col("period"),
            col("period_ts"),
            col("state_id"),
            ops_agg["state_description"],
            col("sector_id"),
            col("sector_abbr"),
            col("sector_name"),
            col("total_generation"),
            col("total_consumption"),
            col("sales").cast("float").alias("retail_sales_mwh"),
            col("revenue").cast("float").alias("revenue"),
            col("price").cast("float").alias("avg_price"),
            col("customers").cast("float").alias("customers"),
            current_timestamp().alias("gold_processed_at"),
        )
    )

    fact.write.mode("overwrite").save_as_table("EIA_DB.GOLD.FACT_ELECTRICITY_OPERATIONAL_SALES")
    print("✓ FACT_ELECTRICITY_OPERATIONAL_SALES")


# ── 4. Main ───────────────────────────────────────────────
def run_gold_pipeline():
    session = _get_session()
    try:
        ops_df, sales_df = load_silver_tables(session)
        create_dim_tables(session, ops_df, sales_df)
        create_fact_table(session, ops_df, sales_df)
        print("[gold_monthly] Pipeline complete.")
    finally:
        session.close()


if __name__ == "__main__":
    run_gold_pipeline()