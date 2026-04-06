"""
gold_electricity_operational_sales.py
======================================
Reads from EIA_DB.SILVER and writes gold dimension and fact tables
to EIA_DB.GOLD, including fossil vs renewable fuel mix percentages.

Fuel type mapping:
    Fossil    → NG  (natural gas)
    Renewable → SUN (solar) + WND (wind)
"""

import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, year, month, current_timestamp,
    sum as sf_sum, when, round as sf_round
)

VALID_STATES = [
    "AK","AL","AR","AZ","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID",
    "IL","IN","KS","KY","LA","MA","MD","ME","MI","MN","MO","MS","MT","NC",
    "ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI","SC","SD",
    "TN","TX","UT","VA","VT","WA","WI","WV","WY"
]

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


def load_silver_tables(session):
    ops_df   = session.table("EIA_DB.SILVER.SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA")
    sales_df = session.table("EIA_DB.SILVER.SILVER_ELECTRICITY_RETAIL_SALES")
    return ops_df, sales_df


def create_dim_tables(session, ops_df, sales_df):
    dim_location = sales_df.select(col("state_id"), col("state_description")).distinct()
    dim_location.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_LOCATION")
    print("✓ DIM_LOCATION")

    dim_sector = sales_df.select(col("sector_abbr"), col("sector_name")).distinct()
    dim_sector.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_SECTOR")
    print("✓ DIM_SECTOR")

    dim_fuel = (
        ops_df
        .filter(col("fuel_type_id").isin(["NG", "SUN", "WND"]))
        .select(col("fuel_type_id"), col("fuel_type_description"))
        .distinct()
    )
    dim_fuel.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_FUEL_TYPE_MONTHLY")
    print("✓ DIM_FUEL_TYPE_MONTHLY")

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


def create_fact_table(session, ops_df, sales_df):

    # ── Aggregate sales to period + state level first ─────────────────────
    sales_agg = (
        sales_df
        .filter(col("sector_abbr") != "ALL").filter(col("state_id").isin(VALID_STATES))
        .group_by("period", "state_id", "state_description", "sector_abbr", "sector_name")
        .agg(
            sf_sum(col("sales").cast("float")).alias("retail_sales_mwh"),
            sf_sum(col("revenue").cast("float")).alias("revenue"),
            sf_sum(col("customers").cast("float")).alias("customers"),
            when(sf_sum(col("sales").cast("float")) > 0,
                sf_sum(col("price").cast("float") * col("sales").cast("float")) /
                sf_sum(col("sales").cast("float"))
            ).otherwise(None).alias("avg_price"),
        )
    )

    # ── Total generation per period + state ───────────────────────────────
    total_gen = (
        ops_df
        .group_by("period", "state_id")
        .agg(sf_sum(col("generation").cast("float")).alias("total_gen"))
    )

    # ── Fossil and renewable generation per period + state ────────────────
    fuel_agg = (
        ops_df
        .with_column(
            "fossil_gen",
            when(col("fuel_type_id") == "NG", col("generation").cast("float")).otherwise(0)
        )
        .with_column(
            "renewable_gen",
            when(col("fuel_type_id").isin(["SUN", "WND"]), col("generation").cast("float")).otherwise(0)
        )
        .group_by("period", "state_id")
        .agg(
            sf_sum(col("fossil_gen")).alias("fossil_gen"),
            sf_sum(col("renewable_gen")).alias("renewable_gen"),
        )
    )

    # ── Compute fuel mix percentages ──────────────────────────────────────
    fuel_mix = (
        fuel_agg
        .join(total_gen, on=["period", "state_id"], how="inner")
        .filter(col("total_gen") > 0)
        .select(
            col("period"),
            col("state_id"),
            sf_round((col("fossil_gen")    / col("total_gen") * 100), 2).alias("fossil_pct"),
            sf_round((col("renewable_gen") / col("total_gen") * 100), 2).alias("renewable_pct"),
            col("total_gen"),
        )
    )

    # ── Join sales + fuel mix ─────────────────────────────────────────────
    fact = (
        sales_agg
        .join(fuel_mix, on=["period", "state_id"], how="left")
        .select(
            col("period"),
            col("state_id"),
            sales_agg["state_description"],
            col("sector_abbr"),
            col("sector_name"),
            col("retail_sales_mwh"),
            col("revenue"),
            col("avg_price"),
            col("customers"),
            col("total_gen"),
            col("fossil_pct"),
            col("renewable_pct"),
            current_timestamp().alias("gold_processed_at"),
        )
    )

    fact.write.mode("overwrite").save_as_table("EIA_DB.GOLD.FACT_ELECTRICITY_OPERATIONAL_SALES")
    print("✓ FACT_ELECTRICITY_OPERATIONAL_SALES")


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