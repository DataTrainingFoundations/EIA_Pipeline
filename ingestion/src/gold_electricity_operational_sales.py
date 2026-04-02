"""
gold_electricity_operational_sales.py
-------------------------------------
Transforms silver operations and retail sales tables into a gold table
ready for the Monthly Sales Trends dashboard.
"""

import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, year, month, substr, round as sf_round

# ── 1. Connect to Snowflake ───────────────────────────────────────────────
def _get_session() -> Session:
    return Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "database":  os.getenv("SNOWFLAKE_DATABASE"),  # This can remain your shared DB
        "schema":    os.getenv("SNOWFLAKE_SCHEMA"),    # This can remain your shared schema
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
    }).create()

# ── 2. Load silver tables ────────────────────────────────────────────────
def load_silver_tables(session):
    ops_df = session.table("SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA")
    sales_df = session.table("SILVER_ELECTRICITY_RETAIL_SALES")
    return ops_df, sales_df

# ── 3. Transform & join ─────────────────────────────────────────────────
def create_gold_table(session, ops_df, sales_df):
    # Join on PERIOD + LOCATION + SECTORID
    joined = ops_df.join(
        sales_df,
        (ops_df["PERIOD"] == sales_df["PERIOD"])
        & (ops_df["LOCATION"] == sales_df["LOCATION"])
        & (ops_df["SECTORID"] == sales_df["SECTORID"]),
        how="inner"
    )

    # Derived fields
    gold = joined.with_columns([
        col("PERIOD"),
        col("LOCATION"),
        col("SECTORID"),
        col("STATEDESCRIPTION"),
        col("SECTORNAME"),
        col("SALES"),
        col("REVENUE"),
        col("PRICE"),
        col("CUSTOMERS"),
        col("GENERATION"),
        col("GENERATION_UNITS"),
        sf_round(col("FOSSIL_PCT"), 2).alias("FOSSIL_PCT"),
        sf_round(col("RENEWABLE_PCT"), 2).alias("RENEWABLE_PCT"),
        sf_round(col("NUCLEAR_PCT"), 2).alias("NUCLEAR_PCT"),
        year(col("PERIOD")).alias("YEAR"),
        month(col("PERIOD")).alias("MONTH"),
        substr(col("PERIOD").cast("TEXT"), 1, 7).alias("YEAR_MONTH")  # YYYY-MM
    ])

    # Write to your new database/schema
    gold_table_name = "GOLD_ELECTRICITY_OPERATIONAL_SALES"
    gold.write.mode("overwrite").save_as_table(f"EIA_DB.GOLD.{gold_table_name}")

    print(f"Gold table {gold_table_name} successfully written to EIA_DB.GOLD")

# ── 4. Main pipeline ────────────────────────────────────────────────────
if __name__ == "__main__":
    session = _get_session()
    ops_df, sales_df = load_silver_tables(session)
    create_gold_table(session, ops_df, sales_df)
    session.close()