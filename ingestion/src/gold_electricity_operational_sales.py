import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, year, month, substr, round as sf_round

# ── 1. Connect to Snowflake ───────────────────────────────
def _get_session() -> Session:
    return Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "database":  os.getenv("SNOWFLAKE_DATABASE"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
    }).create()


# ── 2. Load silver tables ────────────────────────────────
def load_silver_tables(session):
    ops_df = session.table("SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA")
    sales_df = session.table("SILVER_ELECTRICITY_RETAIL_SALES")
    return ops_df, sales_df


# ── 3a. Create Dimension Tables ──────────────────────────
def create_dim_tables(session, ops_df, sales_df):
    # Location dimension
    dim_location = ops_df.select(col("LOCATION"), col("STATEDESCRIPTION").alias("STATE")).distinct()
    dim_location.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_LOCATION")
    
    # Sector dimension
    dim_sector = ops_df.select(col("SECTORID"), col("SECTORNAME")).distinct()
    dim_sector.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_SECTOR")
    
    # Time dimension
    dim_time = ops_df.select(
        col("PERIOD").alias("period_ts"),
        year(col("PERIOD")).alias("year"),
        month(col("PERIOD")).alias("month"),
        substr(col("PERIOD").cast("TEXT"), 1, 7).alias("year_month")
    ).distinct()
    dim_time.write.mode("overwrite").save_as_table("EIA_DB.GOLD.DIM_TIME")
    
    print("Dimension tables created: DIM_LOCATION, DIM_SECTOR, DIM_TIME")


# ── 3b. Create Fact Table ───────────────────────────────
def create_fact_table(session, ops_df, sales_df):
    joined = ops_df.join(
        sales_df,
        (ops_df["PERIOD"] == sales_df["PERIOD"])
        & (ops_df["LOCATION"] == sales_df["LOCATION"])
        & (ops_df["SECTORID"] == sales_df["SECTORID"]),
        how="inner"
    )

    fact = joined.with_columns([
        col("PERIOD"),
        col("LOCATION"),
        col("SECTORID"),
        col("SALES"),
        col("REVENUE"),
        col("PRICE"),
        col("CUSTOMERS"),
        col("GENERATION"),
        col("GENERATION_UNITS"),
        sf_round(col("FOSSIL_PCT"), 2).alias("FOSSIL_PCT"),
        sf_round(col("RENEWABLE_PCT"), 2).alias("RENEWABLE_PCT"),
        sf_round(col("NUCLEAR_PCT"), 2).alias("NUCLEAR_PCT"),
    ])

    fact.write.mode("overwrite").save_as_table("EIA_DB.GOLD.FACT_ELECTRICITY_OPERATIONAL_SALES")
    print("Fact table created: FACT_ELECTRICITY_OPERATIONAL_SALES")


# ── 4. Main Pipeline ────────────────────────────────────
def run_gold_pipeline():
    session = _get_session()
    ops_df, sales_df = load_silver_tables(session)
    create_dim_tables(session, ops_df, sales_df)
    create_fact_table(session, ops_df, sales_df)
    session.close()


if __name__ == "__main__":
    run_gold_pipeline()