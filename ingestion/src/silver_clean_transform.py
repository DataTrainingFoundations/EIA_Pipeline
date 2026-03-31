"""
silver_clean_transform.py
=========================

INPUT
-----
    Snowflake table:  <SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA>.<TABLE>
    Filtered to rows where: TRY_TO_DATE(_FETCHED_AT) = <date>

ENVIRONMENT VARIABLES (Snowflake Spark connector)
-------------------------------------------------
SNOWFLAKE_URL        — <account>.snowflakecomputing.com
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
"""

from __future__ import annotations

import argparse
import logging
import os


from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import (
    col,
    current_timestamp,
    to_timestamp,
    trim,
    upper,
    when,
    concat,
    lit
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SF_URL      = os.environ.get("SNOWFLAKE_URL", "")
SF_USER     = os.environ.get("SNOWFLAKE_USER", "")
SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")
SF_ROLE     = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
SF_WH       = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_DB       = os.environ.get("SNOWFLAKE_DATABASE", "EIA")
SF_SCHEMA   = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")

# Dataset ID → Snowflake table name (mirrors dataset_registry.yml)
DATASET_TABLE_MAP = {
    "electricity_generation": "ELECTRICITY_GENERATION_RAW",
    "electricity_demand":     "ELECTRICITY_DEMAND_RAW",
}

# Deduplication key columns per dataset
DEDUP_COLS = {
    "electricity_generation": ["PERIOD", "RESPONDENT", "FUELTYPE"],
    "electricity_demand":     ["PERIOD", "RESPONDENT", "TYPE"],
}


def _build_snowpark():
    connection_params = {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE",  "EIA"),
        "schema":    os.environ.get("SNOWFLAKE_SCHEMA",    "RAW"),
    }
    session = Session.builder.configs(connection_params).create()
    logger.info(
        "Snowpark session opened → %s.%s",
        connection_params["database"],
        connection_params["schema"],
    )
    return session


# ── Snowflake reader ───────────────────────────────────────────────────────────

def _read_snowflake(snowpark, table: str, date: str) -> DataFrame:
    """
    Read raw rows from Snowflake for the given date using the Spark connector.
    Filters to rows fetched on `date` so each silver run is idempotent.
    """
    # sf_options = {
    #     "sfURL":       SF_URL,
    #     "sfUser":      SF_USER,
    #     "sfPassword":  SF_PASSWORD,
    #     "sfRole":      SF_ROLE,
    #     "sfWarehouse": SF_WH,
    #     "sfDatabase":  SF_DB,
    #     "sfSchema":    SF_SCHEMA,
    #     "dbtable":     table,
    # }

    logger.info("Reading from Snowflake: %s.%s.%s  (date=%s)", SF_DB, SF_SCHEMA, table, date)

    df = (
        snowpark.table(table)
        # .format("net.snowflake.spark.snowflake")
        # .options(**sf_options)
        # .load()
        # # Filter to just the rows ingested on the target date
        .filter(f"TRY_TO_DATE(_FETCHED_AT) = '{date}'")
    )

    #count = df.count()
    #logger.info("Rows read from Snowflake %s: %d", table, count)
    return df


# ── Dataset-specific cleaners ──────────────────────────────────────────────────

def _clean_generation(df: DataFrame) -> DataFrame:
    """Clean and normalise electricity generation records."""
    return (
        df.filter(col("VALUE").isNotNull())
        .filter(col("VALUE") >= 0)
        .withColumn("RESPONDENT",      upper(trim(col("RESPONDENT"))))
        .withColumn("FUELTYPE",        upper(trim(col("FUELTYPE"))))
        # Rename hyphenated Snowflake columns to underscore equivalents
        .withColumnRenamed("RESPONDENT_NAME", "respondent_name")
        .withColumnRenamed("TYPE_NAME",       "fuel_type_name")
        .withColumnRenamed("VALUE_UNITS",     "units")
        .withColumn(
            "PERIOD_TS",
            to_timestamp(concat(col("period"), lit(":00:00")))
        )
        .withColumn(
            "value_gwh",
            when(col("units") == "megawatthours", col("VALUE") / 1000.0)
            .otherwise(col("VALUE")),
        )
        # Standardise column names to lowercase for silver schema consistency
        .withColumnRenamed("RESPONDENT", "respondent")
        .withColumnRenamed("FUELTYPE",   "fueltype")
        .withColumnRenamed("PERIOD",     "period")
        .withColumnRenamed("VALUE",      "value")
    )


def _clean_demand(df: DataFrame) -> DataFrame:
    """Clean and normalise electricity demand records."""
    return (
        df.filter(col("VALUE").isNotNull())
        .filter(col("VALUE") >= 0)
        .withColumn("RESPONDENT", upper(trim(col("RESPONDENT"))))
        .withColumn("TYPE",       upper(trim(col("TYPE"))))
        .withColumnRenamed("RESPONDENT_NAME", "respondent_name")
        .withColumnRenamed("TYPE_NAME",       "demand_type_name")
        .withColumnRenamed("VALUE_UNITS",     "units")
        .withColumn(
            "PERIOD_TS",
            to_timestamp(concat(col("period"), lit(":00:00")))
        )
        .withColumn(
            "value_gwh",
            when(col("units") == "megawatthours", col("VALUE") / 1000.0)
            .otherwise(col("VALUE")),
        )
        .withColumnRenamed("RESPONDENT", "respondent")
        .withColumnRenamed("TYPE",       "type")
        .withColumnRenamed("PERIOD",     "period")
        .withColumnRenamed("VALUE",      "value")
    )

###TODO: Add clean _power_operations, and _clean retail_sales


# ── Entry point ────────────────────────────────────────────────────────────────

def run(dataset: str, date: str) -> None:
    if dataset not in DATASET_TABLE_MAP:
        raise ValueError(
            f"Unknown dataset '{dataset}'. Known: {list(DATASET_TABLE_MAP)}"
        )

    sf_table    = DATASET_TABLE_MAP[dataset]
    dedup_cols  = DEDUP_COLS[dataset]
    #silver_path = f"s3a://silver/eia/{dataset}/date={date}"
    silver_table = f"SILVER_{dataset.upper()}"
    #app_name    = f"silver_{dataset}_{date}"

    snowpark = _build_snowpark()

    # ── Read from Snowflake ───────────────────────────────────────────────────
    raw_df = _read_snowflake(snowpark, sf_table, date)

    # ── Clean ─────────────────────────────────────────────────────────────────
    if dataset == "electricity_generation":
        clean_df = _clean_generation(raw_df)
    else:
        clean_df = _clean_demand(raw_df)

    # ── Deduplicate (Snowflake may have overlapping ingest windows) ───────────
    # Dedup cols were uppercased in Snowflake; after rename they're lowercase
    lower_dedup = [c.lower() for c in dedup_cols]
    dedup_df = clean_df.dropDuplicates(lower_dedup)

    # ── Add silver metadata ───────────────────────────────────────────────────
    silver_df = dedup_df.withColumn("silver_processed_at", current_timestamp())

    record_count = silver_df.count()
    logger.info("Writing %d clean records to %s", record_count, silver_table)

    # (
    #     silver_df.write
    #     .mode("overwrite")
    #     .format("parquet")
    #     .option("compression", "snappy")
    #     .save(silver_path)
    # )

    (
        silver_df.write.mode("overwrite").save_as_table(f"{SF_DB}.SILVER.{silver_table}")
    )

    logger.info("Silver write complete -> %s", silver_table)
    snowpark.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver: Snowflake RAW -> clean Parquet")
    parser.add_argument(
        "--dataset",
        required=True,
        choices=list(DATASET_TABLE_MAP),
        help="Dataset name (electricity_generation | electricity_demand)",
    )
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.dataset, args.date)