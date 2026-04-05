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


from snowflake.snowpark import DataFrame, Session, Window
from snowflake.snowpark.functions import (
    col,
    current_timestamp,
    to_timestamp,
    trim,
    upper,
    when,
    concat,
    lit,
    row_number,
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
    "electricity_power_operational_data": "ELECTRICITY_POWER_OPERATIONAL_RAW",
    "electricity_retail_sales": "ELECTRICITY_RETAIL_SALES_RAW",
}

# Deduplication key columns per dataset
DEDUP_COLS = {
    "electricity_generation": ["PERIOD", "RESPONDENT", "FUELTYPE"],
    "electricity_demand":     ["PERIOD", "RESPONDENT", "TYPE"],
    "electricity_power_operational_data": ["PERIOD", "STATE_ID", "SECTOR_ID", "FUEL_TYPE_ID"],
    "electricity_retail_sales": ["PERIOD", "STATE_ID", "SECTOR_ABBR"],
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
        .filter(f"LEFT(_FETCHED_AT, 10) = '{date}'")
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
def _clean_retail_sales(df: DataFrame) -> DataFrame:
    """
    Clean and normalise electricity retail sales records.
 
    Source columns (after Snowflake uppercasing):
        PERIOD, STATE, STATENAME, SECTOR, SECTORNAME,
        CUSTOMERS, PRICE, REVENUE, SALES
 
    Notes:
    - PERIOD is monthly ("YYYY-MM") — converted to first-of-month timestamp.
    - No unit conversion: customers is a count, price is $/kWh, revenue is $,
      sales is MWh.  All are kept as-is in their native units.
    - Null rows are dropped only for the core identifier columns; individual
      metric columns may be null for some state/sector combinations.
    """
    return (
        df.filter(col("PERIOD").isNotNull())
        .filter(col("STATEID").isNotNull())
        .filter(col("SECTORID").isNotNull())
        .withColumn("STATEID",  upper(trim(col("STATEID"))))
        .withColumn("SECTORID", upper(trim(col("SECTORID"))))
        # Monthly period → first-of-month timestamp for consistent typing
        .withColumn(
            "period_ts",
            to_timestamp(concat(col("PERIOD"), lit("-01")), "yyyy-MM-dd"),
        )
        # Rename descriptive columns to consistent snake_case
        .withColumnRenamed("STATEDESCRIPTION",  "state_description")
        .withColumnRenamed("SECTORNAME", "sector_name")
        # Lowercase the metric columns to match silver schema convention
        .withColumnRenamed("CUSTOMERS", "customers")
        .withColumnRenamed("PRICE",     "price")
        .withColumnRenamed("REVENUE",   "revenue")
        .withColumnRenamed("SALES",     "sales")
        # Standardise identifier columns to lowercase
        .withColumnRenamed("STATEID",  "state_id")
        .withColumnRenamed("SECTORID", "sector_abbr")
        .withColumnRenamed("PERIOD", "period")
    )
 
 
def _clean_power_operational(df: DataFrame) -> DataFrame:
    """
    Clean and normalise electric power operational data records.
 
    Source columns (after Snowflake uppercasing):
        PERIOD, LOCATION, LOCATION_NAME, STATEDESCRIPTION,
        SECTORID, SECTORDESCRIPTION, FUELTYPEID, FUELTYPEDESCRIPTION,
        ASH_CONTENT, CONSUMPTION_FOR_EG, GENERATION, HEAT_CONTENT
 
    Notes:
    - PERIOD is monthly ("YYYY-MM") — converted to first-of-month timestamp.
    - No unit conversion: values are kept in their native EIA units
      (generation = MWh, consumption = MMBtu, ash-content = %, heat = MMBtu/unit).
    - Rows where ALL four metric columns are null are dropped as fully empty;
      rows with partial nulls are kept because not every fuel type reports
      every metric.
    - SECTORID is cast to integer to match the registry schema.
    """
 
    return (
        df.filter(col("PERIOD").isNotNull())
        .filter(col("LOCATION").isNotNull())
        .filter(col("FUELTYPEID").isNotNull())
        # Drop rows with no metric values at all
        .filter(
            col("GENERATION").isNotNull()
            | col("CONSUMPTION_FOR_EG").isNotNull()
            | col("ASH_CONTENT").isNotNull()
            | col("HEAT_CONTENT").isNotNull()
        )
        .withColumn("LOCATION",   upper(trim(col("LOCATION"))))
        .withColumn("FUELTYPEID", upper(trim(col("FUELTYPEID"))))
        # Monthly period → first-of-month timestamp
        .withColumn(
            "period_ts",
            to_timestamp(concat(col("PERIOD"), lit("-01")), "yyyy-MM-dd"),
        )
        # Cast sectorid to integer (arrives as string from Snowflake JSON)
        .withColumn("sector_id", col("SECTORID").cast("integer"))
        # Rename descriptive columns to snake_case
        .withColumnRenamed("STATEDESCRIPTION",     "state_description")
        .withColumnRenamed("SECTORDESCRIPTION",    "sector_description")
        .withColumnRenamed("FUELTYPEDESCRIPTION",  "fuel_type_description")
        # Rename hyphen-derived metric columns (hyphens → underscores in Snowflake)
        .withColumnRenamed("ASH_CONTENT",         "ash_content")
        .withColumnRenamed("CONSUMPTION_FOR_EG",  "consumption_for_eg")
        .withColumnRenamed("GENERATION",          "generation")
        .withColumnRenamed("HEAT_CONTENT",        "heat_content")
        # Standardise identifier columns to lowercase
        .withColumnRenamed("LOCATION",   "state_id")
        .withColumnRenamed("FUELTYPEID", "fuel_type_id")
        .withColumnRenamed("PERIOD",     "period")
        .drop("SECTORID")   # replaced by the cast lowercase version above
    )

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
    elif dataset == "electricity_demand":
        clean_df = _clean_demand(raw_df)
    elif dataset == "electricity_retail_sales":
        clean_df = _clean_retail_sales(raw_df)
    elif dataset == "electricity_power_operational_data":
        clean_df = _clean_power_operational(raw_df)
    else:
        raise ValueError(f"No cleaner implemented for dataset '{dataset}'")

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
        silver_df.write.mode("append").save_as_table(f"{SF_DB}.SILVER.{silver_table}")
    )

    logger.info("Silver write complete -> %s", silver_table)
    snowpark.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver: Snowflake RAW -> clean Parquet")
    parser.add_argument(
        "--dataset",
        required=True,
        choices=list(DATASET_TABLE_MAP),
        help=("Dataset name: electricity_generation | electricity_demand | "
              "electricity_retail_sales | electricity_power_operational_data")
    )
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.dataset, args.date)