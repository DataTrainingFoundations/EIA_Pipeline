from __future__ import annotations

from snowflake.snowpark.functions import col, concat, current_timestamp, lit, to_timestamp, trim, upper, when

from pipeline.core.registry import get_raw_table_name, get_silver_table_name

DEDUP_COLS = {
    "electricity_generation": ["period", "respondent", "fueltype"],
    "electricity_demand": ["period", "respondent", "type"],
}


def read_raw_for_date(session, table_name: str, target_date: str):
    return session.table(table_name).filter(f"TRY_TO_DATE(_FETCHED_AT) = '{target_date}'")


def clean_generation(df):
    return (
        df.filter(col("VALUE").is_not_null())
        .filter(col("VALUE") >= 0)
        .with_column("RESPONDENT", upper(trim(col("RESPONDENT"))))
        .with_column("FUELTYPE", upper(trim(col("FUELTYPE"))))
        .with_column_renamed("RESPONDENT_NAME", "respondent_name")
        .with_column_renamed("TYPE_NAME", "fuel_type_name")
        .with_column_renamed("VALUE_UNITS", "units")
        .with_column("PERIOD_TS", to_timestamp(concat(col("PERIOD"), lit(":00:00"))))
        .with_column("value_gwh", when(col("units") == "megawatthours", col("VALUE") / 1000.0).otherwise(col("VALUE")))
        .with_column_renamed("RESPONDENT", "respondent")
        .with_column_renamed("FUELTYPE", "fueltype")
        .with_column_renamed("PERIOD", "period")
        .with_column_renamed("VALUE", "value")
    )


def clean_demand(df):
    return (
        df.filter(col("VALUE").is_not_null())
        .filter(col("VALUE") >= 0)
        .with_column("RESPONDENT", upper(trim(col("RESPONDENT"))))
        .with_column("TYPE", upper(trim(col("TYPE"))))
        .with_column_renamed("RESPONDENT_NAME", "respondent_name")
        .with_column_renamed("TYPE_NAME", "demand_type_name")
        .with_column_renamed("VALUE_UNITS", "units")
        .with_column("PERIOD_TS", to_timestamp(concat(col("PERIOD"), lit(":00:00"))))
        .with_column("value_gwh", when(col("units") == "megawatthours", col("VALUE") / 1000.0).otherwise(col("VALUE")))
        .with_column_renamed("RESPONDENT", "respondent")
        .with_column_renamed("TYPE", "type")
        .with_column_renamed("PERIOD", "period")
        .with_column_renamed("VALUE", "value")
    )


def run_silver(session, dataset: str, target_date: str, database: str) -> int:
    raw_table = get_raw_table_name(f"{dataset}_hourly")
    silver_table = f"{database}.SILVER.{get_silver_table_name(f'{dataset}_hourly')}"
    raw_df = read_raw_for_date(session, raw_table, target_date)
    clean_df = clean_generation(raw_df) if dataset == "electricity_generation" else clean_demand(raw_df)
    dedup_df = clean_df.drop_duplicates(DEDUP_COLS[dataset])
    silver_df = dedup_df.with_column("partition_date", lit(target_date)).with_column("silver_processed_at", current_timestamp())
    count = silver_df.count()
    silver_df.write.mode("overwrite").save_as_table(silver_table)
    return count
