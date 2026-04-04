from __future__ import annotations

from snowflake.snowpark.functions import col, concat, current_timestamp, lit, to_timestamp, trim, upper, when

from pipeline.core.registry import get_raw_table_name, get_silver_table_name

DEDUP_COLS = {
    "electricity_generation": ["period", "respondent", "fueltype"],
    "electricity_demand": ["period", "respondent", "type"],
    "electricity_retail_sales": ["period", "stateid", "sectorid"],
    "electricity_power_operational_data": ["period", "location", "sectorid", "fueltypeid"],
}

HOURLY_DATASETS = {"electricity_generation", "electricity_demand"}


def _full_dataset_id(dataset: str) -> str:
    suffix = "_hourly" if dataset in HOURLY_DATASETS else "_monthly"
    return f"{dataset}{suffix}"


def _delete_partition_if_present(session, full_table_name: str, target_date: str) -> None:
    try:
        session.sql(f"delete from {full_table_name} where partition_date = '{target_date}'").collect()
    except Exception:
        return


def _write_partitioned_table(session, df, full_table_name: str, target_date: str) -> int:
    output = df.with_column("partition_date", lit(target_date)).with_column("silver_processed_at", current_timestamp())
    count = output.count()
    _delete_partition_if_present(session, full_table_name, target_date)
    output.write.mode("append").save_as_table(full_table_name)
    return count


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


def clean_retail_sales(df):
    return (
        df.filter(col("PERIOD").is_not_null())
        .filter(col("STATEID").is_not_null())
        .filter(col("SECTORID").is_not_null())
        .with_column("STATEID", upper(trim(col("STATEID"))))
        .with_column("SECTORID", trim(col("SECTORID").cast("string")))
        .with_column("PERIOD_TS", to_timestamp(concat(col("PERIOD"), lit("-01"))))
        .with_column("CUSTOMERS", col("CUSTOMERS").cast("double"))
        .with_column("PRICE", col("PRICE").cast("double"))
        .with_column("REVENUE", col("REVENUE").cast("double"))
        .with_column("SALES", col("SALES").cast("double"))
        .with_column_renamed("STATEDESCRIPTION", "state_description")
        .with_column_renamed("SECTORNAME", "sector_name")
        .with_column_renamed("STATEID", "stateid")
        .with_column_renamed("SECTORID", "sectorid")
        .with_column_renamed("PERIOD", "period")
    )


def clean_power_operational(df):
    return (
        df.filter(col("PERIOD").is_not_null())
        .filter(col("LOCATION").is_not_null())
        .filter(col("SECTORID").is_not_null())
        .filter(col("FUELTYPEID").is_not_null())
        .filter(
            col("GENERATION").is_not_null()
            | col("CONSUMPTION_FOR_EG").is_not_null()
            | col("ASH_CONTENT").is_not_null()
            | col("HEAT_CONTENT").is_not_null()
        )
        .with_column("LOCATION", upper(trim(col("LOCATION"))))
        .with_column("SECTORID", trim(col("SECTORID").cast("string")))
        .with_column("FUELTYPEID", upper(trim(col("FUELTYPEID"))))
        .with_column("PERIOD_TS", to_timestamp(concat(col("PERIOD"), lit("-01"))))
        .with_column("GENERATION", col("GENERATION").cast("double"))
        .with_column("CONSUMPTION_FOR_EG", col("CONSUMPTION_FOR_EG").cast("double"))
        .with_column("ASH_CONTENT", col("ASH_CONTENT").cast("double"))
        .with_column("HEAT_CONTENT", col("HEAT_CONTENT").cast("double"))
        .with_column_renamed("STATEDESCRIPTION", "state_description")
        .with_column_renamed("SECTORDESCRIPTION", "sector_description")
        .with_column_renamed("FUELTYPEDESCRIPTION", "fuel_type_description")
        .with_column_renamed("GENERATION_UNITS", "generation_units")
        .with_column_renamed("CONSUMPTION_FOR_EG_UNITS", "consumption_for_eg_units")
        .with_column_renamed("HEAT_CONTENT_UNITS", "heat_content_units")
        .with_column_renamed("LOCATION", "location")
        .with_column_renamed("SECTORID", "sectorid")
        .with_column_renamed("FUELTYPEID", "fueltypeid")
        .with_column_renamed("PERIOD", "period")
    )


def run_silver(session, dataset: str, target_date: str, database: str) -> int:
    dataset_id = _full_dataset_id(dataset)
    raw_table = get_raw_table_name(dataset_id)
    silver_table = f"{database}.SILVER.{get_silver_table_name(dataset_id)}"
    raw_df = read_raw_for_date(session, raw_table, target_date)
    clean_map = {
        "electricity_generation": clean_generation,
        "electricity_demand": clean_demand,
        "electricity_retail_sales": clean_retail_sales,
        "electricity_power_operational_data": clean_power_operational,
    }
    clean_df = clean_map[dataset](raw_df)
    dedup_df = clean_df.drop_duplicates(DEDUP_COLS[dataset])
    return _write_partitioned_table(session, dedup_df, silver_table, target_date)
