from __future__ import annotations

import logging

from snowflake.snowpark import Window
from snowflake.snowpark.functions import (
    col,
    concat,
    current_timestamp,
    lit,
    md5,
    row_number,
    to_date,
    to_timestamp,
    trim,
    upper,
    when,
)

from pipeline.core.registry import get_raw_table_name, get_silver_table_name
from pipeline.core.snowflake import table_exists
from pipeline.core.windowing import month_anchor_date

logger = logging.getLogger(__name__)

DEDUP_COLS = {
    "electricity_generation": ["period", "respondent", "fueltype"],
    "electricity_demand": ["period", "respondent", "type"],
    "electricity_retail_sales": ["period", "state_id", "sector_abbr"],
    "electricity_power_operational_data": ["period", "state_id", "sector_id", "fuel_type_id"],
}

OUTPUT_COLS = {
    "electricity_generation": [
        "period",
        "period_ts",
        "business_date",
        "respondent",
        "respondent_name",
        "fueltype",
        "fuel_type_name",
        "units",
        "value",
        "value_gwh",
        "source_fetched_at",
        "source_ingested_at",
    ],
    "electricity_demand": [
        "period",
        "period_ts",
        "business_date",
        "respondent",
        "respondent_name",
        "type",
        "demand_type_name",
        "units",
        "value",
        "value_gwh",
        "source_fetched_at",
        "source_ingested_at",
    ],
    "electricity_retail_sales": [
        "period",
        "period_ts",
        "business_date",
        "state_id",
        "state_description",
        "sector_abbr",
        "sector_name",
        "customers",
        "price",
        "revenue",
        "sales",
        "source_fetched_at",
        "source_ingested_at",
    ],
    "electricity_power_operational_data": [
        "period",
        "period_ts",
        "business_date",
        "state_id",
        "state_description",
        "sector_id",
        "sector_description",
        "fuel_type_id",
        "fuel_type_description",
        "generation",
        "consumption_for_eg",
        "ash_content",
        "heat_content",
        "source_fetched_at",
        "source_ingested_at",
    ],
}

HOURLY_DATASETS = {"electricity_generation", "electricity_demand"}


def _full_dataset_id(dataset: str) -> str:
    suffix = "_hourly" if dataset in HOURLY_DATASETS else "_monthly"
    return f"{dataset}{suffix}"


def _normalized_partition_date(dataset: str, target_date: str) -> str:
    if dataset in HOURLY_DATASETS:
        return target_date
    return month_anchor_date(target_date)


def _delete_partition_if_present(session, full_table_name: str, partition_date: str) -> None:
    try:
        session.sql(f"delete from {full_table_name} where partition_date = '{partition_date}'").collect()
    except Exception:
        return


def _table_needs_replace(session, full_table_name: str, expected_columns: list[str]) -> bool:
    if not table_exists(session, full_table_name):
        return False
    existing_columns = {column.lower() for column in session.table(full_table_name).schema.names}
    return existing_columns != set(expected_columns)


def _write_partitioned_table(session, df, full_table_name: str, partition_date: str, expected_columns: list[str]) -> int:
    output = (
        df.with_column("partition_date", lit(partition_date))
        .with_column("silver_processed_at", current_timestamp())
    )
    count = output.count()
    if _table_needs_replace(session, full_table_name, expected_columns + ["partition_date", "silver_processed_at"]):
        output.write.mode("overwrite").save_as_table(full_table_name)
        return count
    _delete_partition_if_present(session, full_table_name, partition_date)
    output.write.mode("append").save_as_table(full_table_name, column_order="name")
    return count


def read_raw_for_business_date(session, dataset: str, table_name: str, target_date: str):
    raw_df = session.table(table_name)
    if dataset in HOURLY_DATASETS:
        return raw_df.filter(f"TRY_TO_DATE(SUBSTR(PERIOD, 1, 10)) = '{target_date}'")

    target_month = month_anchor_date(target_date)[:7]
    return raw_df.filter(f"PERIOD = '{target_month}'")


def _base_metadata(df):
    return (
        df.with_column("source_fetched_at", to_timestamp(col("_FETCHED_AT")))
        .with_column("source_ingested_at", to_timestamp(col("_INGESTED_AT")))
    )


def clean_generation(df):
    return (
        _base_metadata(df)
        .filter(col("VALUE").is_not_null())
        .filter(col("VALUE") >= 0)
        .with_column("RESPONDENT", upper(trim(col("RESPONDENT"))))
        .with_column("FUELTYPE", upper(trim(col("FUELTYPE"))))
        .with_column_renamed("RESPONDENT_NAME", "respondent_name")
        .with_column_renamed("TYPE_NAME", "fuel_type_name")
        .with_column_renamed("VALUE_UNITS", "units")
        .with_column("period_ts", to_timestamp(concat(col("PERIOD"), lit(":00:00"))))
        .with_column("business_date", to_date(col("PERIOD").substr(1, 10)))
        .with_column("value_gwh", when(col("units") == "megawatthours", col("VALUE") / 1000.0).otherwise(col("VALUE")))
        .with_column_renamed("RESPONDENT", "respondent")
        .with_column_renamed("FUELTYPE", "fueltype")
        .with_column_renamed("PERIOD", "period")
        .with_column_renamed("VALUE", "value")
        .select(*OUTPUT_COLS["electricity_generation"])
    )


def clean_demand(df):
    return (
        _base_metadata(df)
        .filter(col("VALUE").is_not_null())
        .filter(col("VALUE") >= 0)
        .with_column("RESPONDENT", upper(trim(col("RESPONDENT"))))
        .with_column("TYPE", upper(trim(col("TYPE"))))
        .with_column_renamed("RESPONDENT_NAME", "respondent_name")
        .with_column_renamed("TYPE_NAME", "demand_type_name")
        .with_column_renamed("VALUE_UNITS", "units")
        .with_column("period_ts", to_timestamp(concat(col("PERIOD"), lit(":00:00"))))
        .with_column("business_date", to_date(col("PERIOD").substr(1, 10)))
        .with_column("value_gwh", when(col("units") == "megawatthours", col("VALUE") / 1000.0).otherwise(col("VALUE")))
        .with_column_renamed("RESPONDENT", "respondent")
        .with_column_renamed("TYPE", "type")
        .with_column_renamed("PERIOD", "period")
        .with_column_renamed("VALUE", "value")
        .select(*OUTPUT_COLS["electricity_demand"])
    )


def clean_retail_sales(df):
    return (
        _base_metadata(df)
        .filter(col("PERIOD").is_not_null())
        .filter(col("STATEID").is_not_null())
        .filter(col("SECTORID").is_not_null())
        .with_column("STATEID", upper(trim(col("STATEID"))))
        .with_column("SECTORID", trim(col("SECTORID").cast("string")))
        .with_column("period_ts", to_timestamp(concat(col("PERIOD"), lit("-01"))))
        .with_column("business_date", to_date(concat(col("PERIOD"), lit("-01"))))
        .with_column("CUSTOMERS", col("CUSTOMERS").cast("double"))
        .with_column("PRICE", col("PRICE").cast("double"))
        .with_column("REVENUE", col("REVENUE").cast("double"))
        .with_column("SALES", col("SALES").cast("double"))
        .with_column_renamed("STATEDESCRIPTION", "state_description")
        .with_column_renamed("SECTORNAME", "sector_name")
        .with_column_renamed("STATEID", "state_id")
        .with_column_renamed("SECTORID", "sector_abbr")
        .with_column_renamed("PERIOD", "period")
        .with_column_renamed("CUSTOMERS", "customers")
        .with_column_renamed("PRICE", "price")
        .with_column_renamed("REVENUE", "revenue")
        .with_column_renamed("SALES", "sales")
        .select(*OUTPUT_COLS["electricity_retail_sales"])
    )


def clean_power_operational(df):
    return (
        _base_metadata(df)
        .filter(col("PERIOD").is_not_null())
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
        .with_column("period_ts", to_timestamp(concat(col("PERIOD"), lit("-01"))))
        .with_column("business_date", to_date(concat(col("PERIOD"), lit("-01"))))
        .with_column("GENERATION", col("GENERATION").cast("double"))
        .with_column("CONSUMPTION_FOR_EG", col("CONSUMPTION_FOR_EG").cast("double"))
        .with_column("ASH_CONTENT", col("ASH_CONTENT").cast("double"))
        .with_column("HEAT_CONTENT", col("HEAT_CONTENT").cast("double"))
        .with_column_renamed("STATEDESCRIPTION", "state_description")
        .with_column_renamed("SECTORDESCRIPTION", "sector_description")
        .with_column_renamed("FUELTYPEDESCRIPTION", "fuel_type_description")
        .with_column_renamed("LOCATION", "state_id")
        .with_column_renamed("SECTORID", "sector_id")
        .with_column_renamed("FUELTYPEID", "fuel_type_id")
        .with_column_renamed("PERIOD", "period")
        .with_column_renamed("GENERATION", "generation")
        .with_column_renamed("CONSUMPTION_FOR_EG", "consumption_for_eg")
        .with_column_renamed("ASH_CONTENT", "ash_content")
        .with_column_renamed("HEAT_CONTENT", "heat_content")
        .select(*OUTPUT_COLS["electricity_power_operational_data"])
    )


def _payload_hash(df, dataset: str):
    columns = OUTPUT_COLS[dataset]
    expr = None
    for index, column_name in enumerate(columns):
        piece = col(column_name).cast("string")
        expr = piece if index == 0 else concat(expr, lit("|"), piece)
    return md5(expr)


def _deduplicate(clean_df, dataset: str):
    business_keys = DEDUP_COLS[dataset]
    ranked = (
        clean_df.with_column("payload_hash", _payload_hash(clean_df, dataset))
        .with_column(
            "row_rank",
            row_number().over(
                Window.partition_by(*[col(column_name) for column_name in business_keys]).order_by(
                    col("source_ingested_at").desc_nulls_last(),
                    col("source_fetched_at").desc_nulls_last(),
                    col("payload_hash").desc_nulls_last(),
                )
            ),
        )
    )
    return ranked.filter(col("row_rank") == lit(1)).drop("payload_hash", "row_rank")


def run_silver(session, dataset: str, target_date: str, database: str) -> int:
    dataset_id = _full_dataset_id(dataset)
    raw_table = get_raw_table_name(dataset_id)
    silver_table = f"{database}.SILVER.{get_silver_table_name(dataset_id)}"
    raw_df = read_raw_for_business_date(session, dataset, raw_table, target_date)
    clean_map = {
        "electricity_generation": clean_generation,
        "electricity_demand": clean_demand,
        "electricity_retail_sales": clean_retail_sales,
        "electricity_power_operational_data": clean_power_operational,
    }
    clean_df = clean_map[dataset](raw_df)
    dedup_df = _deduplicate(clean_df, dataset)
    expected_columns = [column.lower() for column in dedup_df.schema.names]
    return _write_partitioned_table(
        session,
        dedup_df,
        silver_table,
        _normalized_partition_date(dataset, target_date),
        expected_columns,
    )


def run_silver_partitions(session, dataset: str, target_dates: list[str], database: str) -> list[dict]:
    results: list[dict] = []
    dataset_id = _full_dataset_id(dataset)
    raw_table = get_raw_table_name(dataset_id)
    clean_map = {
        "electricity_generation": clean_generation,
        "electricity_demand": clean_demand,
        "electricity_retail_sales": clean_retail_sales,
        "electricity_power_operational_data": clean_power_operational,
    }
    silver_table = f"{database}.SILVER.{get_silver_table_name(dataset_id)}"
    for target_date in target_dates:
        logger.info("Building silver dataset=%s target_date=%s", dataset, target_date)
        raw_df = read_raw_for_business_date(session, dataset, raw_table, target_date)
        raw_count = raw_df.count()
        clean_df = clean_map[dataset](raw_df)
        dedup_df = _deduplicate(clean_df, dataset)
        dedup_count = dedup_df.count()
        expected_columns = [column.lower() for column in dedup_df.schema.names]
        written = _write_partitioned_table(
            session,
            dedup_df,
            silver_table,
            _normalized_partition_date(dataset, target_date),
            expected_columns,
        )
        results.append(
            {
                "partition_date": _normalized_partition_date(dataset, target_date),
                "rows_read": raw_count,
                "rows_written": written,
                "duplicates_removed": max(raw_count - dedup_count, 0),
            }
        )
        logger.info(
            "Built silver dataset=%s target_date=%s rows_read=%s rows_written=%s duplicates_removed=%s",
            dataset,
            target_date,
            raw_count,
            written,
            max(raw_count - dedup_count, 0),
        )
    return results
