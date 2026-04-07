from __future__ import annotations

import logging

from snowflake.snowpark.functions import (
    col,
    concat,
    current_timestamp,
    lit,
    max as sf_max,
    md5,
    round as sf_round,
    sum as sf_sum,
    when,
)

from pipeline.core.registry import get_gold_table_names
from pipeline.core.snowflake import table_exists
from pipeline.core.windowing import month_anchor_date

logger = logging.getLogger(__name__)

GEN_STABLE_COLS = [
    "period",
    "period_ts",
    "business_date",
    "respondent",
    "respondent_name",
    "fueltype",
    "fuel_type_name",
    "value_gwh",
]
DEM_STABLE_COLS = [
    "period",
    "period_ts",
    "business_date",
    "respondent",
    "respondent_name",
    "type",
    "demand_type_name",
    "value_gwh",
]
SALES_STABLE_COLS = [
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
]
OPS_STABLE_COLS = [
    "period",
    "period_ts",
    "business_date",
    "state_id",
    "state_description",
    "sector_id",
    "fuel_type_id",
    "generation",
]
RENEWABLE_FUELS = {"SUN", "WND", "WAT"}
FOSSIL_FUELS = {"NG", "COL"}
NUCLEAR_FUELS = {"NUC"}


def _silver_table(database: str, table_name: str) -> str:
    return f"{database}.SILVER.{table_name}"


def _gold_table(database: str, table_name: str) -> str:
    return f"{database}.GOLD.{table_name}"


def _record_hash(*parts):
    expr = None
    for index, part in enumerate(parts):
        piece = part.cast("string")
        expr = piece if index == 0 else concat(expr, lit("|"), piece)
    return md5(expr)


def _delete_partition_if_present(session, full_table_name: str, partition_date: str) -> None:
    try:
        session.sql(f"delete from {full_table_name} where partition_date = '{partition_date}'").collect()
    except Exception:
        return


def _write_partitioned_table(session, df, full_table_name: str, partition_date: str) -> int:
    output = df.with_column("gold_processed_at", current_timestamp())
    count = output.count()
    _delete_partition_if_present(session, full_table_name, partition_date)
    output.write.mode("append").save_as_table(full_table_name, column_order="name")
    return count


def _write_dimension_table(df, full_table_name: str) -> int:
    output = df.with_column("gold_processed_at", current_timestamp())
    count = output.count()
    output.write.mode("overwrite").save_as_table(full_table_name, column_order="name")
    return count


def _stabilize_generation(df):
    return (
        df.select(*GEN_STABLE_COLS)
        .dropna(subset=["period_ts", "respondent", "fueltype"])
        .drop_duplicates(GEN_STABLE_COLS)
    )


def _stabilize_demand(df):
    return (
        df.select(*DEM_STABLE_COLS)
        .dropna(subset=["period_ts", "respondent", "type"])
        .drop_duplicates(DEM_STABLE_COLS)
    )


def _stabilize_monthly_sales(df):
    return (
        df.select(*SALES_STABLE_COLS)
        .dropna(subset=["period", "state_id", "sector_abbr"])
        .drop_duplicates(SALES_STABLE_COLS)
        .filter(col("sector_abbr") != lit("ALL"))
    )


def _stabilize_monthly_ops(df):
    return (
        df.select(*OPS_STABLE_COLS)
        .dropna(subset=["period", "state_id", "sector_id", "fuel_type_id"])
        .drop_duplicates(OPS_STABLE_COLS)
    )


def _safe_pct(numerator, denominator):
    return when(denominator > lit(0), sf_round(numerator / denominator * 100, 2)).otherwise(lit(None))


def _build_hourly_generation_partition(session, target_date: str, database: str, gold_tables: dict[str, str]) -> dict[str, int]:
    generation_silver = _silver_table(database, "SILVER_ELECTRICITY_GENERATION")
    if not table_exists(session, generation_silver):
        return {}
    gen_df = session.table(generation_silver).filter(col("partition_date") == lit(target_date))
    if gen_df.count() == 0:
        return {}
    gen_df = _stabilize_generation(gen_df)
    fact_generation_name = _gold_table(database, gold_tables["fact_generation_hourly"])
    agg_generation_name = _gold_table(database, gold_tables["agg_daily_generation"])

    fact_generation = (
        gen_df.group_by("period_ts", "business_date", "respondent", "respondent_name", "fueltype", "fuel_type_name")
        .agg(sf_round(sf_sum("value_gwh"), 4).alias("generation_gwh"))
        .select(
            _record_hash(col("period_ts"), col("respondent"), col("fueltype")).alias("record_id"),
            col("period_ts"),
            col("respondent").alias("ba_code"),
            col("respondent_name").alias("ba_name"),
            col("fueltype").alias("fuel_code"),
            col("fuel_type_name").alias("fuel_name"),
            col("generation_gwh"),
            lit(target_date).alias("partition_date"),
        )
    )
    agg_generation = (
        gen_df.group_by("business_date", "fueltype", "fuel_type_name")
        .agg(sf_round(sf_sum("value_gwh"), 4).alias("total_gwh"))
        .select(
            col("business_date").alias("report_date"),
            col("fueltype").alias("fuel_code"),
            col("fuel_type_name").alias("fuel_name"),
            col("total_gwh"),
            lit(target_date).alias("partition_date"),
        )
    )
    return {
        "fact_generation_hourly": _write_partitioned_table(session, fact_generation, fact_generation_name, target_date),
        "agg_daily_generation": _write_partitioned_table(session, agg_generation, agg_generation_name, target_date),
    }


def _build_hourly_demand_partition(session, target_date: str, database: str, gold_tables: dict[str, str]) -> dict[str, int]:
    demand_silver = _silver_table(database, "SILVER_ELECTRICITY_DEMAND")
    if not table_exists(session, demand_silver):
        return {}
    dem_df = session.table(demand_silver).filter(col("partition_date") == lit(target_date))
    if dem_df.count() == 0:
        return {}
    dem_df = _stabilize_demand(dem_df)
    fact_demand_name = _gold_table(database, gold_tables["fact_demand_hourly"])
    agg_demand_peak_name = _gold_table(database, gold_tables["agg_daily_demand_peak"])

    demand_actual = (
        dem_df.filter(col("type") == "D")
        .group_by("period_ts", "business_date", "respondent", "respondent_name")
        .agg(sf_round(sf_sum("value_gwh"), 4).alias("demand_gwh"))
    )
    demand_forecast = (
        dem_df.filter(col("type") == "DF")
        .group_by("period_ts", "respondent")
        .agg(sf_round(sf_sum("value_gwh"), 4).alias("forecast_gwh"))
    )
    fact_demand = demand_actual.join(demand_forecast, on=["period_ts", "respondent"], how="left").select(
        _record_hash(col("period_ts"), col("respondent")).alias("record_id"),
        col("period_ts"),
        col("respondent").alias("ba_code"),
        col("respondent_name").alias("ba_name"),
        col("demand_gwh"),
        col("forecast_gwh"),
        lit(target_date).alias("partition_date"),
    )
    agg_demand_peak = (
        dem_df.filter(col("type") == "D")
        .group_by("business_date", "respondent", "respondent_name")
        .agg(sf_round(sf_max("value_gwh"), 4).alias("peak_gwh"))
        .select(
            col("business_date").alias("report_date"),
            col("respondent").alias("ba_code"),
            col("respondent_name").alias("ba_name"),
            col("peak_gwh"),
            lit(target_date).alias("partition_date"),
        )
    )
    return {
        "fact_demand_hourly": _write_partitioned_table(session, fact_demand, fact_demand_name, target_date),
        "agg_daily_demand_peak": _write_partitioned_table(session, agg_demand_peak, agg_demand_peak_name, target_date),
    }


def _build_monthly_operational_sales(session, database: str, target_date: str, gold_tables: dict[str, str]) -> dict[str, int]:
    partition_date = month_anchor_date(target_date)
    sales_table = _silver_table(database, "SILVER_ELECTRICITY_RETAIL_SALES")
    ops_table = _silver_table(database, "SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA")
    if not table_exists(session, sales_table) or not table_exists(session, ops_table):
        return {}

    sales_df = _stabilize_monthly_sales(
        session.table(sales_table).filter(col("partition_date") == lit(partition_date))
    )
    ops_df = _stabilize_monthly_ops(
        session.table(ops_table).filter(col("partition_date") == lit(partition_date))
    )
    if sales_df.count() == 0 or ops_df.count() == 0:
        return {}

    mix_df = (
        ops_df.group_by("period", "business_date", "state_id", "state_description")
        .agg(
            sf_round(sf_sum("generation"), 4).alias("generation"),
            sf_round(
                sf_sum(when(col("fuel_type_id").isin(list(FOSSIL_FUELS)), col("generation")).otherwise(lit(0.0))),
                4,
            ).alias("fossil_generation"),
            sf_round(
                sf_sum(when(col("fuel_type_id").isin(list(RENEWABLE_FUELS)), col("generation")).otherwise(lit(0.0))),
                4,
            ).alias("renewable_generation"),
            sf_round(
                sf_sum(when(col("fuel_type_id").isin(list(NUCLEAR_FUELS)), col("generation")).otherwise(lit(0.0))),
                4,
            ).alias("nuclear_generation"),
        )
        .select(
            col("period"),
            col("business_date"),
            col("state_id"),
            col("state_description"),
            col("generation"),
            _safe_pct(col("fossil_generation"), col("generation")).alias("fossil_pct"),
            _safe_pct(col("renewable_generation"), col("generation")).alias("renewable_pct"),
            _safe_pct(col("nuclear_generation"), col("generation")).alias("nuclear_pct"),
        )
    )

    combined = (
        sales_df.join(mix_df, on=["period", "business_date", "state_id", "state_description"], how="left")
        .select(
            _record_hash(col("period"), col("state_id"), col("sector_abbr")).alias("record_id"),
            col("business_date").alias("period"),
            col("state_id"),
            col("state_description"),
            col("sector_abbr"),
            col("sector_name"),
            col("customers"),
            col("price"),
            col("revenue"),
            col("sales"),
            col("generation"),
            col("fossil_pct"),
            col("renewable_pct"),
            col("nuclear_pct"),
            lit(partition_date).alias("partition_date"),
        )
    )
    monthly_table = _gold_table(database, gold_tables["gold_electricity_operational_sales"])
    return {
        "gold_electricity_operational_sales": _write_partitioned_table(session, combined, monthly_table, partition_date)
    }


def refresh_gold_dimensions(session, *, database: str, scope: str) -> dict[str, int]:
    if scope not in {"hourly", "all"}:
        return {}
    gold_tables = get_gold_table_names()
    results: dict[str, int] = {}
    fact_generation_name = _gold_table(database, gold_tables["fact_generation_hourly"])
    fact_demand_name = _gold_table(database, gold_tables["fact_demand_hourly"])
    dim_ba_name = _gold_table(database, gold_tables["dim_balancing_authority"])
    dim_fuel_name = _gold_table(database, gold_tables["dim_fuel_type"])

    if table_exists(session, fact_generation_name) or table_exists(session, fact_demand_name):
        ba_frames = []
        if table_exists(session, fact_generation_name):
            ba_frames.append(session.table(fact_generation_name).select(col("ba_code"), col("ba_name")))
        if table_exists(session, fact_demand_name):
            ba_frames.append(session.table(fact_demand_name).select(col("ba_code"), col("ba_name")))
        dim_ba = ba_frames[0]
        for frame in ba_frames[1:]:
            dim_ba = dim_ba.union(frame)
        results["dim_balancing_authority"] = _write_dimension_table(dim_ba.drop_duplicates(["ba_code"]), dim_ba_name)

    if table_exists(session, fact_generation_name):
        dim_fuel = session.table(fact_generation_name).select(col("fuel_code"), col("fuel_name")).drop_duplicates(["fuel_code"])
        results["dim_fuel_type"] = _write_dimension_table(dim_fuel, dim_fuel_name)
    return results


def run_gold_partitions(session, *, database: str, target_dates: list[str], scope: str = "all") -> dict[str, dict[str, int]]:
    gold_tables = get_gold_table_names()
    results: dict[str, dict[str, int]] = {}
    for target_date in target_dates:
        logger.info("Building gold scope=%s target_date=%s", scope, target_date)
        partition_results: dict[str, int] = {}
        if scope in {"all", "hourly"}:
            partition_results.update(_build_hourly_generation_partition(session, target_date, database, gold_tables))
            partition_results.update(_build_hourly_demand_partition(session, target_date, database, gold_tables))
        if scope in {"all", "monthly"}:
            partition_results.update(_build_monthly_operational_sales(session, database, target_date, gold_tables))
        results[target_date] = partition_results
        logger.info("Built gold scope=%s target_date=%s results=%s", scope, target_date, partition_results)
    return results


def run_gold(session, target_date: str, database: str, scope: str = "all") -> dict[str, int]:
    results = run_gold_partitions(session, database=database, target_dates=[target_date], scope=scope)
    refresh_gold_dimensions(session, database=database, scope=scope)
    return results.get(target_date, {})
