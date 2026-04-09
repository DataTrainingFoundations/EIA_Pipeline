"""Coverage and summary queries for the EIA Streamlit app."""

from __future__ import annotations

from typing import Any

from data_access_shared import (
    AGG_DAILY_GENERATION,
    DIM_FUEL_TYPE,
    FAST_CACHE_TTL,
    FACT_DEMAND_HOURLY,
    FACT_GENERATION_HOURLY,
    FACT_SALES_MONTHLY,
    SLOW_CACHE_TTL,
    SILVER_ELECTRICITY_RETAIL_SALES,
    _safe_read_sql,
    qualified_table,
)

MONTHLY_SALES_TABLE = qualified_table("SILVER", SILVER_ELECTRICITY_RETAIL_SALES)
MONTHLY_GOLD_TABLE = qualified_table("GOLD", FACT_SALES_MONTHLY)
RAW_GENERATION_TABLE = qualified_table("RAW", "ELECTRICITY_GENERATION_RAW")
RAW_DEMAND_TABLE = qualified_table("RAW", "ELECTRICITY_DEMAND_RAW")
RAW_RETAIL_TABLE = qualified_table("RAW", "ELECTRICITY_RETAIL_SALES_RAW")
RAW_OPERATIONAL_TABLE = qualified_table("RAW", "ELECTRICITY_POWER_OPERATIONAL_RAW")
META_PIPELINE_STATE = qualified_table("META", "PIPELINE_RUN_STATE")


def table_has_rows(table_name: str = FACT_GENERATION_HOURLY) -> bool:
    query = f"select exists (select 1 from {table_name} limit 1)"
    try:
        result = _safe_read_sql(query, ttl=FAST_CACHE_TTL)
        return bool(result.iloc[0, 0])
    except Exception:
        return False


def get_generation_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(period_ts) as min_period,
        max(period_ts) as max_period,
        count(*) as row_count,
        count(distinct ba_code) as ba_count,
        count(distinct fuel_code) as fuel_count
    from {FACT_GENERATION_HOURLY}
    """
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL).iloc[0].to_dict()


def get_demand_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(period_ts) as min_period,
        max(period_ts) as max_period,
        count(*) as row_count,
        count(distinct ba_code) as ba_count
    from {FACT_DEMAND_HOURLY}
    """
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL).iloc[0].to_dict()


def get_daily_generation_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(report_date) as min_date,
        max(report_date) as max_date,
        count(*) as row_count
    from {AGG_DAILY_GENERATION}
    """
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL).iloc[0].to_dict()


def get_monthly_sales_coverage() -> dict[str, Any]:
    query = f"""
    select
        min(period) as min_period,
        max(period) as max_period,
        count(*) as row_count,
        count(distinct state_id) as state_count,
        count(distinct sector_abbr) as sector_count
    from {MONTHLY_SALES_TABLE}
    where sector_abbr != 'ALL'
    """
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL).iloc[0].to_dict()


def get_pipeline_state_summary() -> list[dict[str, Any]]:
    query = f"""
    select
        dataset_id,
        frequency,
        bootstrap_ingest_complete,
        bootstrap_transform_complete,
        last_raw_partition,
        last_silver_partition,
        last_gold_partition,
        updated_at
    from {META_PIPELINE_STATE}
    order by dataset_id
    """
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL).to_dict("records")


def get_raw_coverage_summary() -> list[dict[str, Any]]:
    query = f"""
    select 'electricity_generation_hourly' as dataset_id, min(try_to_date(substr(period, 1, 10))) as min_partition, max(try_to_date(substr(period, 1, 10))) as max_partition, count(*) as row_count from {RAW_GENERATION_TABLE}
    union all
    select 'electricity_demand_hourly' as dataset_id, min(try_to_date(substr(period, 1, 10))) as min_partition, max(try_to_date(substr(period, 1, 10))) as max_partition, count(*) as row_count from {RAW_DEMAND_TABLE}
    union all
    select 'electricity_retail_sales_monthly' as dataset_id, min(to_date(period || '-01')) as min_partition, max(to_date(period || '-01')) as max_partition, count(*) as row_count from {RAW_RETAIL_TABLE}
    union all
    select 'electricity_power_operational_data_monthly' as dataset_id, min(to_date(period || '-01')) as min_partition, max(to_date(period || '-01')) as max_partition, count(*) as row_count from {RAW_OPERATIONAL_TABLE}
    order by dataset_id
    """
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL).to_dict("records")


def get_gold_coverage_summary() -> list[dict[str, Any]]:
    query = f"""
    select 'fact_generation_hourly' as table_name, min(partition_date) as min_partition, max(partition_date) as max_partition, count(*) as row_count from {FACT_GENERATION_HOURLY}
    union all
    select 'fact_demand_hourly' as table_name, min(partition_date) as min_partition, max(partition_date) as max_partition, count(*) as row_count from {FACT_DEMAND_HOURLY}
    union all
    select 'agg_daily_generation' as table_name, min(report_date) as min_partition, max(report_date) as max_partition, count(*) as row_count from {AGG_DAILY_GENERATION}
    union all
    select 'fact_sales_monthly' as table_name, min(partition_date) as min_partition, max(partition_date) as max_partition, count(*) as row_count from {MONTHLY_GOLD_TABLE}
    order by table_name
    """
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL).to_dict("records")


def get_generation_zero_value_summary(limit: int = 14) -> list[dict[str, Any]]:
    query = f"""
    select
        report_date,
        fuel_code,
        count_if(generation_gwh = 0) as zero_rows,
        count(*) as total_rows,
        sum(generation_gwh) as total_generation_gwh
    from {FACT_GENERATION_HOURLY}
    group by 1, 2
    having count_if(generation_gwh = 0) > 0
    order by report_date desc, fuel_code
    limit {int(limit)}
    """
    return _safe_read_sql(query, ttl=FAST_CACHE_TTL).to_dict("records")


def list_ba_codes(table_name: str = FACT_GENERATION_HOURLY) -> list[str]:
    query = f"select distinct ba_code from {table_name} order by ba_code"
    df = _safe_read_sql(query, ttl=SLOW_CACHE_TTL)
    return df["ba_code"].dropna().tolist()


def list_fuel_codes() -> list[str]:
    query = f"select fuel_code, fuel_name from {DIM_FUEL_TYPE} order by fuel_code"
    df = _safe_read_sql(query, ttl=SLOW_CACHE_TTL)
    return df["fuel_code"].dropna().tolist()
