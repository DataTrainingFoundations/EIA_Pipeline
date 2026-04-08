{{ config(tags=["hourly_gold", "hourly_generation"]) }}

with fact_rollup as (
    select
        cast(period_ts as date) as report_date,
        fuel_code,
        round(sum(generation_gwh), 4) as fact_total_gwh,
        partition_date
    from {{ source('gold', 'FACT_GENERATION_HOURLY') }}
    where {{ eia_source_partition_predicate('gold', 'FACT_GENERATION_HOURLY') }}
    group by 1, 2, 4
),
agg_generation as (
    select
        report_date,
        fuel_code,
        round(total_gwh, 4) as agg_total_gwh,
        partition_date
    from {{ source('gold', 'AGG_DAILY_GENERATION') }}
    where {{ eia_source_partition_predicate('gold', 'AGG_DAILY_GENERATION') }}
)
select
    coalesce(f.report_date, a.report_date) as report_date,
    coalesce(f.fuel_code, a.fuel_code) as fuel_code,
    coalesce(f.partition_date, a.partition_date) as partition_date,
    f.fact_total_gwh,
    a.agg_total_gwh
from fact_rollup f
full outer join agg_generation a
    on f.report_date = a.report_date
   and f.fuel_code = a.fuel_code
   and f.partition_date = a.partition_date
where coalesce(f.fact_total_gwh, -1) <> coalesce(a.agg_total_gwh, -1)
