{{ config(tags=["hourly_gold"]) }}

with silver_generation as (
    select
        period_ts,
        respondent as ba_code,
        fueltype as fuel_code,
        round(sum(value_gwh), 4) as expected_generation_gwh,
        partition_date
    from {{ source('silver', 'SILVER_ELECTRICITY_GENERATION') }}
    where {{ eia_source_partition_predicate('silver', 'SILVER_ELECTRICITY_GENERATION') }}
    group by 1, 2, 3, 5
),
gold_generation as (
    select
        period_ts,
        ba_code,
        fuel_code,
        round(generation_gwh, 4) as actual_generation_gwh,
        partition_date
    from {{ source('gold', 'FACT_GENERATION_HOURLY') }}
    where {{ eia_source_partition_predicate('gold', 'FACT_GENERATION_HOURLY') }}
)
select
    coalesce(s.period_ts, g.period_ts) as period_ts,
    coalesce(s.ba_code, g.ba_code) as ba_code,
    coalesce(s.fuel_code, g.fuel_code) as fuel_code,
    coalesce(s.partition_date, g.partition_date) as partition_date,
    s.expected_generation_gwh,
    g.actual_generation_gwh
from silver_generation s
full outer join gold_generation g
    on s.period_ts = g.period_ts
   and s.ba_code = g.ba_code
   and s.fuel_code = g.fuel_code
   and s.partition_date = g.partition_date
where coalesce(s.expected_generation_gwh, -1) <> coalesce(g.actual_generation_gwh, -1)
