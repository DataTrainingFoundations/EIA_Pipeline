{{ config(tags=["hourly_gold"]) }}

with silver_actual as (
    select
        period_ts,
        respondent as ba_code,
        round(sum(value_gwh), 4) as expected_demand_gwh,
        partition_date
    from {{ source('silver', 'SILVER_ELECTRICITY_DEMAND') }}
    where type = 'D'
      and {{ eia_source_partition_predicate('silver', 'SILVER_ELECTRICITY_DEMAND') }}
    group by 1, 2, 4
),
gold_demand as (
    select
        period_ts,
        ba_code,
        round(demand_gwh, 4) as actual_demand_gwh,
        partition_date
    from {{ source('gold', 'FACT_DEMAND_HOURLY') }}
    where {{ eia_source_partition_predicate('gold', 'FACT_DEMAND_HOURLY') }}
)
select
    coalesce(s.period_ts, g.period_ts) as period_ts,
    coalesce(s.ba_code, g.ba_code) as ba_code,
    coalesce(s.partition_date, g.partition_date) as partition_date,
    s.expected_demand_gwh,
    g.actual_demand_gwh
from silver_actual s
full outer join gold_demand g
    on s.period_ts = g.period_ts
   and s.ba_code = g.ba_code
   and s.partition_date = g.partition_date
where coalesce(s.expected_demand_gwh, -1) <> coalesce(g.actual_demand_gwh, -1)
