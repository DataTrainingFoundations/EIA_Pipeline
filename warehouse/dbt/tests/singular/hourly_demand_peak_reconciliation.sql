{{ config(tags=["hourly_gold"]) }}

with fact_peaks as (
    select
        cast(period_ts as date) as report_date,
        ba_code,
        round(max(demand_gwh), 4) as fact_peak_gwh,
        partition_date
    from {{ source('gold', 'FACT_DEMAND_HOURLY') }}
    where {{ eia_source_partition_predicate('gold', 'FACT_DEMAND_HOURLY') }}
    group by 1, 2, 4
),
agg_peaks as (
    select
        report_date,
        ba_code,
        round(peak_gwh, 4) as agg_peak_gwh,
        partition_date
    from {{ source('gold', 'AGG_DAILY_DEMAND_PEAK') }}
    where {{ eia_source_partition_predicate('gold', 'AGG_DAILY_DEMAND_PEAK') }}
)
select
    coalesce(f.report_date, a.report_date) as report_date,
    coalesce(f.ba_code, a.ba_code) as ba_code,
    coalesce(f.partition_date, a.partition_date) as partition_date,
    f.fact_peak_gwh,
    a.agg_peak_gwh
from fact_peaks f
full outer join agg_peaks a
    on f.report_date = a.report_date
   and f.ba_code = a.ba_code
   and f.partition_date = a.partition_date
where coalesce(f.fact_peak_gwh, -1) <> coalesce(a.agg_peak_gwh, -1)
