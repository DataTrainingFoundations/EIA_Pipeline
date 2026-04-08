{{ config(tags=["monthly_gold"]) }}

with sales_silver as (
    select
        business_date as period,
        state_id,
        state_description,
        sector_abbr,
        round(customers, 4) as customers,
        round(price, 4) as price,
        round(revenue, 4) as revenue,
        round(sales, 4) as sales,
        partition_date
    from {{ source('silver', 'SILVER_ELECTRICITY_RETAIL_SALES') }}
    where sector_abbr <> 'ALL'
      and {{ eia_source_partition_predicate('silver', 'SILVER_ELECTRICITY_RETAIL_SALES') }}
),
ops_rollup as (
    select
        business_date as period,
        state_id,
        state_description,
        round(sum(generation), 4) as generation,
        round(sum(case when fuel_type_id in ('NG', 'COL') then generation else 0 end), 4) as fossil_generation,
        round(sum(case when fuel_type_id in ('SUN', 'WND', 'WAT') then generation else 0 end), 4) as renewable_generation,
        round(sum(case when fuel_type_id = 'NUC' then generation else 0 end), 4) as nuclear_generation,
        partition_date
    from {{ source('silver', 'SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA') }}
    where {{ eia_source_partition_predicate('silver', 'SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA') }}
    group by 1, 2, 3, 8
),
ops_mix as (
    select
        period,
        state_id,
        state_description,
        generation,
        case when generation > 0 then round(fossil_generation / generation * 100, 2) end as fossil_pct,
        case when generation > 0 then round(renewable_generation / generation * 100, 2) end as renewable_pct,
        case when generation > 0 then round(nuclear_generation / generation * 100, 2) end as nuclear_pct,
        partition_date
    from ops_rollup
),
expected as (
    select
        s.period,
        s.state_id,
        s.sector_abbr,
        s.customers,
        s.price,
        s.revenue,
        s.sales,
        o.generation,
        o.fossil_pct,
        o.renewable_pct,
        o.nuclear_pct,
        s.partition_date
    from sales_silver s
    left join ops_mix o
      on s.period = o.period
     and s.state_id = o.state_id
     and s.state_description = o.state_description
     and s.partition_date = o.partition_date
),
gold_actual as (
    select
        period,
        state_id,
        sector_abbr,
        round(customers, 4) as customers,
        round(price, 4) as price,
        round(revenue, 4) as revenue,
        round(sales, 4) as sales,
        round(generation, 4) as generation,
        round(fossil_pct, 2) as fossil_pct,
        round(renewable_pct, 2) as renewable_pct,
        round(nuclear_pct, 2) as nuclear_pct,
        partition_date
    from {{ source('gold', 'FACT_SALES_MONTHLY') }}
    where {{ eia_source_partition_predicate('gold', 'FACT_SALES_MONTHLY') }}
)
select
    coalesce(e.period, g.period) as period,
    coalesce(e.state_id, g.state_id) as state_id,
    coalesce(e.sector_abbr, g.sector_abbr) as sector_abbr,
    coalesce(e.partition_date, g.partition_date) as partition_date,
    e.customers as expected_customers,
    g.customers as actual_customers,
    e.sales as expected_sales,
    g.sales as actual_sales,
    e.generation as expected_generation,
    g.generation as actual_generation,
    e.fossil_pct as expected_fossil_pct,
    g.fossil_pct as actual_fossil_pct,
    e.renewable_pct as expected_renewable_pct,
    g.renewable_pct as actual_renewable_pct,
    e.nuclear_pct as expected_nuclear_pct,
    g.nuclear_pct as actual_nuclear_pct
from expected e
full outer join gold_actual g
    on e.period = g.period
   and e.state_id = g.state_id
   and e.sector_abbr = g.sector_abbr
   and e.partition_date = g.partition_date
where coalesce(e.customers, -1) <> coalesce(g.customers, -1)
   or coalesce(e.price, -1) <> coalesce(g.price, -1)
   or coalesce(e.revenue, -1) <> coalesce(g.revenue, -1)
   or coalesce(e.sales, -1) <> coalesce(g.sales, -1)
   or coalesce(e.generation, -1) <> coalesce(g.generation, -1)
   or coalesce(e.fossil_pct, -1) <> coalesce(g.fossil_pct, -1)
   or coalesce(e.renewable_pct, -1) <> coalesce(g.renewable_pct, -1)
   or coalesce(e.nuclear_pct, -1) <> coalesce(g.nuclear_pct, -1)
   or coalesce(g.sector_abbr, '') = 'ALL'
