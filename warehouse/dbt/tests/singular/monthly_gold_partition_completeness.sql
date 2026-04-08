{{ config(tags=["monthly_gold"]) }}

with silver_max as (
    select greatest(
        (select max(partition_date) from {{ source('silver', 'SILVER_ELECTRICITY_RETAIL_SALES') }}),
        (select max(partition_date) from {{ source('silver', 'SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA') }})
    ) as max_silver_partition
),
gold_max as (
    select max(partition_date) as max_gold_partition
    from {{ source('gold', 'GOLD_ELECTRICITY_OPERATIONAL_SALES') }}
)
select *
from silver_max
cross join gold_max
where max_silver_partition is not null
  and max_gold_partition is not null
  and datediff('month', max_gold_partition, max_silver_partition) > 1
