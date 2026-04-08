{{ config(tags=["hourly_gold"]) }}

with silver_max as (
    select greatest(
        (select max(partition_date) from {{ source('silver', 'SILVER_ELECTRICITY_GENERATION') }}),
        (select max(partition_date) from {{ source('silver', 'SILVER_ELECTRICITY_DEMAND') }})
    ) as max_silver_partition
),
gold_max as (
    select greatest(
        (select max(partition_date) from {{ source('gold', 'FACT_GENERATION_HOURLY') }}),
        (select max(partition_date) from {{ source('gold', 'FACT_DEMAND_HOURLY') }})
    ) as max_gold_partition
)
select *
from silver_max
cross join gold_max
where max_silver_partition is not null
  and max_gold_partition is not null
  and datediff('day', max_gold_partition, max_silver_partition) > 1
