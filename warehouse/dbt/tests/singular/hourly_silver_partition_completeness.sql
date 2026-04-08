{{ config(tags=["hourly_silver"]) }}

with raw_max as (
    select greatest(
        (select max(try_to_date(substr(period, 1, 10))) from {{ source('raw', 'ELECTRICITY_GENERATION_RAW') }}),
        (select max(try_to_date(substr(period, 1, 10))) from {{ source('raw', 'ELECTRICITY_DEMAND_RAW') }})
    ) as max_raw_business_date
),
silver_max as (
    select greatest(
        (select max(partition_date) from {{ source('silver', 'SILVER_ELECTRICITY_GENERATION') }}),
        (select max(partition_date) from {{ source('silver', 'SILVER_ELECTRICITY_DEMAND') }})
    ) as max_silver_partition
)
select *
from raw_max
cross join silver_max
where max_raw_business_date is not null
  and max_silver_partition is not null
  and datediff('day', max_silver_partition, max_raw_business_date) > 1
