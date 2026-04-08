{{ config(tags=["monthly_silver"]) }}

with raw_max as (
    select greatest(
        (select max(to_date(period || '-01')) from {{ source('raw', 'ELECTRICITY_RETAIL_SALES_RAW') }}),
        (select max(to_date(period || '-01')) from {{ source('raw', 'ELECTRICITY_POWER_OPERATIONAL_RAW') }})
    ) as max_raw_business_date
),
silver_max as (
    select greatest(
        (select max(partition_date) from {{ source('silver', 'SILVER_ELECTRICITY_RETAIL_SALES') }}),
        (select max(partition_date) from {{ source('silver', 'SILVER_ELECTRICITY_POWER_OPERATIONAL_DATA') }})
    ) as max_silver_partition
)
select *
from raw_max
cross join silver_max
where max_raw_business_date is not null
  and max_silver_partition is not null
  and datediff('month', max_silver_partition, max_raw_business_date) > 1
