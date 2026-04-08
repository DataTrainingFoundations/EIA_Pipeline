{{ config(tags=["monthly_gold"]) }}

select *
from {{ source('gold', 'FACT_SALES_MONTHLY') }}
where {{ eia_source_partition_predicate('gold', 'FACT_SALES_MONTHLY') }}
  and coalesce(fossil_pct, 0) + coalesce(renewable_pct, 0) + coalesce(nuclear_pct, 0) > 100.5
