{{ config(tags=["monthly_gold"]) }}

select *
from {{ source('gold', 'GOLD_ELECTRICITY_OPERATIONAL_SALES') }}
where {{ eia_source_partition_predicate('gold', 'GOLD_ELECTRICITY_OPERATIONAL_SALES') }}
  and coalesce(fossil_pct, 0) + coalesce(renewable_pct, 0) + coalesce(nuclear_pct, 0) > 100.5
