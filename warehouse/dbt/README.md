# dbt Validation

This project validates Snowflake `RAW`, `SILVER`, and `GOLD` outputs produced by the Python runtime under `pipeline/`.

## Local usage

1. Set the `DBT_*` Snowflake environment variables from `.env`.
2. Copy `profiles.yml.example` to a local dbt profiles directory if you are not using the Airflow runtime wrapper.
3. Run:

```bash
cd warehouse/dbt
dbt deps
dbt source freshness --selector freshness_hourly_raw
dbt test --selector hourly_full_validation
```

The Airflow transform DAGs call this project through `pipeline/orchestration/validation_runtime.py`.
