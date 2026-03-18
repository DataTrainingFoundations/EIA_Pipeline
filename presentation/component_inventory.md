# Component Inventory

## Runtime Services

From `docker-compose.yml`:

- `postgres`
- `minio`
- `minio-init`
- `kafka`
- `spark-master`
- `spark-worker-1`
- `spark-worker-2`
- `airflow`
- `ingestion`
- `app`
- `admin-app`

## Dataset Registry Entries

From `ingestion/src/dataset_registry.yml`:

### `electricity_region_data`

- route: `electricity/rto/region-data`
- topic: `eia_electricity_region_data`
- frequency: `hourly`
- Bronze: `s3a://bronze/electricity_region_data`
- checkpoint: `s3a://bronze/checkpoints/electricity_region_data`
- Silver: `s3a://silver/region_data`
- Gold: `s3a://gold/facts/region_demand_forecast_hourly`
- Platinum target: `platinum.region_demand_daily`

### `electricity_fuel_type_data`

- route: `electricity/rto/fuel-type-data`
- topic: `eia_electricity_fuel_type_data`
- frequency: `hourly`
- Bronze: `s3a://bronze/electricity_fuel_type_data`
- checkpoint: `s3a://bronze/checkpoints/electricity_fuel_type_data`
- Silver: `s3a://silver/fuel_type_data`
- Gold: `s3a://gold/facts/fuel_generation_hourly`

### `electricity_power_operational_data`

- route: `electricity/electric-power-operational-data`
- topic: `eia_electricity_power_operational_data`
- frequency: `monthly`
- Bronze: `s3a://bronze/electricity_power_operational_data`
- checkpoint: `s3a://bronze/checkpoints/electricity_power_operational_data`
- Silver: `s3a://silver/electric_power_operational_data`
- Gold: `s3a://gold/facts/electric_power_operations_monthly`
- Platinum target: `platinum.electric_power_operations_monthly`

## Airflow DAGs

### Active dataset incrementals

- `electricity_region_data_incremental`
- `electricity_fuel_type_data_incremental`
- `electricity_power_operational_data_incremental`

### Active dataset backfills

- `electricity_region_data_backfill`
- `electricity_fuel_type_data_backfill`
- `electricity_power_operational_data_backfill`

### Active serving DAGs

- `platinum_grid_operations_hourly`
- `platinum_resource_planning_daily`

### Present but not actively auto-unpaused

- Bronze verification DAGs
- Bronze repair DAGs

## Spark Jobs

### Bronze

- `bronze_kafka_to_minio.py`
- `bronze_hourly_coverage_verify.py`

### Silver

- `silver_clean_transform.py`

### Gold

- `gold_region_fuel_serving_hourly.py`
- `gold_power_operations_monthly.py`

### Platinum

- `platinum_region_demand_daily.py`
- `platinum_grid_operations_hourly.py`
- `platinum_resource_planning_daily.py`
- `platinum_power_operations_monthly.py`

## Postgres Tables

### `ops` schema

- `ops.backfill_jobs`
- `ops.bronze_hourly_coverage`
- `ops.bronze_repair_jobs`

### `platinum` schema

- `platinum.region_demand_daily`
- `platinum.grid_operations_hourly`
- `platinum.grid_operations_alert_hourly`
- `platinum.resource_planning_daily`
- `platinum.electric_power_operations_monthly`

## Apps

### Business app

- home: `app/streamlit_app.py`
- pages:
  - `app/pages/grid_operations_manager.py`
  - `app/pages/resource_planning_lead.py`
  - `app/pages/utility_strategy_director.py` (empty)

### Admin app

- home: `admin_app/streamlit_app.py`
- pages for overview, comparisons, Airflow status, and log exploration
