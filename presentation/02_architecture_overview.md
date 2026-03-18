# Architecture Overview

## System Shape

The runtime architecture in `docker-compose.yml` is a single local platform composed of:

- `postgres`
- `minio`
- `minio-init`
- `kafka`
- `spark-master`
- `spark-worker-1`
- `spark-worker-2`
- `airflow`
- `ingestion` tool container
- `app`
- `admin-app`

The business flow is:

`EIA API -> ingestion CLI -> Kafka -> Spark Bronze -> Spark Silver -> Spark Gold -> Spark Platinum stage tables -> Airflow merge/validation -> Postgres serving tables`

## Runtime Components

### External EIA API

All source data originates from `https://api.eia.gov/v2`, accessed by `ingestion/src/eia_api.py`.

The dataset registry in `ingestion/src/dataset_registry.yml` defines which routes are called, including:

- `electricity/rto/region-data`
- `electricity/rto/fuel-type-data`
- `electricity/electric-power-operational-data`

### Ingestion CLI

`ingestion/src/fetch_eia.py` is the Airflow-facing command entrypoint. It:

- loads dataset definitions from `dataset_registry.yml`
- translates requested source windows
- fetches paginated rows from EIA
- builds deterministic event envelopes
- publishes them into Kafka

The design is registry-driven rather than hard-coded per dataset.

### Kafka

Each dataset maps to a topic in `dataset_registry.yml`:

- `eia_electricity_region_data`
- `eia_electricity_fuel_type_data`
- `eia_electricity_power_operational_data`

Kafka acts as a bounded ingestion buffer. Topics are created on demand by `ingestion/src/publish_kafka.py`.

### Spark Cluster

Spark runs as one master plus two workers in `docker-compose.yml`.

Shared runtime behavior lives in `spark/common/*`:

- `config.py`
- `spark_session.py`
- `io.py`
- `quality.py`
- `windowing.py`
- `logging_utils.py`

Spark owns the transformation layers:

- Bronze: `bronze_kafka_to_minio.py`
- Silver: `silver_clean_transform.py`
- Gold: `gold_region_fuel_serving_hourly.py`, `gold_power_operations_monthly.py`
- Platinum: `platinum_grid_operations_hourly.py`, `platinum_resource_planning_daily.py`, `platinum_region_demand_daily.py`, `platinum_power_operations_monthly.py`

### MinIO Storage Layers

`minio-init` creates buckets:

- `bronze`
- `silver`
- `gold`

MinIO is used through S3A paths, including:

- Bronze raw event storage such as `s3a://bronze/electricity_region_data`
- Bronze checkpoints such as `s3a://bronze/checkpoints/electricity_region_data`
- Silver dataset paths such as `s3a://silver/region_data`
- Gold fact and dimension paths such as:
  - `s3a://gold/facts/region_demand_forecast_hourly`
  - `s3a://gold/facts/fuel_generation_hourly`
  - `s3a://gold/facts/electric_power_operations_monthly`
  - `s3a://gold/dimensions/respondent`
  - `s3a://gold/dimensions/fuel_type`

### Postgres Warehouse

Postgres plays two distinct roles.

#### `ops` schema

This stores pipeline metadata, including:

- `ops.backfill_jobs`
- `ops.bronze_hourly_coverage`
- `ops.bronze_repair_jobs`

#### `platinum` schema

This stores business-facing serving tables, including:

- `platinum.region_demand_daily`
- `platinum.grid_operations_hourly`
- `platinum.grid_operations_alert_hourly`
- `platinum.resource_planning_daily`
- `platinum.electric_power_operations_monthly`

### Airflow

Airflow is the orchestration layer. Its behavior is split across helpers:

- `pipeline_factories.py`
- `pipeline_dataset_dags.py`
- `pipeline_serving_dags.py`
- `pipeline_backfill.py`
- `pipeline_validation.py`
- `pipeline_builders.py`

#### DAG families

Active dataset DAGs:

- `electricity_region_data_incremental`
- `electricity_region_data_backfill`
- `electricity_fuel_type_data_incremental`
- `electricity_fuel_type_data_backfill`
- `electricity_power_operational_data_incremental`
- `electricity_power_operational_data_backfill`

Active serving DAGs:

- `platinum_grid_operations_hourly`
- `platinum_resource_planning_daily`

Dormant-on-disk safety-net DAGs:

- Bronze verification DAGs
- Bronze repair DAGs

`airflow/scripts/bootstrap.sh` pauses DAGs during startup, warms Spark package dependencies, then unpauses dataset DAGs first and serving DAGs later.

### Business App

The business app in `app/` is a Streamlit consumer over Postgres query helpers, not an internal API layer.

Implemented business pages:

- `app/pages/grid_operations_manager.py`
- `app/pages/resource_planning_lead.py`

`app/pages/utility_strategy_director.py` exists but is empty.

### Admin App

The admin app in `admin_app/` is a read-only operational support tool. It can inspect:

- Airflow DAG health
- parquet layer parity
- warehouse state
- log files
- API-to-storage comparisons

## Operational Dependencies

The pipeline has real dependency order:

- Airflow depends on Postgres, Kafka, and MinIO
- Spark depends on Kafka and MinIO
- business app depends on Postgres
- admin app depends on Postgres, MinIO, and Airflow

## Notes On Current Scope

- The system is clearly medallion-shaped in implementation.
- Streamlit, not a REST API service, is the business-facing interface.
- Bronze uses explicit offset tracking in MinIO rather than Kafka consumer groups.
- Monthly power operations is a real pipeline branch.
- The admin app looks like it was added to support parity checks and operational trust, which fits the rest of the codebase well.
