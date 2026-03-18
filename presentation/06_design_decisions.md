# Design Decisions

This document explains why the current architecture makes sense for this codebase and what alternatives were likely considered.

## Kafka

### Chosen Option

Use Kafka as a dataset-specific handoff boundary between EIA ingestion and Bronze persistence.

### Code Evidence

- topic-per-dataset registry entries in `ingestion/src/dataset_registry.yml`
- producer and topic management in `ingestion/src/publish_kafka.py`
- Bronze Kafka batch reader in `spark/jobs/bronze_kafka_to_minio.py`

### Why This Choice Fits

The pipeline clearly wants ingestion and storage processing to be decoupled. Kafka helps because ingestion can finish once events are durably published, while Bronze reads in controlled batches later.

### Plausible Alternatives

- direct batch ingestion from EIA straight into MinIO parquet
- direct batch ingestion into Postgres
- queue-free fetch and transform in one task

### Why Those Alternatives Are Weaker Here

- direct fetch-to-storage would tightly couple API calls and storage writes
- direct fetch-to-Postgres would skip the raw replay boundary
- one-step batch tasks would make retries and raw replay more brittle

### Tradeoffs Accepted

- more moving parts than a simple batch script
- current Kafka setup is single-partition and single-replica

## Airflow

### Chosen Option

Use Airflow as the orchestration layer for incrementals, backfills, stage merges, sensors, and validation.

### Code Evidence

- dynamic DAG registration in `airflow/dags/pipeline_factories.py`
- dataset DAG builders in `pipeline_dataset_dags.py`
- serving DAG builders in `pipeline_serving_dags.py`
- backfill queue management in `pipeline_backfill.py`

### Why This Choice Fits

The system has orchestration needs beyond a simple timer:

- registry-driven DAG creation
- separate incremental and backfill modes
- queued historical chunk processing
- stage-table merge control
- validation gates
- first-backfill sensors before serving DAGs

### Plausible Alternatives

- cron
- a lightweight Python scheduler
- a simpler DAG engine with less metadata

### Why Those Alternatives Are Weaker Here

- cron does not naturally model task dependencies, sensors, and backfill state
- simpler schedulers would push more orchestration logic into custom scripts

### Tradeoffs Accepted

- more orchestration code and framework overhead

## Spark

### Chosen Option

Use Spark for Bronze-to-Silver cleanup, Gold curation, and Platinum derivations over partitioned parquet datasets.

### Code Evidence

- transform entrypoints in `spark/jobs/*`
- shared parquet merge helpers in `spark/common/io.py`
- quality assertions in `spark/common/quality.py`

### Why This Choice Fits

The platform is doing more than a few SQL summaries:

- reading Kafka
- writing partitioned parquet across multiple layers
- reconciling revised records by business key
- computing window functions and z-scores
- supporting both hourly and monthly branches

### Plausible Alternatives

- pandas-only scripts
- SQL-only transformations inside Postgres
- dbt over warehouse tables alone

### Why Those Alternatives Are Weaker Here

- pandas would not match the current partitioned object-storage architecture well
- SQL-only would make raw object storage and Kafka integration less natural

### Tradeoffs Accepted

- more infrastructure and dependency complexity

## Postgres Serving Layer

### Chosen Option

Use Postgres for stable serving tables and pipeline metadata, not for the full raw and curated history.

### Code Evidence

- warehouse schemas in `warehouse/migrations/*`
- stage merges in `airflow/dags/pipeline_validation.py`
- query helpers in `app/data_access_*.py`

### Why This Choice Fits

The business app needs stable row-level serving tables and simple SQL query access. The pipeline also needs metadata tables for backfill and verification state.

### Plausible Alternatives

- serve directly from parquet files
- use MinIO plus DuckDB only
- use one warehouse for every stage

### Why Those Alternatives Are Weaker Here

- dashboards should not query raw parquet directly if stable business tables are the goal
- using one database for all stages would collapse useful layer separation

## Streamlit App Boundary

### Chosen Option

Use Streamlit as the business-facing consumption layer over Postgres.

### Code Evidence

- `app/streamlit_app.py`
- `app/pages/grid_operations_manager.py`
- `app/pages/resource_planning_lead.py`
- `app/data_access_shared.py`

### Why This Choice Fits

The codebase is optimized for presentation of curated outputs, not for exposing a programmable service interface.

### Plausible Alternatives

- internal REST API plus a separate frontend
- direct SQL notebooks
- BI tool only

### Why Those Alternatives Are Weaker Here

- a REST API would add a service layer that this repo does not currently need
- notebooks are weaker as a stable business-facing interface

## External EIA API As The Source

### Chosen Option

Use the public EIA v2 API as the system of ingestion.

### Code Evidence

- `ingestion/src/eia_api.py`
- dataset routes in `ingestion/src/dataset_registry.yml`

### Why This Choice Fits

The platform is built around public electricity datasets and around proving end-to-end data engineering capability over a real external source.

## Why The Overall Architecture Makes Sense

The architecture makes sense because it separates concerns cleanly:

- ingestion is about acquiring source rows safely
- Kafka is the handoff boundary
- Bronze preserves raw replay-safe events
- Silver enforces data quality
- Gold resolves revisions into canonical facts and dimensions
- Platinum computes business-facing metrics
- Postgres serves stable tables
- Streamlit exposes those tables to users
