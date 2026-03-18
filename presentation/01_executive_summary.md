# Executive Summary

## What The System Does

This platform ingests electricity data from the external EIA v2 API, stages it through Kafka and Spark-managed storage layers, and serves decision-oriented outputs through Postgres-backed Streamlit dashboards.

The main business outputs actually surfaced in the business app are:

- hourly grid operations views in `platinum.grid_operations_hourly` and `platinum.grid_operations_alert_hourly`
- daily resource planning views in `platinum.resource_planning_daily`

There is also a monthly power-operations branch built from `electricity_power_operational_data` into `platinum.electric_power_operations_monthly`. That branch is active in code and schema but is not currently surfaced in the business Streamlit app.

## Why It Exists

The system exists to turn raw EIA electricity feeds into operationally useful views.

- Near-real-time hourly data matters because grid operators need to see demand, forecast miss, ramp stress, generation coverage, and alert conditions quickly.
- Historical and rolling-window data matters because planners need daily comparisons, carbon intensity, renewable share, fuel diversity, and gas dependence trends.

The app code reflects those two decision modes:

- `app/pages/grid_operations_manager.py` focuses on alert triage and current operational stress.
- `app/pages/resource_planning_lead.py` focuses on structural planning signals over time.

## Business Value

The platform’s value is not just collecting data. It creates a controlled path from raw external rows to stable, explainable business metrics.

- It preserves a replay-safe ingestion boundary through Kafka and deterministic `event_id` values.
- It applies dataset-specific cleaning and validation before downstream use.
- It separates raw persistence, cleaned data, curated facts, and serving tables.
- It gives teams stable Postgres tables instead of making dashboards read raw event data directly.

That design reduces the risk of mixing fetch logic, cleaning logic, and dashboard logic in one place.

## Why The Technology Stack Matters

### Kafka

Kafka is used as a dataset-specific buffering boundary, not as the final system of record. Ingestion publishes to topics from `ingestion/src/dataset_registry.yml`, and Bronze reads bounded Kafka batches in `spark/jobs/bronze_kafka_to_minio.py`.

Why it matters:

- it decouples API fetch from Spark writes
- it supports controlled replay into Bronze
- it gives the pipeline a durable handoff point before storage-layer transformations

### Airflow

Airflow dynamically builds dataset and serving DAGs from shared helpers in `airflow/dags/pipeline_dataset_dags.py`, `pipeline_serving_dags.py`, and `pipeline_factories.py`.

Why it matters:

- it orchestrates ingestion, Bronze, Silver, Gold, and Platinum steps
- it manages retries, sensors, backfill queues, and validation tasks
- it controls task ordering and operational recovery

### Spark

Spark jobs own Bronze-to-Silver cleanup, Gold curation, and Platinum metric derivation in `spark/jobs/*`.

Why it matters:

- it can process partitioned datasets incrementally
- it keeps transformation logic separate from orchestration logic
- it supports multi-stage data modeling instead of pushing all logic into a single dashboard query layer

### Postgres

The serving layer lives in Postgres schemas `ops` and `platinum`, with DDL in `warehouse/migrations/*`.

Why it matters:

- it provides stable serving tables for dashboards
- it stores metadata for backfill tracking and validation history
- it supports controlled stage-table merges instead of direct overwrite of final marts

### Streamlit

The business-facing app in `app/` is a Streamlit UI over Postgres query helpers, not an internal REST API.

Why it matters:

- it exposes the data in a form business users can consume quickly
- it keeps presentation logic separate from ingestion and transformation logic

## Key Message For A Presentation

This is an end-to-end data platform that converts external electricity feeds into reliable, business-facing operational and planning views. Its value comes from the full pipeline: controlled ingestion, explicit cleaning, curated modeling, orchestrated recovery, and stable serving outputs.
