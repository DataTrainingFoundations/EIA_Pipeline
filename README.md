# EIA Pipeline

This branch targets a Snowflake-first ELT architecture for EIA electricity data.

Target flow:

`EIA API -> Snowflake RAW -> Snowflake SILVER -> Snowflake GOLD -> dbt tests -> Streamlit`

Runtime layout:

- `pipeline/core`: shared settings, registry, windowing, and Snowflake helpers
- `pipeline/config`: dataset metadata and other runtime configuration assets
- `pipeline/ingestion`: EIA API client and RAW ingest orchestration
- `pipeline/silver`: RAW to SILVER transforms
- `pipeline/gold`: SILVER to GOLD transforms and the gold CLI entrypoint
- `pipeline/orchestration`: separate ingest and transform planning/runtime helpers

Airflow runtime:

- `eia_hourly_ingest` for hourly RAW ingestion
- `eia_hourly_transform` for hourly RAW-driven SILVER/GOLD publishing
- `eia_monthly_ingest` for monthly RAW ingestion
- `eia_monthly_transform` for monthly RAW-driven SILVER/GOLD publishing

Gold shape:

- `FACT_GENERATION_HOURLY`
- `FACT_DEMAND_HOURLY`
- `DIM_BALANCING_AUTHORITY`
- `DIM_FUEL_TYPE`
- `AGG_DAILY_GENERATION`
- `AGG_DAILY_DEMAND_PEAK`
- `GOLD_ELECTRICITY_OPERATIONAL_SALES`

The gold layer is intentionally a pragmatic star-schema-style serving model for the Streamlit app.

## Setup

Create an env file:

```bash
cp .env.example .env
```

Create the Snowflake database and schemas:

```sql
CREATE DATABASE IF NOT EXISTS EIA_PIPELINE;
CREATE SCHEMA IF NOT EXISTS EIA_PIPELINE.RAW;
CREATE SCHEMA IF NOT EXISTS EIA_PIPELINE.SILVER;
CREATE SCHEMA IF NOT EXISTS EIA_PIPELINE.GOLD;
CREATE SCHEMA IF NOT EXISTS EIA_PIPELINE.META;
```

Fill in the Snowflake variables in `.env`.
The local Airflow metadata database uses the `POSTGRES_*` values from `.env`.

## Start

```bash
docker compose up -d --build
```

## UIs

Airflow: `http://localhost:28080`  
Airflow login: `admin` / `admin`

If the Airflow login is not initialized correctly:

```bash
docker compose exec airflow airflow users reset-password --username admin --password admin
```

Streamlit: `http://localhost:28501`

## Notes

- The active local runtime is `airflow`, `airflow-db`, `app`, and the optional standalone `ingestion` service backed by `pipeline/ingestion`.
- All substantive pipeline logic now lives under `pipeline/`.
- Snowflake is the source of truth for RAW, SILVER, and GOLD data.
- dbt remains the planned testing layer on top of Snowflake.
- Airflow uses a Postgres metadata database locally so `LocalExecutor` can run multiple tasks without the SQLite bottleneck.
- Ingest and transform are scheduled separately for both hourly and monthly cadence groups.
- Only ingest talks to the EIA API; transform scans RAW and state to determine what still needs publishing.
- The pipeline automatically bootstraps configured historical data with larger bootstrap batch sizes than steady-state repair runs.
- Bootstrap runs can self-trigger follow-up ingest and transform runs until backlog is drained, then fall back to normal schedules.
- On a fresh local startup, Airflow auto-triggers the first hourly and monthly ingest runs when no prior ingest runs exist.
- SILVER and GOLD partition data by business date derived from the EIA `PERIOD` field rather than ingest date.
- Pipeline cadence state is stored in `EIA_PIPELINE.META.PIPELINE_RUN_STATE` with separate ingest and transform progress fields.
