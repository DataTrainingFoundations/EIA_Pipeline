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
- `pipeline/orchestration`: automatic bronze-to-gold partition planning and cadence execution

Airflow runtime:

- `eia_hourly_bronze_to_gold` for fully automatic hourly processing
- `eia_monthly_bronze_to_gold` for fully automatic monthly processing

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

- The active local runtime is `airflow`, `app`, and the optional standalone `ingestion` service backed by `pipeline/ingestion`.
- All substantive pipeline logic now lives under `pipeline/`.
- Snowflake is the source of truth for RAW, SILVER, and GOLD data.
- dbt remains the planned testing layer on top of Snowflake.
- Bronze-to-gold scheduling is automatic for both hourly and monthly cadence groups.
- The pipeline automatically bootstraps configured historical data and then keeps recent partitions repaired without manual backfills.
- SILVER and GOLD partition data by business date derived from the EIA `PERIOD` field rather than ingest date.
- Pipeline cadence state is stored in `EIA_PIPELINE.META.PIPELINE_RUN_STATE`.
