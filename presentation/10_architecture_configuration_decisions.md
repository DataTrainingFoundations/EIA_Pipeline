# Architecture Decisions Through Configuration

This document is a configuration-focused companion to `presentation/06_design_decisions.md`.

The goal here is not only to say what components exist, but to explain which configuration choices define the behavior of the platform and why those choices matter.

## Core Configuration Philosophy

The codebase is opinionated about where configuration should live.

Current pattern:

- deployment and runtime configuration is mostly driven by environment variables
- dataset-specific behavior is driven by the versioned YAML registry in `ingestion/src/dataset_registry.yml`
- orchestration defaults are expressed in code with environment-variable overrides
- there is no current use of Airflow Variables in the DAG codebase

Why this matters:

- config stays versioned with the repo instead of being hidden in the Airflow UI
- compose startup is more reproducible because the same `.env` file drives multiple services
- DAG behavior is easier to review because schedules, pools, and paths are visible in code

## 1. Kafka Boundary And Producer Safety

### Decision

Kafka is the explicit handoff boundary between ingestion and Bronze, and the producer is configured for durability-first behavior rather than maximum throughput.

### Where The Config Lives

- `ingestion/src/publish_kafka.py`
- `docker-compose.yml`
- `.env.example`
- `airflow/dags/pipeline_builders.py`
- `spark/common/config.py`
- `spark/jobs/bronze_kafka_to_minio.py`

### Key Config Choices

- producer bootstrap server comes from `KAFKA_BROKER`, default `kafka:9092`
- security protocol comes from `KAFKA_SECURITY_PROTOCOL`, default `PLAINTEXT`
- producer uses `acks="all"`
- producer uses `retries=5`
- producer uses `enable_idempotence=True`
- producer uses `max_in_flight_requests_per_connection=1`
- topics are created on demand with `num_partitions=1`
- topics are created on demand with `replication_factor=1`

### Why We Chose This

The combination of `acks="all"`, retries, idempotence, and one in-flight request is a strong signal that we optimized this local platform for correctness and replay safety first.

That configuration reduces the chance of duplicate or reordered writes during transient failures. It fits a pipeline where Bronze deduplication and replay safety matter more than raw Kafka producer throughput.

### Tradeoffs Accepted

- single-partition topics limit parallelism
- replication factor `1` means the local stack is not fault tolerant at the broker level
- `PLAINTEXT` is fine for local compose, but not the final production security posture

### Bronze Consumer Side Decision

The Bronze step is also explicitly configured for replay-safe batch reads:

- Airflow exports `KAFKA_STARTING_OFFSETS=earliest` before running the Bronze job
- Spark reads Kafka with `endingOffsets=latest`
- Spark writes explicit next offsets into `offsets.json` under the Bronze checkpoint path
- Kafka read uses `failOnDataLoss=false`

Why this matters:

- the pipeline does not rely on Kafka consumer groups for Bronze progress tracking
- offset state is owned by the pipeline and stored next to Bronze checkpoint state
- bounded batch reads are easier to reason about than an always-on streaming consumer in this project

## 2. Airflow Configuration Strategy

### Decision

Airflow is configured through environment variables and code constants, not through Airflow Variables.

### Where The Config Lives

- `docker-compose.yml`
- `.env.example`
- `airflow/dags/pipeline_constants.py`
- `airflow/dags/pipeline_runtime.py`
- `airflow/dags/pipeline_backfill.py`
- `airflow/dags/pipeline_repair.py`
- `airflow/scripts/bootstrap.sh`

### What We Are Using

- `AIRFLOW__CORE__EXECUTOR=LocalExecutor`
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` for metadata DB connection
- `AIRFLOW__CORE__FERNET_KEY`
- `AIRFLOW__CORE__LOAD_EXAMPLES=False`
- `AIRFLOW_BACKFILL_SCHEDULE`
- `AIRFLOW_PLATINUM_GRID_OPERATIONS_SCHEDULE`
- `AIRFLOW_PLATINUM_RESOURCE_PLANNING_SCHEDULE`
- `AIRFLOW_BACKFILL_MAX_ATTEMPTS`
- `AIRFLOW_BACKFILL_STALE_MINUTES`
- `AIRFLOW_BRONZE_REPAIR_MAX_ATTEMPTS`
- `AIRFLOW_BRONZE_REPAIR_STALE_MINUTES`
- `AIRFLOW_BRONZE_REPAIR_MAX_PENDING`
- `AIRFLOW_GLOBAL_BACKFILL_POOL`
- `AIRFLOW_DAG_UNPAUSE_DELAY_SECONDS`
- `AIRFLOW_PLATINUM_DAG_UNPAUSE_DELAY_SECONDS`

### What We Are Not Using

No current DAG code was found using:

- `from airflow.models import Variable`
- `Variable.get(...)`

### Why We Chose This

Using environment variables instead of Airflow Variables keeps orchestration config in versioned code and deployment files.

That has several benefits:

- easier local setup through `.env`
- less hidden state in the Airflow UI
- simpler review of schedule and retry changes in Git
- consistent configuration across containers

### Important Airflow Defaults

The repo hard-codes several orchestration choices with env overrides:

- backfill schedule default is `5 * * * *`
- grid operations schedule default is `20 * * * *`
- resource planning schedule default is `15,45 * * * *`
- Bronze verification default is `40 * * * *`
- Bronze repair default is `50 * * * *`

This tells us the architecture expects scheduling policy to be operationally adjustable without changing DAG structure.

## 3. Airflow Pooling, Sensors, And Startup Behavior

### Decision

The platform intentionally serializes risky work in Airflow instead of maximizing parallelism.

### Where The Config Lives

- `airflow/scripts/bootstrap.sh`
- `airflow/dags/pipeline_builders.py`
- `airflow/dags/pipeline_dataset_dags.py`

### Key Config Choices

- one Bronze write pool per dataset
- one global backfill pool named `global_backfill_worker`
- each of those pools is set to `1` slot at bootstrap
- dataset DAGs and platinum DAGs are paused during bootstrap and only unpaused after readiness delays
- `ExternalTaskSensor` uses `deferrable=True`
- sensors use `mode="reschedule"`
- first-backfill sensors wait for at least one successful backfill chunk before allowing serving DAGs to proceed

### Why We Chose This

These settings prioritize correctness and cluster stability:

- dataset Bronze writes are serialized so two overlapping writers do not race the same Kafka replay window
- all backfills are globally serialized to reduce machine load in the local compose stack
- deferrable and reschedule sensor modes reduce wasted worker occupancy
- delayed unpause avoids starting scheduled runs before Kafka, Spark, and MinIO are actually ready

### Tradeoffs Accepted

- lower parallel throughput
- more waiting in exchange for fewer race conditions and less startup churn

## 4. Dataset Registry As A Control Plane

### Decision

Dataset-specific behavior is centralized in `ingestion/src/dataset_registry.yml` instead of being duplicated across DAG code, ingestion code, and Spark code.

### What The Registry Controls

- dataset id
- EIA route
- Kafka topic
- frequency
- incremental schedule
- default facets
- Bronze path
- Bronze checkpoint path
- Silver path
- Gold path
- Platinum table
- backfill start
- backfill step
- backfill max pending chunks

### Why We Chose This

The registry makes the orchestration factory pattern viable. Airflow can build multiple DAG families from one shared code path because the per-dataset decisions are data-driven.

This is one of the most important architectural choices in the repo because it avoids hard-coding dataset behavior into many different modules.

### Concrete Examples

- `electricity_region_data` is hourly, routes to `electricity/rto/region-data`, and backfills in `day` steps
- `electricity_fuel_type_data` is hourly, but its incremental schedule is intentionally delayed to `20 18 * * *`
- `electricity_power_operational_data` is monthly and backfills in `month` steps starting in `2010`

## 5. Spark Runtime And Submission Config

### Decision

Spark is configured as a bounded batch compute engine over object storage, not as an unconstrained always-on cluster.

### Where The Config Lives

- `docker-compose.yml`
- `airflow/dags/pipeline_runtime.py`
- `airflow/dags/pipeline_constants.py`
- `spark/common/config.py`
- `spark/common/spark_session.py`
- `airflow/scripts/bootstrap.sh`

### Key Config Choices

- Spark version is `3.5.1`
- the compose stack runs one master and two workers
- Airflow uses `spark-submit` against `spark://spark-master:7077`
- Spark package set is pinned for Kafka and S3A support
- serving jobs add the PostgreSQL JDBC package
- Airflow caps per-job total cores through `SPARK_SUBMIT_TOTAL_CORES`, default `4`
- Airflow caps executor cores through `SPARK_SUBMIT_EXECUTOR_CORES`, default `2`
- Spark session timezone is fixed to `UTC`
- `spark.sql.sources.partitionOverwriteMode=dynamic`
- adaptive query execution is enabled
- `spark.sql.shuffle.partitions` defaults to `8`
- `spark.default.parallelism` defaults to the same value unless overridden
- S3A path-style access defaults to `true`
- S3A SSL is disabled in the local stack
- `spark.sql.files.ignoreMissingFiles=true`

### Why We Chose This

These settings match the actual platform shape:

- the compose cluster is small, so per-job cores are capped to prevent one run from taking the whole cluster
- UTC avoids ambiguous time handling across hourly and daily jobs
- dynamic partition overwrite fits medallion datasets that update only touched partitions
- adaptive execution helps small local clusters behave better under uneven data sizes
- S3A path-style access and disabled SSL are practical choices for MinIO in local compose

## 6. Object Storage And Warehouse Separation

### Decision

MinIO and Postgres are configured for different responsibilities, not as interchangeable storage layers.

### Where The Config Lives

- `docker-compose.yml`
- `spark/common/config.py`
- `airflow/dags/pipeline_constants.py`
- `warehouse/migrations/*`

### Key Config Choices

- MinIO startup creates three buckets: `bronze`, `silver`, and `gold`
- Spark defaults to `s3a://bronze/...`, `s3a://silver/...`, and `s3a://gold/...` style paths
- Postgres is used for `ops` metadata and `platinum` serving tables
- serving jobs write stage tables first and Airflow merges into final tables

### Why We Chose This

This separation keeps medallion storage optimized for file-based batch processing while keeping business consumption and queue metadata in a relational store that the apps can query easily.

That is a more disciplined design than trying to serve dashboards directly from raw parquet or forcing every stage into Postgres.

## 7. Time Window And Schedule Decisions

### Decision

The pipeline standardizes on `[start, end)` window semantics and then adapts to source-system quirks internally.

### Where The Config Lives

- `ingestion/src/fetch_eia.py`
- `ingestion/src/eia_api.py`
- `airflow/dags/pipeline_dataset_dags.py`
- `ingestion/src/dataset_registry.yml`

### Key Config Choices

- ingestion CLI accepts inclusive `start` and exclusive `end`
- default CLI window is the last `24` UTC hours
- Airflow templates pass window boundaries in UTC
- hourly datasets use hourly CLI formatting
- non-hourly datasets use ISO timestamps
- backfill chunking depends on dataset registry step values such as `day` or `month`

### Why We Chose This

The `[start, end)` model makes backfills, incrementals, and validation windows line up more cleanly and avoids repeated boundary-hour bugs.

The code then translates that model to the external EIA API behavior where needed, rather than leaking source-specific quirks into every orchestration layer.

## 8. Local Compose Topology As A Deliberate Choice

### Decision

The local platform is intentionally small, explicit, and demo-friendly rather than pretending to be a production-scale cluster.

### Key Config Choices

- single Postgres instance
- single Kafka broker in KRaft mode
- one MinIO instance
- one Spark master and two workers
- one Airflow service using `LocalExecutor`
- one business Streamlit app
- one admin Streamlit app

### Why We Chose This

The goal of this repo is to demonstrate the full data platform shape in a reproducible local environment. The compose file favors clarity and end-to-end completeness over production-grade redundancy.

That is why some settings are intentionally simple:

- Kafka replication factor `1`
- no TLS on local internal links
- single-node metadata services
- explicit health checks and startup ordering instead of a more complex deployment platform

## 9. Most Important Conclusions

The biggest architectural decisions are not only the choice of Kafka, Airflow, Spark, MinIO, and Postgres. They are the configuration patterns that tie those tools together.

The most important of those patterns are:

- Kafka is configured for durable, replay-safe publishing
- Bronze progress is tracked by explicit offset files, not Kafka consumer group state
- Airflow uses environment variables and versioned code, not Airflow Variables
- dataset-specific behavior is centralized in the YAML registry
- concurrency is intentionally constrained through pools and core limits
- time handling is standardized around UTC and `[start, end)` windows
- object storage and serving storage are separated on purpose

That configuration style is what makes the architecture coherent.
