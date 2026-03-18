# Possible Questions And Answers

## Why did you choose Kafka here?

In this codebase, Kafka is the ingestion handoff boundary between the EIA fetch step and the Bronze storage step. That is visible in `ingestion/src/publish_kafka.py` and `spark/jobs/bronze_kafka_to_minio.py`. Ingestion publishes deterministic event envelopes into dataset-specific topics, and Bronze later reads bounded batches from Kafka and writes raw parquet to MinIO. This gives the pipeline a durable replay point and decouples external API fetching from Spark storage writes.

## Why use Airflow instead of cron?

The orchestration needs are more complex than run a script every hour. Airflow is coordinating incremental and backfill DAGs, managing a backfill queue in Postgres, waiting on first-backfill sensors before serving DAGs, launching Spark jobs, and running post-write validation and merge tasks. That logic is spread across `airflow/dags/pipeline_dataset_dags.py`, `pipeline_serving_dags.py`, `pipeline_backfill.py`, and `pipeline_validation.py`.

## Why use Spark instead of pandas or pure SQL?

Spark is doing more than simple row filtering. It reads Kafka for Bronze, manages partitioned parquet datasets in MinIO, resolves revised business keys in Gold, and computes window-function metrics like z-scores in Platinum. Those behaviors are implemented in `spark/jobs/*` and depend on shared parquet merge helpers in `spark/common/io.py`.

## How are duplicates handled?

Duplicates are handled at multiple layers. In Bronze, `spark/jobs/bronze_kafka_to_minio.py` removes duplicates within the current batch and also suppresses `event_id` values already present in touched Bronze partitions. In Silver, `_latest_by_event` keeps the newest cleaned record per `event_id`, and `assert_no_conflicting_records` rejects cases where the same `event_id` maps to conflicting cleaned values. In Gold, revised records are resolved by business key with the latest `loaded_at` winning.

## What happens when data is malformed?

During ingestion, invalid period values cause `build_event` in `ingestion/src/event_factory.py` to raise an error. In Bronze, unparseable JSON messages are dropped by filtering out null parsed events, and rows missing required identifiers are filtered. In Silver and later Spark jobs, validation helpers in `spark/common/quality.py` raise exceptions when required columns, allowed units, uniqueness, or bounds checks fail.

## How do backfills work?

Backfills are queue-driven. `airflow/dags/pipeline_backfill.py` writes candidate windows into `ops.backfill_jobs`, claims the next newest pending chunk, and marks chunks completed or failed. For daily and hourly-style datasets the code chunks history newest-first in calendar-week-sized windows. For monthly data it chunks month by month.

## How do you recover from failures?

There are several mechanisms. Airflow tasks have retries. Backfill failures are recorded in `ops.backfill_jobs` and can be retried up to a capped attempt count. Stale in-progress backfill jobs are automatically marked failed and made eligible for retry by `recover_stale_backfill_jobs`. Bronze itself is replay-safe because offsets are checkpointed externally and writes are deduplicated by `event_id`.

## How is data quality checked?

The code checks quality at both transformation time and orchestration time. Spark assertions enforce non-null required fields, uniqueness, allowed values, non-negative measures, and numeric bounds in `spark/common/quality.py`. Silver also enforces a retention-ratio rule so a window cannot silently lose too many records during cleaning. Airflow then runs row-count, distinct-count, and numeric-bounds checks after stage writes and merges through helpers in `airflow/dags/pipeline_validation.py`.

## What is in Postgres versus MinIO?

MinIO stores the medallion datasets: Bronze raw events, Silver cleaned parquet, and Gold curated facts and dimensions. Postgres stores the `ops` metadata tables and the `platinum` serving tables. The business app reads Postgres through query helpers in `app/data_access_*.py`; it does not query MinIO directly.

## Is there an API in this system?

There is no internal REST API service in this repo. The true API boundary is the external EIA v2 API accessed by `ingestion/src/eia_api.py`. The business-facing interface is a Streamlit app backed by Postgres, and the admin app is also a Streamlit app.

## Why is there a monthly power dataset branch that is not in the main dashboard?

The codebase supports three dataset branches, and the monthly `electricity_power_operational_data` branch is fully implemented through Bronze, Silver, Gold, and Platinum. The table `platinum.electric_power_operations_monthly` exists in schema and has Spark and Airflow jobs behind it. What is missing is a business UI page that consumes it.

## What are the current weak spots?

The main weak spots visible in code are operational rather than conceptual. Kafka topics are single-partition and single-replica in the current environment. There is no explicit dead-letter topic. There are no visible Slack, email, or PagerDuty hooks. There is no exactly-once transaction boundary across Kafka, MinIO, and Postgres. One declared business page, `app/pages/utility_strategy_director.py`, is empty.
