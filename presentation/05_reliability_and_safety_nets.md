# Reliability And Safety Nets

This document separates implemented protections from protections that were not found in the current codebase.

## Implemented Protections

## 1. Deterministic Event Identity

Implementation:

- `ingestion/src/event_factory.py`

`event_id` is a deterministic hash of dataset id, route, and raw row payload.

Why it matters:

- supports replay-safe deduplication
- lets Bronze suppress exact duplicates
- preserves revised rows when the payload changes

## 2. Kafka Producer Safety Settings

Implementation:

- `ingestion/src/publish_kafka.py`

The Kafka producer uses:

- `acks="all"`
- `retries=5`
- `enable_idempotence=True`
- `max_in_flight_requests_per_connection=1`

## 3. Topic Creation On Demand

Implementation:

- `ensure_topic_exists` in `ingestion/src/publish_kafka.py`

The ingestion boundary creates missing topics automatically before sending.

## 4. Explicit Bronze Offset Checkpointing

Implementation:

- `spark/jobs/bronze_kafka_to_minio.py`

Bronze stores the next Kafka offsets in MinIO under `offsets.json` per dataset checkpoint path.

## 5. Replay-Safe Bronze Deduplication

Implementation:

- `prepare_bronze_write_plan` in `spark/jobs/bronze_kafka_to_minio.py`

Bronze suppresses duplicates:

- already duplicated within the current batch
- already present in touched Bronze partitions

## 6. Bounded Bronze Reads

Implementation:

- `spark/jobs/bronze_kafka_to_minio.py`

Bronze reads from `startingOffsets` to `latest` in one bounded batch and returns.

## 7. `failOnDataLoss=false`

Implementation:

- Kafka read options in `spark/jobs/bronze_kafka_to_minio.py`

Tradeoff:

- it can prefer progress over strict failure on missing Kafka data

## 8. Dataset-Specific Bronze Write Serialization

Implementation:

- `airflow/dags/pipeline_builders.py`
- `airflow/scripts/bootstrap.sh`

Airflow creates one Bronze write pool per dataset and sets each pool to one slot.

## 9. Global Backfill Serialization

Implementation:

- `global_backfill_pool` in `airflow/dags/pipeline_builders.py`
- pool creation in `airflow/scripts/bootstrap.sh`

Backfills are globally serialized through a one-slot pool.

## 10. Airflow Retries

Implementation:

- default args in `airflow/dags/pipeline_dataset_dags.py`
- default args in `airflow/dags/pipeline_serving_dags.py`

Right now:

- dataset incrementals retry twice with 5-minute delay
- backfills retry once with 5-minute delay
- serving DAGs retry once with 5-minute delay

## 11. Backfill Queue State In Postgres

Implementation:

- `airflow/dags/pipeline_backfill.py`
- `warehouse/migrations/004_pipeline_metadata.sql`

Backfill work is tracked in `ops.backfill_jobs` with `pending`, `in_progress`, `completed`, and `failed` states.

## 12. Stale Backfill Recovery

Implementation:

- `recover_stale_backfill_jobs` in `airflow/dags/pipeline_backfill.py`

In-progress jobs older than the configured stale threshold are marked failed and become retry-eligible up to a capped attempt count.

## 13. Stage-Then-Merge Serving Updates

Implementation:

- Spark stage jobs in `spark/jobs/platinum_*.py`
- merge helper in `airflow/dags/pipeline_validation.py`

Spark writes stage tables, then Airflow merges into final targets with `on conflict do update`.

## 14. Post-Stage And Post-Merge Validation

Implementation:

- `airflow/dags/pipeline_validation.py`
- task construction in `pipeline_dataset_dags.py` and `pipeline_serving_dags.py`

Airflow validates:

- stage row presence
- final row presence
- distinct counts
- numeric bounds

## 15. First-Backfill Sensors Before Serving DAGs

Implementation:

- `build_first_backfill_sensor` in `airflow/dags/pipeline_builders.py`
- usage in `pipeline_serving_dags.py`

Grid operations and resource planning DAGs wait until at least one completed backfill chunk exists for both region and fuel datasets.

## 16. Optional Skip Behavior When Inputs Are Missing

Implementation:

- multiple Spark jobs check `path_exists` or `has_partitioned_parquet_input`

Several jobs exit cleanly when required upstream storage is absent instead of hard-failing immediately.

## 17. Dormant Bronze Verification And Repair Framework

Implementation:

- DAG builders in `airflow/dags/pipeline_dataset_dags.py`
- coverage job in `spark/jobs/bronze_hourly_coverage_verify.py`
- metadata tables in `warehouse/migrations/005_bronze_hourly_coverage.sql` and `006_bronze_repair_jobs.sql`

The codebase contains a full framework for hourly Bronze coverage checks and repair-job queueing, but startup auto-unpause only targets incremental, backfill, and serving DAGs.

## What Happens When Things Fail

### Publish failures

- Kafka send exceptions propagate from `publish_events`
- ingestion fails the run instead of silently succeeding

### Validation failures

- Spark assertions raise `ValueError`
- Airflow validation tasks raise on bad row counts, distinct counts, or numeric bounds

### Backfill task failures

- failed chunks are marked `failed` in `ops.backfill_jobs`
- stale in-progress chunks can later be recovered into retryable state

### Missing upstream storage

- some Spark jobs skip when expected inputs are absent

## Protections Not Found Imlemented

- no dead-letter queue or dead-letter Kafka topic
- no explicit Slack, email, PagerDuty, or webhook alerting hooks
- no cross-system exactly-once guarantee spanning Kafka, MinIO, and Postgres
- no multi-broker Kafka replication in the compose setup
- no explicit backpressure control beyond bounded batch sizing and serialized pools

## Additional Constraints Worth Mentioning

Topics are single-partition and single-replica in the current compose setup.

## Summary

The system is not bulletproof, but it is intentionally guarded through deterministic identity, replay-safe Bronze design, serialized mutation points, explicit backfill state, validation after writes, and delayed serving startup.
