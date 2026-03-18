# Presentation Talking Points

## Opening Business Context

This system exists to turn public electricity data into usable operational and planning views. The raw EIA feeds are valuable, but by themselves they are not a decision-support product. The platform adds the missing layers: ingestion control, cleaning, curation, business metric derivation, and stable serving outputs.

The two main business outcomes actually surfaced in the product are clear in the code. One is hourly grid operations monitoring. The other is daily resource planning analysis. There is also a monthly power-operations branch in the pipeline, but that branch is not currently exposed in the business app, so it should be presented as a supporting analytical path rather than the primary user experience.

## Architecture Story

The architecture is deliberately layered. We fetch from the external EIA v2 API, publish dataset-specific events into Kafka, land raw replay-safe events in Bronze storage on MinIO, clean them into Silver, curate them into Gold facts and dimensions, and then build Platinum serving tables in Postgres for the dashboards.

Airflow is the conductor. It decides the time windows, launches ingestion and Spark jobs, manages backfills, waits for dependencies, merges stage tables into final tables, and runs validation checks. Spark owns the transformations. Postgres owns the stable serving layer. Streamlit is the business-facing consumption layer.

## Data Flow Story

The data flow starts in a shared dataset registry. That registry tells both ingestion and Airflow which source route, topic, frequency, and storage paths belong to each dataset. Ingestion fetches rows from EIA, turns each row into a deterministic event with a stable `event_id`, and publishes those events to Kafka.

Bronze then reads Kafka in bounded batches, not with a long-running consumer group. It stores its own next-offset checkpoint in MinIO and suppresses duplicates by `event_id`. Silver applies dataset-specific cleaning rules. Gold resolves revised records into canonical facts and dimensions. Platinum derives the business-facing metrics and writes stage tables, and Airflow merges those into final Postgres tables that the app can query cleanly.

## Data Quality Story

The platform does not rely on one generic cleaning function. It has dataset-specific rules. Region data validates allowed type values like actual versus forecast. Fuel data validates fuel-type records and normalized units. Monthly power data validates multiple unit systems before any derived metrics are computed.

There is also a difference between hard validation and business derivation. Hard validation rejects nulls, duplicates, conflicting records, unsupported units, and invalid numeric bounds. Business derivation is the step where the platform calculates things like forecast error, z-scores, carbon intensity, fuel diversity, and heat rate.

## Reliability Story

The reliability story is stronger than just Airflow retries. Kafka publishing is configured with acknowledgements, retries, and idempotence. Event identity is deterministic. Bronze stores explicit offset checkpoints and suppresses replay duplicates. Airflow serializes Bronze writes and all backfills to avoid conflicting mutation paths. Backfill progress is tracked in Postgres, stale jobs can be recovered, and serving DAGs wait until enough historical data exists before running.

There is also a dormant Bronze verification and repair framework in the codebase. It exists and is implemented, but it is not part of the currently auto-unpaused active orchestration path.

## Tradeoffs Story

This is not the simplest architecture the team could have built. A simpler version could have skipped Kafka, written straight from the API into tables, and done a smaller amount of transformation in SQL or pandas. But that would have made replay, orchestration, and separation of concerns weaker.

The tradeoff the code makes is to accept more moving parts in exchange for a cleaner pipeline boundary between ingestion, transformation, and serving. It also accepts some limits: there is no internal REST API, Kafka is single-partition in the current environment, and there is no explicit dead-letter queue or paging integration.

## Closing Message

The strongest way to present this system is not as a lot of tools connected together. It is better described as a controlled data platform that takes external electricity data and turns it into reliable operational and planning products. Kafka, Airflow, Spark, Postgres, and Streamlit are not there as buzzwords. In this repo, each one has a specific role in making the outputs replayable, explainable, and usable.
