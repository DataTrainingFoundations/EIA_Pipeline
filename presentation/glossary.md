# Glossary

## Bronze

The raw durable storage layer. In this repo it is parquet written from Kafka events into MinIO, partitioned by dataset and event time.

## Silver

The cleaned storage layer. This is where dataset-specific validation, normalization, and event-level cleanup happen.

## Gold

The curated storage layer. This is where revised rows are resolved into canonical facts and reusable dimensions.

## Platinum

The serving layer for business-facing metrics. In this repo, Platinum outputs are written into Postgres tables used by the dashboards.

## Respondent

A source-system balancing-authority or reporting entity identifier used heavily in region and fuel datasets.

## Backfill

Historical pipeline processing for past windows, managed through Airflow and tracked in `ops.backfill_jobs`.

## Backfill Chunk

A queued historical window that Airflow claims and processes as one backfill unit.

## Coverage Ratio

In the grid operations mart, the ratio of total generation observed in the fuel data to actual demand observed in the region data.

## Forecast Error

The difference between actual demand and day-ahead forecast demand.

## Fuel Diversity Index

A planning metric derived as `1 - sum(fuel_share^2)` for a respondent-day. Higher values imply a more diversified mix.

## Heat Rate

A monthly power-operations efficiency-style metric derived from heat input and generation output.

## Stage Table

A temporary or run-scoped Postgres table written by Spark before Airflow merges it into a stable target table.

## Deterministic `event_id`

A stable hash-based identifier built from dataset id, route, and raw source row payload. It is the key to replay-safe deduplication.
