# End-To-End Data Flow

## 1. Dataset Definitions Drive The Pipeline

The pipeline begins with `ingestion/src/dataset_registry.yml`.

That registry defines, per dataset:

- `id`
- EIA `route`
- Kafka `topic`
- source `frequency`
- optional facets and windowing mode
- Bronze, Silver, and Gold storage paths
- optional Platinum target table
- backfill start and chunking behavior

This matters because Airflow and ingestion both reuse the same dataset registry instead of maintaining separate orchestration and fetch definitions.

## 2. Airflow Chooses The Window And Launches Ingestion

Dataset incremental and backfill DAGs are built in `airflow/dags/pipeline_dataset_dags.py`.

For incrementals, Airflow renders time expressions from `data_interval_start` and `data_interval_end` into the ingestion command built by `pipeline_builders.build_fetch_command`.

For backfills, Airflow:

- enqueues candidate windows into `ops.backfill_jobs`
- claims the next newest pending chunk
- passes that chunk’s start and end into ingestion and downstream Spark jobs

Backfill state lives in Postgres, not in Airflow variables or local files.

## 3. Ingestion Fetches Rows From EIA

`ingestion/src/fetch_eia.py` calls `ingestion/src/eia_api.py`.

The fetch flow is:

1. load one or more datasets from the registry
2. translate the pipeline window into EIA API parameters
3. page through the external API
4. collect source rows

### Window semantics

The pipeline uses `[start, end)` semantics internally.

- `resolve_api_window_bounds` in `ingestion/src/eia_api.py` converts that internal window into the EIA API’s inclusive request format
- hourly region data can use `hourly_window_mode: current_day_end`, which extends the API request to the rest of the current UTC day
- monthly power data uses a different translation path than hourly datasets

This is important presentation material because it shows the team already had to handle source-specific quirks.

## 4. Ingestion Builds Deterministic Event Envelopes

`ingestion/src/event_factory.py` wraps each row in an event envelope.

Fields include:

- `event_id`
- `dataset`
- `source`
- `event_timestamp`
- `ingestion_timestamp`
- `metadata`
- `payload`

The `event_id` is a SHA-256 hash over dataset id, route, and raw row payload. That makes it deterministic and stable across re-fetches of the same exact source row.

Why this matters:

- it gives Bronze a replay-safe dedupe key
- it lets the system preserve source revisions when the payload changes

## 5. Ingestion Publishes To Kafka

`ingestion/src/publish_kafka.py` is the publish boundary.

The flow is:

1. ensure the topic exists
2. serialize the event as compact JSON
3. publish it with `event_id` as the Kafka key
4. wait for send acknowledgements

Kafka is used here as the handoff point between API fetch and storage-oriented processing.

## 6. Bronze Reads A Bounded Kafka Batch

`spark/jobs/bronze_kafka_to_minio.py` does not run a long-lived stream. It reads a bounded batch from Kafka:

- `startingOffsets` comes from an explicit checkpoint file in MinIO
- `endingOffsets` is always `latest`
- `failOnDataLoss` is set to `false`

This is a key architectural detail: Bronze is replay-safe, batch-triggered, and offset-controlled by the platform itself.

### Bronze checkpointing

Offsets are read from and written to:

- `s3a://bronze/checkpoints/<dataset>/offsets.json`

This means the next Bronze run starts from the last committed offset file, not from a consumer group position.

## 7. Bronze Parses, Filters, And Writes Raw Event Storage

Bronze transforms Kafka payloads into a parquet schema using `spark/common/schemas.py`.

It then:

- parses JSON into structured columns
- derives `event_ts`, `ingestion_ts`, and partition fields
- filters rows missing required identifiers
- deduplicates within the batch on `event_id`
- reads touched existing Bronze partitions
- suppresses `event_id` values already written previously
- writes only new rows to MinIO Bronze storage

Bronze partitions by:

- dataset
- year
- month
- day
- hour

## 8. Silver Cleans By Dataset

`spark/jobs/silver_clean_transform.py` reads Bronze parquet for the requested window and routes each supported dataset through its own cleaning function:

- `clean_region_data`
- `clean_fuel_type_data`
- `clean_power_operational_data`

The Silver stage performs real normalization and validation:

- type parsing
- text trimming
- unit normalization
- null filtering
- allowed-value enforcement
- event-level dedupe
- retention-ratio checks

Silver writes to day-partitioned parquet datasets in MinIO.

## 9. Gold Builds Canonical Facts And Dimensions

There are two Gold branches.

### Hourly region and fuel Gold

`spark/jobs/gold_region_fuel_serving_hourly.py` creates:

- region demand/forecast fact
- fuel generation fact
- respondent dimension
- fuel type dimension

It resolves revised source records by keeping the latest loaded record for each business key.

### Monthly power Gold

`spark/jobs/gold_power_operations_monthly.py` creates the monthly electric power operations fact.

This branch uses monthly business keys such as:

- `period`
- `location`
- `sector_id`
- `fueltype_id`

## 10. Platinum Builds Serving Metrics

There are three Platinum outcomes in code.

### Daily region demand mart

`spark/jobs/platinum_region_demand_daily.py` aggregates hourly Gold region demand into daily totals and summary metrics, writes a stage table, and lets Airflow merge into `platinum.region_demand_daily`.

### Grid operations and resource planning marts

`spark/jobs/platinum_grid_operations_hourly.py` and `platinum_resource_planning_daily.py` build richer business-facing metrics such as:

- forecast error and anomaly scores
- demand ramps
- generation coverage ratio
- renewable/fossil/gas share
- carbon intensity
- fuel diversity
- peak-hour gas dependence

These jobs write stage tables over JDBC to Postgres.

### Monthly power operations mart

`spark/jobs/platinum_power_operations_monthly.py` writes a stage table for the monthly power branch, then Airflow merges into `platinum.electric_power_operations_monthly`.

## 11. Airflow Merges And Validates Serving Tables

Airflow does not let Spark overwrite final Platinum tables directly.

Instead it:

1. runs the Spark stage job
2. validates the stage table has rows when expected
3. merges stage into target with `on conflict do update`
4. runs post-merge validation tasks

Those merge and validation helpers live in `airflow/dags/pipeline_validation.py`.

This pattern is important because it separates heavy compute from final warehouse mutation.

## 12. Streamlit Reads Stable Postgres Outputs

The business app uses query helpers in:

- `app/data_access_grid.py`
- `app/data_access_planning.py`
- `app/data_access_summary.py`

The dashboards read from stable serving tables instead of touching raw storage layers directly.

## Where The Monthly Branch Diverges

The monthly `electricity_power_operational_data` branch diverges in three ways:

- it uses monthly source frequency and monthly window translation
- it produces a separate Gold fact and Platinum serving table
- it is not currently surfaced in the business Streamlit app

In a presentation, it should be described as a secondary analytical branch that broadens the platform beyond the main hourly operational dashboards.
