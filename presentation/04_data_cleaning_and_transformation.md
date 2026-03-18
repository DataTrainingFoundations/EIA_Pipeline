# Data Cleaning And Transformation

This document focuses on what the code actually does to make source data usable. It separates hard validation from business derivation.

## Stage 1: Bronze Parsing And Raw Persistence

Implementation:

- `spark/jobs/bronze_kafka_to_minio.py`
- `spark/common/schemas.py`
- `spark/common/quality.py`

### Problem Solved

Kafka contains JSON event messages. Spark needs a stable raw storage shape before later cleaning steps can safely read data.

### Actual Rules

- Kafka `value` is parsed against `KAFKA_EVENT_SCHEMA`.
- Rows with unparseable event JSON are dropped by filtering `event is not null`.
- `event_timestamp` and `ingestion_timestamp` are converted to Spark timestamps.
- Partition columns are derived from `event_ts`.
- Rows missing `event_id`, `dataset`, or `event_ts` are filtered out.
- `assert_no_nulls` enforces those required columns.

### Why It Matters

Bronze is the first durable raw layer. If the schema is unstable here, every downstream stage becomes less trustworthy.

## Stage 1A: Bronze Duplicate Suppression

Implementation:

- `prepare_bronze_write_plan` in `spark/jobs/bronze_kafka_to_minio.py`
- tested in `spark/tests/test_bronze_dedupe.py`

### Problem Solved

The platform must tolerate:

- duplicate events within one Kafka batch
- replayed events already written in earlier Bronze runs

### Actual Rules

- incoming batch duplicates are counted by `event_id`
- the batch is deduplicated on `event_id`
- only touched Bronze partition paths are scanned for existing `event_id` values
- the write batch is a left anti join against existing Bronze `event_id` values
- the final write batch must still satisfy `assert_unique_keys(["event_id"])`

### Why It Matters

This is the main reason the Bronze layer is replay-safe without relying on exactly-once end-to-end transactions.

## Stage 2: Silver Cleaning

Implementation:

- `spark/jobs/silver_clean_transform.py`
- `spark/common/quality.py`

Silver is where the code starts enforcing dataset semantics instead of just raw event structure.

### Shared Silver Behaviors

#### Problem Solved

Different datasets need different required fields and valid units, but they all need a reliable cleaned representation.

#### Actual Rules

- period values are parsed with multiple supported formats
- text fields are trimmed
- unit labels are normalized to lowercase and trimmed
- records with missing required identifiers or timestamps are dropped
- `_latest_by_event` keeps the newest cleaned record for each `event_id`
- `assert_no_conflicting_records` rejects cases where the same `event_id` maps to different cleaned values
- `assert_unique_keys(["event_id"])` enforces one cleaned row per event
- `validate_non_empty` enforces a minimum retention ratio of 80% of distinct source `event_id` values, unless no source rows exist

#### Why It Matters

Silver is the platform’s trust boundary for cleaned data. Later stages assume Silver is already normalized enough to support stable business keys.

### Silver Region Rules

Implementation:

- `clean_region_data`

#### Actual Rules

- dataset filter: `electricity_region_data`
- required fields include `period`, `respondent`, `respondent_name`, `type`, `value`, `value_units`
- allowed `type` values are `D` and `DF`
- allowed units are `mwh` and `megawatthours`
- `event_date` is derived from `period`

#### Problem Solved

The region feed mixes actual demand and day-ahead forecast rows. Silver turns them into a consistent shape with explicit type labels.

#### Why It Matters

Gold later depends on `type == "D"` versus `type == "DF"` to build forecast comparison metrics.

### Silver Fuel Rules

Implementation:

- `clean_fuel_type_data`

#### Actual Rules

- dataset filter: `electricity_fuel_type_data`
- required fields include `period`, `respondent`, `respondent_name`, `fueltype`, `value`, `value_units`
- allowed units are `mwh` and `megawatthours`
- `event_date` is derived from `period`

#### Problem Solved

The raw fuel feed needs consistent fuel identifiers and numeric generation values before it can be aggregated by fuel family.

#### Why It Matters

Grid operations and planning both rely on consistent fuel type grouping.

### Silver Power-Operations Rules

Implementation:

- `clean_power_operational_data`

#### Actual Rules

- dataset filter: `electricity_power_operational_data`
- required fields include `period`, `location`, `location_name`, `sector_id`, `sector_name`, `fueltype_id`, `fueltype_name`
- units are validated separately for ash content, consumption, generation, and heat content
- `event_date` is derived from `period`

#### Problem Solved

Monthly operational data contains several numeric measures with different unit systems. Silver validates those assumptions before any derived metric is calculated.

#### Why It Matters

The monthly Platinum branch later computes generation share, heat input, and heat rate, which would be misleading if units were inconsistent.

## Stage 3: Gold Canonicalization

Gold is where the system moves from cleaned rows to canonical business facts and dimensions.

### Gold Region Hourly Fact

Implementation:

- `build_region_hourly_metrics` in `spark/jobs/gold_region_fuel_serving_hourly.py`

#### Actual Rules

- business key: `period`, `respondent`, `type`
- the latest loaded record wins for each business key
- demand rows and forecast rows are aggregated separately
- both are joined into one hourly fact
- forecast error and forecast error percentage are derived
- uniqueness is enforced on `period`, `respondent`

### Gold Fuel Hourly Fact

Implementation:

- `build_fuel_type_hourly_generation`

#### Actual Rules

- business key: `period`, `respondent`, `fueltype`
- latest loaded record wins
- negative generation is clipped to zero (I just realizer this might be a mistake in the way I managed but we nay be able to ignore it)
- uniqueness is enforced on `period`, `respondent`, `fueltype`

### Gold Respondent Dimension

Implementation:

- `build_respondent_dimension`

#### Actual Rules

- combines current observations with existing dimension data
- latest observed respondent name wins
- maintains `first_seen_date`, `last_seen_date`, and `source_dataset_count`

### Gold Fuel Type Dimension

Implementation:

- `build_fuel_type_dimension`

#### Actual Rules

- latest fuel name wins
- fuel categories are mapped as `renewable`, `fossil`, `nuclear`, or `other`
- emissions factors come from the hard-coded `EMISSIONS_FACTORS` map

### Gold Monthly Power Fact

Implementation:

- `build_power_operations_monthly_fact` in `spark/jobs/gold_power_operations_monthly.py`

#### Actual Rules

- business key: `period`, `location`, `sector_id`, `fueltype_id`
- latest loaded record wins
- `generation_thousand_mwh` is converted to `generation_mwh`
- negative values are clipped to zero for generation, consumption, ash content, and heat content

## Stage 4: Platinum Business Derivation

Platinum is where the system calculates business-facing metrics.

## Hard Validation Vs Business Derivation

### Hard Validation

Hard validation appears in both Spark and Airflow:

- required columns must be non-null
- keys must be unique
- values must be in allowed units or categories
- numeric bounds must hold where enforced
- post-merge serving tables must contain expected rows or distinct values

### Business Derivation

Business derivation computes analytically useful outputs such as:

- forecast error
- z-scores
- renewable share
- coverage ratio
- carbon intensity
- fuel diversity index
- heat rate

### Platinum Region Demand Daily

Implementation:

- `spark/jobs/platinum_region_demand_daily.py`

#### Actual Derivations

- `daily_demand_mwh`
- `avg_hourly_demand_mwh`
- `peak_hourly_demand_mwh`
- `source_window_start`
- `source_window_end`

### Platinum Grid Operations Hourly

Implementation:

- `build_grid_operations_status` in `spark/jobs/platinum_grid_operations_hourly.py`

#### Actual Derivations

- total generation from fuel facts
- renewable, fossil, and gas generation totals
- `generation_gap_mwh`
- `coverage_ratio`
- renewable, fossil, and gas share percentages
- `demand_ramp_mwh`
- respondent-level z-scores for forecast error, ramp, and demand
- `alert_count`

It also derives alert rows for:

- forecast error anomalies
- ramp risk
- fuel mix support gap
- demand anomaly

### Platinum Resource Planning Daily

Implementation:

- `build_resource_planning_daily` in `spark/jobs/platinum_resource_planning_daily.py`

#### Actual Derivations

- daily demand and peak demand
- average absolute forecast error percentage
- renewable, fossil, and gas share
- carbon intensity from weighted fuel emissions
- fuel diversity index using `1 - sum(share^2)`
- peak-hour gas share percentage
- `clean_coverage_ratio`
- weekend flag

### Platinum Monthly Power Operations

Implementation:

- `build_power_operations_monthly` in `spark/jobs/platinum_power_operations_monthly.py`

#### Actual Derivations

- `generation_share_pct`
- `fuel_heat_input_mmbtu`
- `heat_rate_btu_per_kwh`
- source window tracking columns

## Summary

The codebase uses a layered strategy:

- Bronze makes raw events durable and replay-safe
- Silver validates and normalizes by dataset
- Gold resolves revisions into canonical facts and dimensions
- Platinum turns curated facts into business-ready metrics
