create schema if not exists raw;
create schema if not exists silver;
create schema if not exists gold;
create schema if not exists meta;
create table if not exists meta.pipeline_run_state (
    dataset_id string,
    frequency string,
    bootstrap_ingest_complete boolean,
    bootstrap_transform_complete boolean,
    last_raw_partition date,
    last_silver_partition date,
    last_gold_partition date,
    last_ingest_started_at timestamp_ntz,
    last_ingest_succeeded_at timestamp_ntz,
    last_transform_started_at timestamp_ntz,
    last_transform_succeeded_at timestamp_ntz,
    last_ingest_error_message string,
    last_transform_error_message string,
    updated_at timestamp_ntz
);
