create table if not exists ops.bronze_hourly_coverage (
    dataset_id text not null,
    hour_start_utc timestamptz not null,
    observed_row_count integer not null,
    expected_row_count integer,
    row_count_delta integer,
    status text not null check (status in ('verified', 'missing', 'partial', 'excess', 'observed')),
    oldest_hour_utc timestamptz not null,
    newest_hour_utc timestamptz not null,
    verification_boundary_hour_utc timestamptz not null,
    verified_at timestamptz not null default now(),
    primary key (dataset_id, hour_start_utc)
);

create index if not exists idx_bronze_hourly_coverage_dataset_status_hour
    on ops.bronze_hourly_coverage (dataset_id, status, hour_start_utc);

create or replace view ops.bronze_hourly_coverage_summary as
select
    dataset_id,
    status,
    count(*) as hour_count,
    min(hour_start_utc) as min_hour_start_utc,
    max(hour_start_utc) as max_hour_start_utc,
    max(verified_at) as last_verified_at
from ops.bronze_hourly_coverage
group by dataset_id, status;
