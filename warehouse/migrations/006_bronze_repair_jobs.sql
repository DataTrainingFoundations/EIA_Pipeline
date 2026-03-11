create table if not exists ops.bronze_repair_jobs (
    id bigserial primary key,
    dataset_id text not null,
    hour_start_utc timestamptz not null,
    hour_end_utc timestamptz not null,
    coverage_status text not null check (coverage_status in ('missing', 'partial', 'excess', 'observed', 'verified')),
    status text not null check (status in ('pending', 'in_progress', 'completed', 'failed')),
    attempt_count integer not null default 0,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    coverage_verified_at timestamptz not null default now(),
    constraint bronze_repair_jobs_unique unique (dataset_id, hour_start_utc, hour_end_utc)
);

create index if not exists idx_bronze_repair_jobs_dataset_status_hour
    on ops.bronze_repair_jobs (dataset_id, status, hour_start_utc);

create or replace view ops.bronze_repair_job_summary as
select
    dataset_id,
    coverage_status,
    status,
    count(*) as job_count,
    min(hour_start_utc) as min_hour_start_utc,
    max(hour_end_utc) as max_hour_end_utc,
    max(updated_at) as last_updated_at
from ops.bronze_repair_jobs
group by dataset_id, coverage_status, status;
