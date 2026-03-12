create schema if not exists ops;

create table if not exists ops.backfill_jobs (
    id bigserial primary key,
    dataset_id text not null,
    chunk_start_utc timestamptz not null,
    chunk_end_utc timestamptz not null,
    status text not null check (status in ('pending', 'in_progress', 'completed', 'failed')),
    attempt_count integer not null default 0,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    constraint backfill_jobs_unique unique (dataset_id, chunk_start_utc, chunk_end_utc)
);

create index if not exists idx_backfill_jobs_dataset_status_start
    on ops.backfill_jobs (dataset_id, status, chunk_start_utc);

create or replace view ops.backfill_job_summary as
select
    dataset_id,
    status,
    count(*) as job_count,
    min(chunk_start_utc) as min_chunk_start_utc,
    max(chunk_end_utc) as max_chunk_end_utc,
    max(updated_at) as last_updated_at
from ops.backfill_jobs
group by dataset_id, status;
