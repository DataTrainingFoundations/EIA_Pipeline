create table if not exists platinum.region_demand_daily (
    date date not null,
    respondent text not null,
    daily_demand_mwh double precision not null,
    avg_hourly_demand_mwh double precision not null,
    peak_hourly_demand_mwh double precision not null,
    loaded_at timestamptz not null,
    source_window_start timestamptz,
    source_window_end timestamptz,
    updated_at timestamptz not null default now(),
    primary key (date, respondent)
);

create index if not exists idx_region_demand_daily_date
    on platinum.region_demand_daily (date);

create index if not exists idx_region_demand_daily_respondent
    on platinum.region_demand_daily (respondent);
