create table if not exists platinum.grid_operations_hourly (
    period timestamptz not null,
    respondent text not null,
    respondent_name text,
    actual_demand_mwh double precision,
    day_ahead_forecast_mwh double precision,
    forecast_error_mwh double precision,
    forecast_error_pct double precision,
    forecast_error_zscore double precision,
    demand_ramp_mwh double precision,
    demand_ramp_zscore double precision,
    demand_zscore double precision,
    total_generation_mwh double precision,
    renewable_generation_mwh double precision,
    fossil_generation_mwh double precision,
    gas_generation_mwh double precision,
    renewable_share_pct double precision,
    fossil_share_pct double precision,
    gas_share_pct double precision,
    generation_gap_mwh double precision,
    coverage_ratio double precision,
    alert_count integer not null default 0,
    updated_at timestamptz not null default now(),
    primary key (period, respondent)
);

create index if not exists idx_grid_operations_hourly_period
    on platinum.grid_operations_hourly (period);

create index if not exists idx_grid_operations_hourly_respondent
    on platinum.grid_operations_hourly (respondent);

create table if not exists platinum.grid_operations_alert_hourly (
    period timestamptz not null,
    respondent text not null,
    respondent_name text,
    alert_type text not null,
    severity text not null,
    metric_value double precision,
    threshold_value double precision,
    message text not null,
    updated_at timestamptz not null default now(),
    primary key (period, respondent, alert_type)
);

create index if not exists idx_grid_operations_alert_hourly_period
    on platinum.grid_operations_alert_hourly (period);

create index if not exists idx_grid_operations_alert_hourly_type
    on platinum.grid_operations_alert_hourly (alert_type);

create table if not exists platinum.resource_planning_daily (
    date date not null,
    respondent text not null,
    respondent_name text,
    daily_demand_mwh double precision not null,
    peak_hourly_demand_mwh double precision,
    avg_abs_forecast_error_pct double precision,
    renewable_share_pct double precision,
    fossil_share_pct double precision,
    gas_share_pct double precision,
    carbon_intensity_kg_per_mwh double precision,
    fuel_diversity_index double precision,
    peak_hour_gas_share_pct double precision,
    clean_coverage_ratio double precision,
    weekend_flag boolean not null,
    updated_at timestamptz not null default now(),
    primary key (date, respondent)
);

create index if not exists idx_resource_planning_daily_date
    on platinum.resource_planning_daily (date);

create index if not exists idx_resource_planning_daily_respondent
    on platinum.resource_planning_daily (respondent);
