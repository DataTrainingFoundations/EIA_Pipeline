create table if not exists platinum.electric_power_operations_monthly (
    period timestamptz not null,
    location text not null,
    location_name text not null,
    sector_id text not null,
    sector_name text not null,
    fueltype_id text not null,
    fueltype_name text not null,
    generation_mwh double precision,
    generation_share_pct double precision,
    consumption_for_eg_thousand_units double precision,
    ash_content_pct double precision,
    heat_content_btu_per_unit double precision,
    fuel_heat_input_mmbtu double precision,
    heat_rate_btu_per_kwh double precision,
    loaded_at timestamptz not null,
    source_window_start timestamptz,
    source_window_end timestamptz,
    updated_at timestamptz not null default now(),
    primary key (period, location, sector_id, fueltype_id)
);

create index if not exists idx_electric_power_operations_monthly_period
    on platinum.electric_power_operations_monthly (period);

create index if not exists idx_electric_power_operations_monthly_sector
    on platinum.electric_power_operations_monthly (sector_id);

create index if not exists idx_electric_power_operations_monthly_fuel
    on platinum.electric_power_operations_monthly (fueltype_id);
