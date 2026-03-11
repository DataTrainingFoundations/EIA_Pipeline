do $$
begin
    if exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'region_demand_daily_summary'
    ) and not exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'region_demand_daily'
    ) then
        alter table platinum.region_demand_daily_summary rename to region_demand_daily;
    end if;

    if exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'grid_ops_hourly_region_status'
    ) and not exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'grid_operations_hourly'
    ) then
        alter table platinum.grid_ops_hourly_region_status rename to grid_operations_hourly;
    end if;

    if exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'grid_ops_alerts'
    ) and not exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'grid_operations_alert_hourly'
    ) then
        alter table platinum.grid_ops_alerts rename to grid_operations_alert_hourly;
    end if;

    if exists (select 1 from pg_indexes where schemaname = 'platinum' and indexname = 'idx_grid_ops_hourly_period')
       and not exists (select 1 from pg_indexes where schemaname = 'platinum' and indexname = 'idx_grid_operations_hourly_period') then
        alter index platinum.idx_grid_ops_hourly_period rename to idx_grid_operations_hourly_period;
    end if;
    if exists (select 1 from pg_indexes where schemaname = 'platinum' and indexname = 'idx_grid_ops_hourly_respondent')
       and not exists (select 1 from pg_indexes where schemaname = 'platinum' and indexname = 'idx_grid_operations_hourly_respondent') then
        alter index platinum.idx_grid_ops_hourly_respondent rename to idx_grid_operations_hourly_respondent;
    end if;
    if exists (select 1 from pg_indexes where schemaname = 'platinum' and indexname = 'idx_grid_ops_alerts_period')
       and not exists (select 1 from pg_indexes where schemaname = 'platinum' and indexname = 'idx_grid_operations_alert_hourly_period') then
        alter index platinum.idx_grid_ops_alerts_period rename to idx_grid_operations_alert_hourly_period;
    end if;
    if exists (select 1 from pg_indexes where schemaname = 'platinum' and indexname = 'idx_grid_ops_alerts_type')
       and not exists (select 1 from pg_indexes where schemaname = 'platinum' and indexname = 'idx_grid_operations_alert_hourly_type') then
        alter index platinum.idx_grid_ops_alerts_type rename to idx_grid_operations_alert_hourly_type;
    end if;
end $$;
