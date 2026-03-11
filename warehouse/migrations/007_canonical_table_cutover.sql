do $$
begin
    if exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'region_demand_daily_summary'
    ) and exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'region_demand_daily'
    ) then
        insert into platinum.region_demand_daily (
            date, respondent, daily_demand_mwh, avg_hourly_demand_mwh, peak_hourly_demand_mwh, loaded_at, source_window_start, source_window_end, updated_at
        )
        select date, respondent, daily_demand_mwh, avg_hourly_demand_mwh, peak_hourly_demand_mwh, loaded_at, source_window_start, source_window_end, updated_at
        from platinum.region_demand_daily_summary
        on conflict (date, respondent) do update set
            daily_demand_mwh = excluded.daily_demand_mwh,
            avg_hourly_demand_mwh = excluded.avg_hourly_demand_mwh,
            peak_hourly_demand_mwh = excluded.peak_hourly_demand_mwh,
            loaded_at = excluded.loaded_at,
            source_window_start = excluded.source_window_start,
            source_window_end = excluded.source_window_end,
            updated_at = excluded.updated_at;
        drop table platinum.region_demand_daily_summary;
    end if;

    if exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'grid_ops_hourly_region_status'
    ) and exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'grid_operations_hourly'
    ) then
        insert into platinum.grid_operations_hourly (
            period, respondent, respondent_name, actual_demand_mwh, day_ahead_forecast_mwh, forecast_error_mwh, forecast_error_pct, forecast_error_zscore,
            demand_ramp_mwh, demand_ramp_zscore, demand_zscore, total_generation_mwh, renewable_generation_mwh, fossil_generation_mwh,
            gas_generation_mwh, renewable_share_pct, fossil_share_pct, gas_share_pct, generation_gap_mwh, coverage_ratio, alert_count, updated_at
        )
        select
            period, respondent, respondent_name, actual_demand_mwh, day_ahead_forecast_mwh, forecast_error_mwh, forecast_error_pct, forecast_error_zscore,
            demand_ramp_mwh, demand_ramp_zscore, demand_zscore, total_generation_mwh, renewable_generation_mwh, fossil_generation_mwh,
            gas_generation_mwh, renewable_share_pct, fossil_share_pct, gas_share_pct, generation_gap_mwh, coverage_ratio, alert_count, updated_at
        from platinum.grid_ops_hourly_region_status
        on conflict (period, respondent) do update set
            respondent_name = excluded.respondent_name,
            actual_demand_mwh = excluded.actual_demand_mwh,
            day_ahead_forecast_mwh = excluded.day_ahead_forecast_mwh,
            forecast_error_mwh = excluded.forecast_error_mwh,
            forecast_error_pct = excluded.forecast_error_pct,
            forecast_error_zscore = excluded.forecast_error_zscore,
            demand_ramp_mwh = excluded.demand_ramp_mwh,
            demand_ramp_zscore = excluded.demand_ramp_zscore,
            demand_zscore = excluded.demand_zscore,
            total_generation_mwh = excluded.total_generation_mwh,
            renewable_generation_mwh = excluded.renewable_generation_mwh,
            fossil_generation_mwh = excluded.fossil_generation_mwh,
            gas_generation_mwh = excluded.gas_generation_mwh,
            renewable_share_pct = excluded.renewable_share_pct,
            fossil_share_pct = excluded.fossil_share_pct,
            gas_share_pct = excluded.gas_share_pct,
            generation_gap_mwh = excluded.generation_gap_mwh,
            coverage_ratio = excluded.coverage_ratio,
            alert_count = excluded.alert_count,
            updated_at = excluded.updated_at;
        drop table platinum.grid_ops_hourly_region_status;
    end if;

    if exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'grid_ops_alerts'
    ) and exists (
        select 1 from information_schema.tables
        where table_schema = 'platinum' and table_name = 'grid_operations_alert_hourly'
    ) then
        insert into platinum.grid_operations_alert_hourly (
            period, respondent, respondent_name, alert_type, severity, metric_value, threshold_value, message, updated_at
        )
        select period, respondent, respondent_name, alert_type, severity, metric_value, threshold_value, message, updated_at
        from platinum.grid_ops_alerts
        on conflict (period, respondent, alert_type) do update set
            respondent_name = excluded.respondent_name,
            severity = excluded.severity,
            metric_value = excluded.metric_value,
            threshold_value = excluded.threshold_value,
            message = excluded.message,
            updated_at = excluded.updated_at;
        drop table platinum.grid_ops_alerts;
    end if;
end $$;
