## Run

Create an env file.

`cp .env.example .env`

Start the platform.

`docker compose up -d --build`

Apply warehouse SQL to an already-running Postgres volume.

`bash scripts/apply_warehouse_sql.sh`

On Windows:

`powershell -ExecutionPolicy Bypass -File scripts/apply_warehouse_sql.ps1`

## Team Docs

Team-facing onboarding now lives next to the code instead of only in `docs/`.

- `ingestion/README.md`
- `airflow/README.md`
- `airflow/dags/README.md`
- `spark/README.md`
- `spark/jobs/README.md`
- `spark/common/README.md`
- `app/README.md`
- `app/pages/README.md`
- `warehouse/README.md`
- `scripts/README.md`

`admin_app/` is intentionally out of scope for shared ownership in this handoff.

## Tests

Run the full suite:

`pytest -q`

Run the standard script wrappers:

`bash scripts/run_tests.sh fast`

`bash scripts/run_tests.sh app-airflow`

`bash scripts/run_tests.sh spark`

On Windows:

`powershell -ExecutionPolicy Bypass -File scripts/run_tests.ps1 -Mode fast`

## Current DAGs

Dataset DAGs:

- `electricity_region_data_incremental`
- `electricity_region_data_backfill`
- `electricity_fuel_type_data_incremental`
- `electricity_fuel_type_data_backfill`

Business serving DAGs:

- `platinum_grid_operations_hourly`
- `platinum_resource_planning_daily`

## Backfill Behavior

Backfill runs newest-first.
The first chunk spans the current UTC week start through the current hour boundary.
Later chunks move backward one UTC calendar week at a time.
Backfill runs on the Airflow schedule from `AIRFLOW_BACKFILL_SCHEDULE`, which defaults to `5 * * * *`.
Each scheduled run processes at most one backfill chunk.

## Time and UI Notes

- Warehouse timestamps are stored in `UTC`.
- Streamlit can label charts using `APP_TIMEZONE` from `.env`.
- The dashboards also label the active display timezone so timestamps are easier to interpret.
- Map markers use approximate balancing-authority coordinates, not official market polygons.

## Operational Notes

- Airflow startup creates the admin user automatically.
- Airflow startup registers, warms Spark dependencies, and then auto-unpauses dataset and business serving DAGs.
- Airflow startup now retries DAG unpause until the scheduler can confirm the DAG is active.
- Airflow submits directly to the Spark standalone cluster; it no longer shells into sibling containers.
- Kafka is used as a bounded ingestion buffer, with dataset-specific topics keyed by `event_id`.
- Bronze uses dataset-specific MinIO paths, bounded Kafka batch reads, and offset tracking in `offsets.json` rather than Kafka consumer groups.
- Bronze is replay-safe by `event_id` and stores hourly partitions beneath each dataset path.
- Bronze verification and repair DAGs are currently disabled from active orchestration and remain on disk only for future reuse.
- Incremental DAGs now seed backfill runs, and backfill execution is globally serialized to reduce machine load.
- `electricity_region_data_incremental` remains hourly, while `electricity_fuel_type_data_incremental` now runs daily at `18:20 UTC` because the fuel source can lag well behind the current hour.
- Backfill DAGs retry transient failures automatically, and stale in-progress backfill jobs are requeued up to a capped attempt count.
- Fuel incremental validation no longer fails when an hourly source window legitimately returns no new valid rows.
- Gold now materializes explicit fact datasets at `s3a://gold/facts/region_demand_forecast_hourly` and `s3a://gold/facts/fuel_generation_hourly`.
- Gold also materializes dimensions at `s3a://gold/dimensions/respondent` and `s3a://gold/dimensions/fuel_type`.
- Platinum marts read the Gold facts, and resource planning also uses the Gold fuel dimension for emissions metadata.
- Platinum DAGs wait for one completed backfill chunk from both datasets before their first successful runs, then continue on hourly incremental curated-gold dependencies.
- Silver and Gold remain day-partitioned by design.
- Backfill operations should use normal scheduling or `airflow dags trigger`, not `airflow dags test`.

## Windowing and Bronze Notes

- Ingestion fetch windows now follow `[start, end)` semantics. Airflow and Spark pass an exclusive `end`, and the ingestion layer translates that into the inclusive EIA API boundary internally.
- Bronze counts can differ from a current API snapshot for two reasons:
  - a prior boundary-hour over-fetch bug, now fixed by the ingestion window translation
  - intentional append-only retention of revised EIA rows by `event_id`, which remains unchanged

## Useful Checks

`docker compose exec -T airflow airflow dags list-runs -d electricity_region_data_backfill --no-backfill`

`docker compose exec -T airflow airflow dags list-runs -d electricity_fuel_type_data_backfill --no-backfill`

`docker compose exec -T airflow airflow dags list-runs -d platinum_grid_operations_hourly --no-backfill`

`docker compose exec -T airflow airflow dags list-runs -d platinum_resource_planning_daily --no-backfill`

## UIs

Airflow `http://localhost:28080`  
Airflow login: `admin` / `admin`  
MinIO `http://localhost:29001`  
MinIO login: `minioadmin` / `minioadmin123`  
Spark master UI `http://localhost:28088`  
Kafka broker `localhost:29092`  
Postgres `localhost:25432`  
Business app `http://localhost:28501`  
Admin console `http://localhost:28502`
