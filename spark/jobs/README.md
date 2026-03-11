# Spark Jobs

## Purpose
This folder contains the executable Spark jobs that Airflow submits during pipeline runs.

## Important Files
- `bronze_kafka_to_minio.py`: Kafka to Bronze batch ingestion.
- `silver_clean_transform.py`: Bronze cleanup into Silver.
- `gold_region_fuel_serving_hourly.py`: curated Gold fact and dimension builder.
- `platinum_grid_operations_hourly.py`: grid operations stage builder.
- `platinum_resource_planning_daily.py`: planning stage builder.
- `bronze_hourly_coverage_verify.py`: Bronze verification stage builder.

## Add Something New
1. Create a new job file only when the output contract is distinct.
2. Keep CLI parsing in `parse_args`.
3. Put reusable code in `spark/common` instead of copying it into another job.
4. Log start, skip, and completion with the shared logging helper.
5. Add or update tests before wiring the new job into Airflow.

## Follow This Function Next
- `main`
- the job-specific transform builder
- the write helper or JDBC write call

## Relevant Tests
- `spark/tests/test_bronze_cli.py`
- `spark/tests/test_gold_platinum_cli.py`
- `spark/tests/test_persona_cli.py`
- `spark/tests/test_silver_cli.py`

## Common Mistakes
- Mixing argument parsing, transformation logic, and write logic into one block.
- Reading all parquet files when a partitioned glob is available.
- Writing logs that do not explain what table or path is being populated.
