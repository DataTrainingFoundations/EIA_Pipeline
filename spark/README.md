# Spark

## Purpose
This folder owns the medallion transformations that move data from Bronze to Silver to Gold and then into platinum stage tables.

## Important Files
- `jobs/`: Spark entrypoints.
- `common/config.py`: Spark runtime configuration.
- `common/io.py`: shared filesystem and parquet helpers.
- `common/logging_utils.py`: shared Spark log formatting.
- `common/quality.py`: reusable data quality assertions.

## Add Something New
1. Decide whether the new work is a reusable helper or a job entrypoint.
2. Put generic filesystem or logging code in `common/`.
3. Keep each `jobs/*.py` file as a readable CLI entrypoint plus named transform helpers.
4. Log the input path, output path or stage table, dataset id, and source window.
5. Add tests in `spark/tests`.

## Follow This Function Next
- Start in the job `main` function.
- Then follow the transform helper used to build the output DataFrame.
- Finish at the write helper or JDBC write call.

## Relevant Tests
- `spark/tests/test_logging_utils.py`
- `spark/tests/test_transforms.py`
- `spark/tests/test_bronze_dedupe.py`
- `spark/tests/test_bronze_verification.py`

## Common Mistakes
- Duplicating path-existence logic instead of using `common/io.py`.
- Hiding business logic inside one long DataFrame chain without named steps.
- Logging completion without row counts or destination details.
