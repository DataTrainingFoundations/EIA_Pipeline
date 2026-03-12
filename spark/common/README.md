# Spark Common Helpers

## Purpose
This folder contains reusable Spark helpers shared across multiple jobs.

## Important Files
- `config.py`: runtime configuration object.
- `io.py`: path existence, parquet globs, and safe reads.
- `logging_utils.py`: structured job log helpers.
- `quality.py`: reusable data quality assertions.
- `windowing.py`: source-window filtering helpers.

## Add Something New
1. Put a helper here only if at least two jobs benefit from it.
2. Keep the helper narrow and free of job-specific business logic.
3. Add a direct unit test in `spark/tests` when you add a new helper.

## Follow This Function Next
- `logging_utils.log_job_start`
- `io.read_partitioned_parquet`
- `quality.assert_*`

## Relevant Tests
- `spark/tests/test_logging_utils.py`
- `spark/tests/test_config.py`

## Common Mistakes
- Moving business logic here just to reduce line count.
- Adding a helper without a direct unit test.
- Creating helpers that silently swallow data quality issues.
