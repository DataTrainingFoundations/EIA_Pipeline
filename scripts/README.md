# Scripts

## Purpose
This folder contains setup, SQL-apply, and test-run commands that make local teammate workflows consistent.

## Important Files
- `apply_warehouse_sql.sh`
- `apply_warehouse_sql.ps1`
- `run_e2e.sh`
- `run_e2e.ps1`
- `run_historical.sh`
- `run_historical.ps1`
- `run_tests.sh`
- `run_tests.ps1`

## Add Something New
1. Add a script here only if it supports a repeatable teammate workflow.
2. Keep Windows and shell variants aligned when both platforms are supported.
3. Document the script here and in the root `README.md`.

## Follow This Function Next
- Use `run_tests.sh` or `run_tests.ps1` for local validation.
- Install shared test tooling with `python -m pip install -r requirements-dev.txt` before using lint, format, or coverage modes.
- Use `scripts/run_tests.sh quality` as the main local "make sure code passes" command.
- Use `scripts/run_tests.sh coverage` or `scripts/run_tests.ps1 -Mode coverage` as the release-style test report gate.
- Follow the called `pytest` target or setup command from there into the repo area you are changing.

## Relevant Tests
- `scripts/run_tests.sh lint`
- `scripts/run_tests.sh format-check`
- `scripts/run_tests.sh quality`
- `scripts/run_tests.sh fast`
- `scripts/run_tests.sh app-airflow`
- `scripts/run_tests.sh spark`
- `scripts/run_tests.sh coverage`

Coverage mode guarantees:

- full-suite pytest run
- total coverage fail-under `70`
- JUnit validation for `0` failures, `0` errors, and `0` skips
- HTML outputs at `htmlcov/index.html` and `test-results/pytest-report.html`

## Common Mistakes
- Hiding one-off debugging commands here without documentation.
- Updating only one platform-specific script.
- Running end-to-end scripts when a focused test target would be faster.
