# Testing State

This document describes the current testing posture of the repo as it exists today. It separates what is actually present from what still needs to be added before the test strategy can be called mature.

## Short Answer

The project has meaningful automated testing, and it is not limited to unit tests.

Current state:

- `pytest` is the main automated test runner
- the repo has test suites for `ingestion`, `airflow`, `app`, `admin_app`, and `spark`
- there are also script-driven end-to-end and historical backfill checks in `scripts/`
- the repo now has repeatable coverage tooling through `pytest.ini`, `.coveragerc`, `requirements-dev.txt`, and `scripts/run_tests.*`
- there is still no enforced minimum coverage threshold

## How To Run It

Install shared test tooling:

`python -m pip install -r requirements-dev.txt`

Run the full suite:

`pytest -q`

Run coverage:

`powershell -ExecutionPolicy Bypass -File scripts/run_tests.ps1 -Mode coverage`

HTML coverage report:

`htmlcov/index.html`

## Current Coverage Snapshot

Coverage should be discussed in two ways:

- measured code coverage
- practical test surface coverage

### Measured Code Coverage

On March 19, 2026, the command below was run successfully:

`powershell -ExecutionPolicy Bypass -File scripts/run_tests.ps1 -Mode coverage`

Result:

- `79` passed
- `26` skipped
- overall measured coverage: `58.2%`
- branch coverage was enabled for the run
- HTML report was written to `htmlcov/index.html`

Measured breakdown by area:

| Area | Coverage |
| --- | ---: |
| `ingestion/src` | `85.8%` |
| `airflow/dags` | `56.9%` |
| `app` | `74.9%` |
| `admin_app` | `66.2%` |
| `spark/common` + `spark/jobs` | `36.5%` |

Important caveat:

- most skipped tests were Spark tests that require a local Spark session
- in this environment, Spark session startup failed with a Py4J constructor error, so many data-frame-heavy tests were skipped
- one Airflow test module was skipped because the local Python environment did not have the `airflow` package installed
- because of that, the `58.2%` number is a real baseline, but it is still conservative for Spark-heavy logic

### Practical Test Surface Coverage

By static count, the repo currently contains:

- `29` test files
- `105` named test functions

Breakdown by area:

- `ingestion/tests`: `3` files, `19` tests
- `airflow/tests`: `6` files, `16` tests
- `app/tests`: `4` files, `15` tests
- `admin_app/tests`: `5` files, `17` tests
- `spark/tests`: `11` files, `38` tests

That means the repo has broad automated test surface even though full measured execution coverage is lower than we would want.

## Types Of Testing Present In The Repo

### 1. Unit Tests

These exist across all major areas and focus on pure functions, argument parsing, validation helpers, query helpers, and transform logic.

Examples:

- ingestion window-boundary and event-building logic
- Airflow helper functions for backfill, validation, and runtime helpers
- app data-access and page-logic helpers
- Spark quality helpers and transformation functions

### 2. Component Or Integration-Style Tests

These tests exercise a larger slice of behavior than a pure unit test, but usually with mocks, fixtures, or local Spark sessions instead of a full running platform.

Examples:

- DAG construction and task wiring tests in `airflow/tests`
- CLI entrypoint tests in `ingestion/tests` and `spark/tests`
- Spark transformation tests that run against a local Spark session
- app query-layer tests that validate filtering and error handling behavior

This is important because many of the current tests are more valuable than shallow unit tests. They validate orchestration wiring, data-shape assumptions, and CLI behavior.

### 3. Scripted End-To-End Or Smoke Testing

The repo also contains shell and PowerShell scripts that exercise a running environment:

- `scripts/run_e2e.sh`
- `scripts/run_e2e.ps1`
- `scripts/run_historical.sh`
- `scripts/run_historical.ps1`

These are not the same thing as a full environment integration suite inside `pytest`, but they do provide repeatable environment-level validation paths.

### 4. What Is Not Clearly Present

The following test layers were not found as first-class, repo-owned practices:

- browser or UI automation such as Playwright or Cypress
- load or performance testing
- security testing
- contract testing between services
- chaos or resilience testing
- enforced coverage gates in local scripts or CI-style config

## What Meaningful Coverage Should Mean Here

Meaningful coverage is not the same thing as maximizing a percentage.

In this codebase, meaningful coverage should prove that the highest-risk pipeline behaviors are protected:

- ingestion windows are translated correctly at API boundaries
- deterministic `event_id` creation remains stable
- Bronze replay and deduplication logic stays safe
- Silver and Gold transforms preserve business-key semantics
- Airflow backfill queue state transitions stay correct
- DAG wiring still launches the right steps in the right order
- validation checks fail when row counts, distinct counts, or numeric bounds are wrong
- serving-layer queries still support the business app correctly

If those areas are covered, a lower numeric percentage can still be more useful than a higher percentage built from shallow tests.

## Current Strengths

- all major subsystems have automated tests
- ingestion has strong measured coverage
- Spark transformation logic has meaningful test depth
- Airflow DAG builders and queue helpers are tested directly
- ingestion edge cases such as windowing and validation are tested
- app query and page-support logic has targeted regression coverage
- the repo includes repeatable smoke and historical scripts, not only local one-off commands

## Current Gaps

- no enforced minimum coverage threshold
- no explicit branch coverage target by subsystem
- no visible automated UI test layer
- local environment issues currently suppress a large block of Spark execution tests
- one Airflow test currently depends on the `airflow` package being available locally
- limited proof of full environment integration beyond scripts
- no visible performance or security validation layer

## Practical Assessment

The current testing posture is better than "just unit tests" but weaker than a fully mature test strategy.

The repo is strongest at:

- logic-level regression protection
- data transformation correctness
- DAG and helper wiring validation

The repo is weaker at:

- measured quality targets
- full-system automation
- non-functional testing

## If We Want To Improve Testing Next

The next useful upgrades would be:

- stabilize local Spark test execution so the skipped Spark suite can run everywhere
- install or containerize a local Airflow test environment so the skipped factory test always executes
- define a minimum coverage threshold for critical modules
- convert the smoke scripts into more repeatable integration checks where practical
- add at least one browser-level test pass for the Streamlit surfaces
- add a small number of environment-level tests for the Kafka -> Bronze -> Silver -> Gold -> Platinum handoff

## Bottom Line

The project has real testing across multiple layers and now has a repeatable measured coverage workflow. What it still lacks is a disciplined coverage target, stable execution of all environment-dependent tests, and a clearly agreed definition of what "enough coverage" means for release confidence.
