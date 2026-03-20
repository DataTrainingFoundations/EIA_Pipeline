# Testing State

This document captures the current repo testing posture after the March 19, 2026 testing hardening pass.

## Short Answer

The repo now has a stable default local test workflow with:

- `pytest` as the main test runner
- automated suites for `ingestion`, `airflow`, `app`, `admin_app`, and `spark`
- `0` failures, `0` skips, and `0` warning summaries on the canonical report run
- an enforced total coverage gate of `70%`
- ready-to-open HTML artifacts for both coverage and the full pytest execution report

## Canonical Commands

Install shared test tooling:

`python -m pip install -r requirements-dev.txt`

Run the full suite:

`pytest -q`

Run the release-style report gate:

`powershell -ExecutionPolicy Bypass -File scripts/run_tests.ps1 -Mode coverage`

Generated artifacts:

- coverage HTML: `htmlcov/index.html`
- pytest HTML: `test-results/pytest-report.html`
- JUnit XML: `test-results/junit.xml`

## Current Measured State

On March 19, 2026, the command below ran successfully on this Windows workspace:

`powershell -ExecutionPolicy Bypass -File scripts/run_tests.ps1 -Mode coverage`

Result:

- `146` passed
- `0` skipped
- `0` warning summaries
- overall measured coverage: `72.3%`
- branch coverage was enabled
- enforced total coverage gate: `70%`

Measured breakdown by area:

| Area | Coverage |
| --- | ---: |
| `ingestion/src` | `85.6%` |
| `airflow/dags` | `68.1%` |
| `app` | `75.2%` |
| `admin_app` | `75.7%` |
| `spark/common` + `spark/jobs` | `60.2%` |

## What Changed In Practice

- the Airflow factory test no longer skips when `apache-airflow` is not installed locally
- the Spark tests no longer skip on Windows because they no longer depend on local Hadoop parquet writes for the affected cases
- coverage mode now produces both coverage HTML and a full pytest HTML report
- coverage mode now fails if JUnit reports any failures, errors, or skips
- warnings are treated as test failures in the main pytest configuration
- the local report run is now self-sufficient in this environment and does not depend on installing Airflow or Windows Hadoop tooling

## Practical Test Surface

The repo now has broader direct regression coverage in the highest-risk areas:

- Airflow validation helpers
- Airflow bronze repair queue helpers
- admin comparison key-building helpers
- parquet discovery and query helpers
- Spark duplicate suppression, merge, and transform helpers
- Spark Bronze verification and monthly power transforms

## What This Means

The testing posture is now materially stronger than the previous baseline.

Current strengths:

- stable default local execution with no skips
- enforced minimum coverage threshold
- HTML artifacts suitable for review and handoff
- stronger regression protection around orchestration helpers and Spark data-shape logic

Current remaining gaps:

- no browser or UI automation layer
- no performance or load testing
- no dedicated contract or security testing layer
- several thin Streamlit/bootstrap entrypoints still contribute low or zero coverage and remain good future candidates for targeted tests

## Bottom Line

The repo now has a disciplined local testing gate instead of a best-effort one. The default report run is repeatable, zero-skip in this environment, produces openable HTML artifacts, and clears an enforced `70%` total coverage bar.
