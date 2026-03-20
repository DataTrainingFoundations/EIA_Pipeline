# Code Quality And Design Patterns

This document is meant to support a presentation segment about maintainability. It focuses on what the repo demonstrably does today, not on idealized process language.

## Code Quality Story

The codebase shows intentional quality controls in three main ways:

- shared helpers are separated from job entrypoints and UI pages
- high-risk pipeline behavior is covered by automated tests
- data quality is enforced in code, not left to manual inspection

Examples visible in the repo:

- ingestion event construction is centralized in `ingestion/src/event_factory.py`
- dataset configuration is centralized in `ingestion/src/dataset_registry.yml`
- Spark quality assertions are centralized in `spark/common/quality.py`
- Airflow validation helpers are centralized in `airflow/dags/pipeline_validation.py`
- Streamlit query access is separated into `app/data_access_*.py`

That structure matters because it reduces duplicated logic and makes regressions easier to test.

## Design Patterns Used

The repo does not advertise formal Gang-of-Four terminology, but several practical design patterns are clearly present.

### 1. Registry-Driven Configuration

The strongest pattern in the system is a registry-driven control plane.

Evidence:

- `ingestion/src/dataset_registry.yml`
- `ingestion/src/registry.py`
- `airflow/dags/pipeline_factories.py`
- `airflow/dags/pipeline_dataset_dags.py`

Why it matters:

- dataset-specific behavior is expressed as data
- ingestion, orchestration, and transforms stay aligned from one source of truth
- adding a dataset is more configuration-heavy than code-heavy

### 2. Factory / Builder Pattern

Airflow task and DAG creation is organized through builder-style helpers.

Evidence:

- `airflow/dags/pipeline_builders.py`
- `airflow/dags/pipeline_factories.py`

What this achieves:

- repeated task wiring is standardized
- command construction stays consistent across datasets
- DAG creation is easier to test than handwritten per-dataset files

### 3. Shared Helper Modules

Spark and app code both separate reusable logic from outer entrypoints.

Evidence:

- `spark/common/io.py`
- `spark/common/quality.py`
- `spark/common/windowing.py`
- `app/data_access_grid.py`
- `app/data_access_summary.py`

What this achieves:

- transform logic is easier to test in isolation
- page code stays more focused on presentation
- common rules are updated once instead of in many files

### 4. Layered Pipeline Separation

The architecture itself is a separation-of-concerns pattern.

Layers:

- ingestion acquires source rows
- Kafka provides the handoff boundary
- Bronze preserves raw replay-safe data
- Silver cleans and validates
- Gold curates canonical records
- Platinum derives business-facing metrics
- Postgres serves stable outputs
- Streamlit presents them

This is not just conceptual. The folder structure and job naming follow that boundary consistently.

## Linting

Linting should be presented as part of the code-quality strategy, even though this repo's strongest current safeguards are tests and runtime validation.

What linting adds:

- catches style drift before code review
- flags simple correctness issues early
- keeps shared modules readable as the project grows
- creates a more consistent standard across `ingestion`, `airflow`, `spark`, and app code

The most practical linting path for this codebase would be:

- `ruff` for fast linting
- `black` for formatting
- optional `mypy` later for stricter type checks on critical modules

That makes the presentation message stronger:

- the project already has strong validation and testing
- linting is the natural next layer to make code quality more consistent

## Practical Strengths

- reusable helpers reduce copy-paste logic
- factory and registry patterns make dataset expansion safer
- validation logic exists at both Spark and Airflow layers
- tests cover ingestion, orchestration, app logic, admin tooling, and Spark

## Practical Gaps

- linting and formatting are not yet enforced in the repo
- no visible type-checking gate such as `mypy`
- no CI-visible quality gate for style violations

## Bottom Line

The repo demonstrates maintainable design through registry-driven configuration, builder-style orchestration, shared helper modules, and layered separation of concerns. Its current strengths are testing and validation. Adding linting would be the clearest next step to make code quality more automatic and more consistent.
