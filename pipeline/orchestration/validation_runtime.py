from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class DbtValidationError(RuntimeError):
    """Raised when dbt validation fails."""


def _default_project_dir() -> Path:
    return Path(os.environ.get("DBT_PROJECT_DIR") or "/opt/airflow/warehouse/dbt").resolve()


def _default_target() -> str:
    return os.environ.get("DBT_TARGET", "default").strip() or "default"


def _default_dbt_binary() -> str:
    configured = os.environ.get("DBT_BINARY", "").strip()
    if configured:
        return configured
    return "/home/airflow/dbt-venv/bin/dbt"


def _resolve_dbt_binary() -> str | None:
    preferred = _default_dbt_binary()
    preferred_path = Path(preferred)
    if preferred_path.is_file():
        return str(preferred_path)
    return shutil.which(preferred) or shutil.which("dbt")


def _default_threads() -> int:
    raw = os.environ.get("DBT_THREADS", "4").strip() or "4"
    return int(raw)


def _required_dbt_env() -> dict[str, str]:
    values = {
        "account": os.environ.get("DBT_SNOWFLAKE_ACCOUNT", "").strip(),
        "user": os.environ.get("DBT_SNOWFLAKE_USER", "").strip(),
        "password": os.environ.get("DBT_SNOWFLAKE_PASSWORD", "").strip(),
        "role": os.environ.get("DBT_SNOWFLAKE_ROLE", "SYSADMIN").strip() or "SYSADMIN",
        "warehouse": os.environ.get("DBT_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH").strip() or "COMPUTE_WH",
        "database": os.environ.get("DBT_SNOWFLAKE_DATABASE", "EIA_PIPELINE").strip() or "EIA_PIPELINE",
        "schema": os.environ.get("DBT_SNOWFLAKE_SCHEMA", "GOLD").strip() or "GOLD",
    }
    missing = [key for key, value in values.items() if key in {"account", "user", "password"} and not value]
    if missing:
        raise DbtValidationError(f"Missing required DBT Snowflake environment variables: {', '.join(sorted(missing))}")
    return values


def _build_profiles_yml(target_name: str) -> str:
    values = _required_dbt_env()
    threads = _default_threads()
    return (
        "eia_pipeline:\n"
        f"  target: {target_name}\n"
        "  outputs:\n"
        f"    {target_name}:\n"
        "      type: snowflake\n"
        f"      account: {values['account']}\n"
        f"      user: {values['user']}\n"
        f"      password: {values['password']}\n"
        f"      role: {values['role']}\n"
        f"      warehouse: {values['warehouse']}\n"
        f"      database: {values['database']}\n"
        f"      schema: {values['schema']}\n"
        f"      threads: {threads}\n"
    )


def _selector_for_cadence(cadence_group: str) -> str:
    selectors = {
        "hourly": "hourly_full_validation",
        "monthly": "monthly_full_validation",
    }
    if cadence_group not in selectors:
        raise DbtValidationError(f"Unsupported cadence group '{cadence_group}' for dbt validation")
    return selectors[cadence_group]


def _freshness_selector_for_cadence(cadence_group: str) -> str:
    selectors = {
        "hourly": "freshness_hourly_raw",
        "monthly": "freshness_monthly_raw",
    }
    if cadence_group not in selectors:
        raise DbtValidationError(f"Unsupported cadence group '{cadence_group}' for dbt freshness")
    return selectors[cadence_group]


_DATASET_VALIDATION_SELECTORS = {
    "hourly": {
        "electricity_generation_hourly": "hourly_generation_validation",
        "electricity_demand_hourly": "hourly_demand_validation",
    },
    "monthly": {
        "electricity_retail_sales_monthly": "monthly_retail_sales_validation",
        "electricity_power_operational_data_monthly": "monthly_power_operational_validation",
    },
}


_DATASET_FRESHNESS_SELECTORS = {
    "hourly": {
        "electricity_generation_hourly": "freshness_hourly_generation",
        "electricity_demand_hourly": "freshness_hourly_demand",
    },
    "monthly": {
        "electricity_retail_sales_monthly": "freshness_monthly_retail_sales",
        "electricity_power_operational_data_monthly": "freshness_monthly_power_operational",
    },
}


def _datasets_with_planned_work(plan_summary: dict[str, Any]) -> list[str]:
    datasets: list[str] = []
    for dataset_id, plan in plan_summary.get("dataset_plans", {}).items():
        if plan.get("planned_partitions"):
            datasets.append(str(dataset_id))
    return sorted(datasets)


def _selector_args(*, cadence_group: str, planned_datasets: list[str]) -> list[str]:
    if not planned_datasets:
        return ["--selector", _selector_for_cadence(cadence_group)]
    selectors = _DATASET_VALIDATION_SELECTORS.get(cadence_group, {})
    resolved = sorted({selectors[dataset_id] for dataset_id in planned_datasets if dataset_id in selectors})
    if not resolved:
        return ["--selector", _selector_for_cadence(cadence_group)]
    if len(resolved) == 1:
        return ["--selector", resolved[0]]
    args: list[str] = []
    for selector in resolved:
        args.extend(["--selector", selector])
    return args


def _freshness_selector_args(*, cadence_group: str, planned_datasets: list[str]) -> list[str]:
    if not planned_datasets:
        return ["--selector", _freshness_selector_for_cadence(cadence_group)]
    selectors = _DATASET_FRESHNESS_SELECTORS.get(cadence_group, {})
    resolved = sorted({selectors[dataset_id] for dataset_id in planned_datasets if dataset_id in selectors})
    if not resolved:
        return ["--selector", _freshness_selector_for_cadence(cadence_group)]
    if len(resolved) == 1:
        return ["--selector", resolved[0]]
    args: list[str] = []
    for selector in resolved:
        args.extend(["--selector", selector])
    return args


def _partitions_for_plan(plan_summary: dict[str, Any]) -> list[str]:
    partitions: set[str] = set()
    for plan in plan_summary.get("dataset_plans", {}).values():
        for partition in plan.get("planned_partitions", []):
            if partition:
                partitions.add(str(partition))
    return sorted(partitions)


def _build_dbt_vars(plan_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "cadence_group": plan_summary.get("cadence_group", ""),
        "start_date": plan_summary.get("override_start_date", ""),
        "end_date": plan_summary.get("override_end_date", ""),
        "dataset_id": plan_summary.get("selected_dataset", ""),
        "bootstrap_mode": bool(plan_summary.get("bootstrap_mode", False)),
        "partition_dates": _partitions_for_plan(plan_summary),
    }


def _load_json_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_summary(artifact: dict[str, Any], *, artifact_type: str) -> dict[str, Any]:
    if not artifact:
        return {"artifact_type": artifact_type, "status": "missing", "results": []}

    statuses: dict[str, int] = {}
    results: list[dict[str, Any]] = []
    entries = artifact.get("results", [])
    if not entries and artifact_type == "freshness":
        entries = artifact.get("sources", [])

    for item in entries:
        status = str(item.get("status", "unknown")).lower()
        statuses[status] = statuses.get(status, 0) + 1
        results.append(
            {
                "unique_id": item.get("unique_id") or item.get("node", {}).get("unique_id"),
                "status": status,
                "message": item.get("message") or item.get("failures"),
            }
        )

    return {
        "artifact_type": artifact_type,
        "status": "ok",
        "counts": statuses,
        "results": results,
    }


def _run_dbt_command(
    command: list[str],
    *,
    project_dir: Path,
    profiles_dir: Path,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles_dir)
    if extra_env:
        env.update(extra_env)
    logger.info("Running dbt command: %s", " ".join(command))
    completed = subprocess.run(
        command,
        cwd=str(project_dir),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.stdout:
        logger.info("dbt stdout:\n%s", completed.stdout)
    if completed.stderr:
        logger.warning("dbt stderr:\n%s", completed.stderr)
    return completed


def run_dbt_validation(plan_summary: dict[str, Any]) -> dict[str, Any]:
    cadence_group = str(plan_summary.get("cadence_group", "")).strip()
    if not cadence_group:
        raise DbtValidationError("Plan summary is missing cadence_group for dbt validation")

    project_dir = _default_project_dir()
    if not project_dir.exists():
        raise DbtValidationError(f"dbt project directory does not exist: {project_dir}")

    dbt_binary = _resolve_dbt_binary()
    if not dbt_binary:
        raise DbtValidationError("dbt executable not found in PATH")

    target_name = _default_target()
    dbt_vars = _build_dbt_vars(plan_summary)
    planned_datasets = _datasets_with_planned_work(plan_summary)
    summary: dict[str, Any] = {
        "cadence_group": cadence_group,
        "project_dir": str(project_dir),
        "target": target_name,
        "vars": dbt_vars,
        "planned_datasets": planned_datasets,
        "commands": [],
    }
    if not planned_datasets:
        summary["status"] = "skipped"
        summary["reason"] = "no_planned_partitions"
        return summary
    dbt_env = {
        "DBT_CADENCE_GROUP": str(dbt_vars["cadence_group"]),
        "DBT_START_DATE": str(dbt_vars["start_date"]),
        "DBT_END_DATE": str(dbt_vars["end_date"]),
        "DBT_DATASET_ID": str(dbt_vars["dataset_id"]),
        "DBT_BOOTSTRAP_MODE": json.dumps(bool(dbt_vars["bootstrap_mode"])),
        "DBT_PARTITION_DATES_JSON": json.dumps(dbt_vars["partition_dates"], separators=(",", ":")),
    }

    with tempfile.TemporaryDirectory(prefix="dbt-profiles-") as temp_dir:
        profiles_dir = Path(os.environ.get("DBT_PROFILES_DIR") or temp_dir).resolve()
        profiles_dir.mkdir(parents=True, exist_ok=True)
        profiles_file = profiles_dir / "profiles.yml"
        profiles_file.write_text(_build_profiles_yml(target_name), encoding="utf-8")

        deps_command = [dbt_binary, "deps", "--project-dir", str(project_dir), "--profiles-dir", str(profiles_dir)]
        deps_result = _run_dbt_command(
            deps_command,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            extra_env=dbt_env,
        )
        summary["commands"].append({"name": "deps", "returncode": deps_result.returncode})
        if deps_result.returncode != 0:
            raise DbtValidationError("dbt deps failed")

        freshness_command = [
            dbt_binary,
            "source",
            "freshness",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles_dir),
            "--target",
            target_name,
        ]
        freshness_command.extend(_freshness_selector_args(cadence_group=cadence_group, planned_datasets=planned_datasets))
        freshness_result = _run_dbt_command(
            freshness_command,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            extra_env=dbt_env,
        )
        summary["commands"].append({"name": "source_freshness", "returncode": freshness_result.returncode})
        summary["freshness"] = _artifact_summary(
            _load_json_artifact(project_dir / "target" / "sources.json"),
            artifact_type="freshness",
        )
        if freshness_result.returncode != 0:
            raise DbtValidationError("dbt source freshness failed")

        test_command = [
            dbt_binary,
            "test",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles_dir),
            "--target",
            target_name,
        ]
        test_command.extend(_selector_args(cadence_group=cadence_group, planned_datasets=planned_datasets))
        test_result = _run_dbt_command(
            test_command,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            extra_env=dbt_env,
        )
        summary["commands"].append({"name": "test", "returncode": test_result.returncode})
        summary["tests"] = _artifact_summary(
            _load_json_artifact(project_dir / "target" / "run_results.json"),
            artifact_type="tests",
        )
        if test_result.returncode != 0:
            raise DbtValidationError("dbt test failed")

    return summary
