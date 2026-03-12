from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from config import AppConfig
from eia_client import build_expected_stage_keys
from models import ComparisonRequest, ComparisonResult, ComparisonSummary
from parquet_store import ParquetStore
from registry import dataset_options_for_stage, load_registry
from warehouse_store import WarehouseStore


def supported_dataset_ids(stage: str) -> list[str]:
    return dataset_options_for_stage(stage)


def _empty_key_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])


def _trim_to_window(df: pd.DataFrame, request: ComparisonRequest) -> pd.DataFrame:
    if df.empty:
        return df
    normalized = df.copy()
    normalized["period_start_utc"] = pd.to_datetime(normalized["period_start_utc"], utc=True, errors="coerce")
    return normalized[
        (normalized["period_start_utc"] >= request.start_utc)
        & (normalized["period_start_utc"] < request.end_utc)
    ].reset_index(drop=True)


def run_comparison(
    config: AppConfig,
    request: ComparisonRequest,
    progress_cb: Callable[[str], None] | None = None,
) -> ComparisonResult:
    if progress_cb:
        progress_cb(f"Loading comparison inputs for `{request.stage}` / `{request.dataset_id}`")
    registry = load_registry(str(config.registry_path))
    parquet_store = ParquetStore(config)
    warehouse_store = WarehouseStore(config)

    if request.stage == "platinum":
        if progress_cb:
            progress_cb("Fetching expected API keys")
        expected = build_expected_stage_keys(config, request, registry["electricity_region_data"])
        if progress_cb:
            progress_cb("Reading Platinum keys from Postgres")
        actual = warehouse_store.fetch_platinum_keys(request)
    else:
        dataset = registry[request.dataset_id]
        if progress_cb:
            progress_cb("Fetching expected API keys")
        expected = build_expected_stage_keys(config, request, dataset)
        if progress_cb:
            progress_cb("Reading stage keys from parquet storage")
        actual = parquet_store.fetch_stage_keys(request, dataset)

    expected = _trim_to_window(expected, request) if not expected.empty else _empty_key_frame()
    actual = actual if not actual.empty else _empty_key_frame()

    if progress_cb:
        progress_cb("Building comparison diff")
    expected_unique = expected.drop_duplicates(subset=["grain_key"])
    actual_unique = actual.drop_duplicates(subset=["grain_key"])
    merged = expected_unique.merge(
        actual_unique,
        on="grain_key",
        how="outer",
        indicator=True,
        suffixes=("_expected", "_actual"),
    )

    missing_df = merged[merged["_merge"] == "left_only"].copy()
    missing_df["period_start_utc"] = missing_df["period_start_utc_expected"]
    missing_df["respondent"] = missing_df["respondent_expected"]
    missing_df["dimension_value"] = missing_df["dimension_value_expected"]
    missing_df["reason"] = "missing"

    extra_df = merged[merged["_merge"] == "right_only"].copy()
    extra_df["period_start_utc"] = extra_df["period_start_utc_actual"]
    extra_df["respondent"] = extra_df["respondent_actual"]
    extra_df["dimension_value"] = extra_df["dimension_value_actual"]
    extra_df["reason"] = "extra"

    delta_parts = [
        frame
        for frame in [
            expected.assign(source="expected"),
            actual.assign(source="actual"),
            missing_df[["period_start_utc", "reason"]].rename(columns={"reason": "source"}),
            extra_df[["period_start_utc", "reason"]].rename(columns={"reason": "source"}),
        ]
        if not frame.empty
    ]
    delta_source = pd.concat(delta_parts, ignore_index=True, sort=False) if delta_parts else pd.DataFrame(columns=["period_start_utc", "source"])
    delta_df = (
        delta_source.assign(period_start_utc=pd.to_datetime(delta_source["period_start_utc"], utc=True, errors="coerce"))
        .dropna(subset=["period_start_utc"])
        .groupby(["period_start_utc", "source"], dropna=False)
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .sort_values("period_start_utc")
    )

    summary = ComparisonSummary(
        dataset_id=request.dataset_id,
        stage=request.stage,
        expected_count=int(len(expected)),
        actual_count=int(len(actual)),
        missing_count=int(len(missing_df)),
        extra_count=int(len(extra_df)),
        status=_derive_status(expected, actual, missing_df, extra_df),
    )
    if progress_cb:
        progress_cb(f"Completed `{request.stage}` / `{request.dataset_id}`")
    return ComparisonResult(
        summary=summary,
        missing_df=missing_df[["grain_key", "period_start_utc", "respondent", "dimension_value", "reason"]],
        extra_df=extra_df[["grain_key", "period_start_utc", "respondent", "dimension_value", "reason"]],
        delta_df=delta_df,
        expected_df=expected,
        actual_df=actual,
    )


def _derive_status(expected: pd.DataFrame, actual: pd.DataFrame, missing_df: pd.DataFrame, extra_df: pd.DataFrame) -> str:
    if len(expected) == 0 and len(actual) == 0:
        return "not_populated"
    if len(missing_df) == 0 and len(extra_df) == 0 and len(expected) == len(actual):
        return "ok"
    if len(missing_df) == 0 and len(extra_df) == 0:
        return "count_mismatch"
    return "mismatch"


def build_default_snapshot(config: AppConfig, start_utc, end_utc) -> pd.DataFrame:
    rows = []
    for stage in ("bronze", "silver", "gold"):
        for dataset_id in supported_dataset_ids(stage):
            result = run_comparison(
                config,
                ComparisonRequest(dataset_id=dataset_id, stage=stage, start_utc=start_utc, end_utc=end_utc),
            )
            rows.append(
                {
                    "stage": stage,
                    "dataset_id": dataset_id,
                    "status": result.summary.status,
                    "expected_count": result.summary.expected_count,
                    "actual_count": result.summary.actual_count,
                    "missing_count": result.summary.missing_count,
                    "extra_count": result.summary.extra_count,
                }
            )
    for dataset_id in supported_dataset_ids("platinum"):
        result = run_comparison(
            config,
            ComparisonRequest(dataset_id=dataset_id, stage="platinum", start_utc=start_utc, end_utc=end_utc),
        )
        rows.append(
            {
                "stage": "platinum",
                "dataset_id": dataset_id,
                "status": result.summary.status,
                "expected_count": result.summary.expected_count,
                "actual_count": result.summary.actual_count,
                "missing_count": result.summary.missing_count,
                "extra_count": result.summary.extra_count,
            }
        )
    return pd.DataFrame.from_records(rows)
