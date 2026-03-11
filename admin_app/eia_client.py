from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

from config import AppConfig
from models import ComparisonRequest
from registry import DatasetRegistryEntry

EIA_API_BASE_URL = "https://api.eia.gov/v2"


def build_event_id(dataset_id: str, route: str, row: dict[str, Any]) -> str:
    payload = json.dumps(
        {"dataset": dataset_id, "route": route, "row": row},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_period(period_value: Any) -> pd.Timestamp | pd.NaT:
    if not isinstance(period_value, str):
        return pd.NaT
    return pd.to_datetime(period_value.replace("Z", "+00:00"), utc=True, errors="coerce")


def _format_eia_hour(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H")


def _apply_facets(params: dict[str, Any], default_facets: dict[str, list[str]]) -> None:
    for facet_name, values in default_facets.items():
        params[f"facets[{facet_name}][]"] = values


def fetch_dataset_rows(
    config: AppConfig,
    dataset: DatasetRegistryEntry,
    request: ComparisonRequest,
    *,
    page_size: int = 5000,
    max_pages: int = 500,
) -> list[dict[str, Any]]:
    if not config.eia_api_key:
        raise RuntimeError("EIA_API_KEY is required for API comparisons.")

    session = requests.Session()
    query_url = f"{EIA_API_BASE_URL}/{dataset.route}/data/"
    offset = 0
    rows: list[dict[str, Any]] = []
    total_rows: int | None = None

    for _ in range(max_pages):
        params: dict[str, Any] = {
            "api_key": config.eia_api_key,
            "frequency": dataset.frequency,
            "start": _format_eia_hour(request.start_utc),
            "end": _format_eia_hour(request.end_utc),
            "offset": offset,
            "length": page_size,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        }
        _apply_facets(params, dataset.default_facets)
        for index, column_name in enumerate(dataset.data_columns or ("value",)):
            params[f"data[{index}]"] = column_name
        if request.respondent_filter:
            params["facets[respondent][]"] = [request.respondent_filter]

        response = session.get(query_url, params=params, timeout=config.query_timeout_seconds)
        response.raise_for_status()
        body = response.json().get("response", {})
        page_rows = body.get("data", [])
        if not page_rows:
            break

        rows.extend(page_rows)
        offset += len(page_rows)
        if body.get("total") is not None:
            total_rows = int(body["total"])
        if total_rows is not None and offset >= total_rows:
            break
        if len(page_rows) < page_size and total_rows is None:
            break

    return rows


def _region_frame(rows: list[dict[str, Any]], dataset: DatasetRegistryEntry) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["event_id", "period_start_utc", "respondent", "respondent_name", "type", "value"])

    return pd.DataFrame.from_records(
        [
            {
                "event_id": build_event_id(dataset.dataset_id, dataset.route, row),
                "period_start_utc": _parse_period(row.get("period")),
                "respondent": row.get("respondent"),
                "respondent_name": row.get("respondent-name"),
                "type": row.get("type"),
                "value": pd.to_numeric(row.get("value"), errors="coerce"),
            }
            for row in rows
        ]
    )


def _fuel_frame(rows: list[dict[str, Any]], dataset: DatasetRegistryEntry) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["event_id", "period_start_utc", "respondent", "respondent_name", "fueltype", "value"])

    return pd.DataFrame.from_records(
        [
            {
                "event_id": build_event_id(dataset.dataset_id, dataset.route, row),
                "period_start_utc": _parse_period(row.get("period")),
                "respondent": row.get("respondent"),
                "respondent_name": row.get("respondent-name"),
                "fueltype": row.get("fueltype"),
                "value": pd.to_numeric(row.get("value"), errors="coerce"),
            }
            for row in rows
        ]
    )


def _standardize_keys(df: pd.DataFrame, dimension_column: str | None = None) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])

    normalized = df.copy()
    normalized["period_start_utc"] = pd.to_datetime(normalized["period_start_utc"], utc=True, errors="coerce")
    normalized["respondent"] = normalized["respondent"].astype("string")
    if dimension_column:
        normalized["dimension_value"] = normalized[dimension_column].astype("string")
    else:
        normalized["dimension_value"] = pd.Series([None] * len(normalized), dtype="string")
    normalized = normalized.dropna(subset=["period_start_utc", "respondent"])
    if dimension_column:
        normalized = normalized.dropna(subset=["dimension_value"])
    normalized["grain_key"] = (
        normalized["period_start_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        + "|"
        + normalized["respondent"].fillna("")
        + "|"
        + normalized["dimension_value"].fillna("")
    )
    return normalized[["grain_key", "period_start_utc", "respondent", "dimension_value"]]


def _build_region_silver(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    return df.dropna(subset=["period_start_utc", "respondent", "value"]).drop_duplicates(subset=["event_id"])


def _build_fuel_silver(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    return df.dropna(subset=["period_start_utc", "respondent", "fueltype", "value"]).drop_duplicates(subset=["event_id"])


def _build_region_gold(df: pd.DataFrame) -> pd.DataFrame:
    silver_df = _build_region_silver(df)
    if silver_df.empty:
        return pd.DataFrame(
            columns=["period_start_utc", "respondent", "respondent_name", "actual_demand_mwh", "day_ahead_forecast_mwh"]
        )
    silver_df = silver_df.sort_values(["period_start_utc", "respondent", "type", "event_id"]).drop_duplicates(
        subset=["period_start_utc", "respondent", "type"],
        keep="last",
    )

    demand_df = (
        silver_df[silver_df["type"] == "D"]
        .groupby(["period_start_utc", "respondent"], dropna=False)
        .agg(respondent_name=("respondent_name", "max"), actual_demand_mwh=("value", "last"))
        .reset_index()
    )
    forecast_df = (
        silver_df[silver_df["type"] == "DF"]
        .groupby(["period_start_utc", "respondent"], dropna=False)
        .agg(forecast_respondent_name=("respondent_name", "max"), day_ahead_forecast_mwh=("value", "last"))
        .reset_index()
    )
    gold_df = demand_df.merge(forecast_df, on=["period_start_utc", "respondent"], how="outer")
    if "respondent_name" not in gold_df.columns:
        gold_df["respondent_name"] = pd.NA
    if "forecast_respondent_name" not in gold_df.columns:
        gold_df["forecast_respondent_name"] = pd.NA
    gold_df["respondent_name"] = (
        gold_df["respondent_name"]
        .fillna(gold_df["forecast_respondent_name"])
        .fillna(gold_df["respondent"])
    )
    return gold_df[["period_start_utc", "respondent", "respondent_name", "actual_demand_mwh", "day_ahead_forecast_mwh"]]


def _build_fuel_gold(df: pd.DataFrame) -> pd.DataFrame:
    silver_df = _build_fuel_silver(df)
    if silver_df.empty:
        return pd.DataFrame(columns=["period_start_utc", "respondent", "respondent_name", "fueltype", "generation_mwh"])
    silver_df = silver_df.sort_values(["period_start_utc", "respondent", "fueltype", "event_id"]).drop_duplicates(
        subset=["period_start_utc", "respondent", "fueltype"],
        keep="last",
    )
    return (
        silver_df.assign(generation_mwh=silver_df["value"].clip(lower=0.0))
        [["period_start_utc", "respondent", "respondent_name", "fueltype", "generation_mwh"]]
        .reset_index(drop=True)
    )


def build_expected_stage_keys(config: AppConfig, request: ComparisonRequest, dataset: DatasetRegistryEntry) -> pd.DataFrame:
    rows = fetch_dataset_rows(config, dataset, request)

    if dataset.dataset_id == "electricity_region_data":
        raw_df = _region_frame(rows, dataset)
        if request.stage == "bronze":
            return _standardize_keys(raw_df.rename(columns={"type": "dimension_value"}), "dimension_value")
        if request.stage == "silver":
            return _standardize_keys(_build_region_silver(raw_df).rename(columns={"type": "dimension_value"}), "dimension_value")
        if request.stage == "gold":
            return _standardize_keys(_build_region_gold(raw_df))
        if request.stage == "platinum" and request.dataset_id in {
            "platinum.region_demand_daily",
            "platinum.grid_operations_hourly",
            "platinum.resource_planning_daily",
        }:
            region_gold_df = _build_region_gold(raw_df)
            if request.dataset_id == "platinum.grid_operations_hourly":
                return _standardize_keys(region_gold_df).drop_duplicates(subset=["grain_key"]).reset_index(drop=True)
            daily_df = region_gold_df[region_gold_df["actual_demand_mwh"].notna()].copy()
            if daily_df.empty:
                return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])
            daily_df["period_start_utc"] = daily_df["period_start_utc"].dt.floor("D")
            return _standardize_keys(daily_df).drop_duplicates(subset=["grain_key"]).reset_index(drop=True)

    if dataset.dataset_id == "electricity_fuel_type_data":
        raw_df = _fuel_frame(rows, dataset)
        if request.stage == "bronze":
            return _standardize_keys(raw_df.rename(columns={"fueltype": "dimension_value"}), "dimension_value")
        if request.stage == "silver":
            return _standardize_keys(_build_fuel_silver(raw_df).rename(columns={"fueltype": "dimension_value"}), "dimension_value")
        if request.stage == "gold":
            return _standardize_keys(_build_fuel_gold(raw_df).rename(columns={"fueltype": "dimension_value"}), "dimension_value")

    return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])
