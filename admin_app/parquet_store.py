from __future__ import annotations

import glob
from datetime import timedelta

import duckdb
import pandas as pd
import s3fs

from config import AppConfig
from models import ComparisonRequest
from registry import DatasetRegistryEntry


def _sql_array(values: list[str]) -> str:
    quoted = ["'" + value.replace("'", "''") + "'" for value in values]
    return "[" + ",".join(quoted) + "]"


def _standardize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])
    normalized = df.copy()
    normalized["period_start_utc"] = pd.to_datetime(normalized["period_start_utc"], utc=True, errors="coerce")
    normalized["respondent"] = normalized["respondent"].astype("string")
    normalized["dimension_value"] = normalized["dimension_value"].astype("string")
    normalized = normalized.dropna(subset=["period_start_utc", "respondent"])
    normalized["grain_key"] = (
        normalized["period_start_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        + "|"
        + normalized["respondent"].fillna("")
        + "|"
        + normalized["dimension_value"].fillna("")
    )
    return normalized[["grain_key", "period_start_utc", "respondent", "dimension_value"]]


class ParquetStore:
    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def _connect(self, *, requires_s3: bool) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(database=":memory:")
        if requires_s3:
            try:
                conn.execute("LOAD httpfs")
            except duckdb.Error:
                conn.execute("INSTALL httpfs")
                conn.execute("LOAD httpfs")
            conn.execute(f"SET s3_endpoint='{self._config.minio_endpoint.replace('http://', '').replace('https://', '')}'")
            conn.execute(f"SET s3_access_key_id='{self._config.minio_access_key}'")
            conn.execute(f"SET s3_secret_access_key='{self._config.minio_secret_key}'")
            conn.execute("SET s3_use_ssl=false")
            conn.execute("SET s3_url_style='path'")
        return conn

    def _s3_filesystem(self) -> s3fs.S3FileSystem:
        return s3fs.S3FileSystem(
            key=self._config.minio_access_key,
            secret=self._config.minio_secret_key,
            endpoint_url=self._config.minio_endpoint_with_scheme,
            use_ssl=False,
            client_kwargs={"verify": False},
        )

    def _list_files(self, patterns: list[str]) -> list[str]:
        if patterns and patterns[0].startswith("s3://"):
            fs = self._s3_filesystem()
            matches: list[str] = []
            for pattern in patterns:
                for match in fs.glob(pattern.replace("s3://", "")):
                    matches.append(f"s3://{match}")
            return sorted(set(matches))

        matches: list[str] = []
        for pattern in patterns:
            matches.extend(glob.glob(pattern))
        return sorted(set(matches))

    def _date_patterns(self, base_path: str, request: ComparisonRequest) -> list[str]:
        patterns: list[str] = []
        current = request.start_utc.date()
        while current < request.end_utc.date():
            patterns.append(f"{base_path.rstrip('/')}/event_date={current.isoformat()}/*.parquet")
            current += timedelta(days=1)
        if not patterns:
            patterns.append(f"{base_path.rstrip('/')}/event_date={request.start_utc.date().isoformat()}/*.parquet")
        return patterns

    def _bronze_patterns(self, base_path: str, dataset_id: str, request: ComparisonRequest) -> list[str]:
        patterns: list[str] = []
        current = request.start_utc.date()
        final_day = (request.end_utc - timedelta(hours=1)).date()
        while current <= final_day:
            patterns.append(
                f"{base_path.rstrip('/')}/dataset_partition={dataset_id}"
                f"/event_year={current.year}"
                f"/event_month={current.month}"
                f"/event_day={current.day}"
                "/event_hour=*/*.parquet"
            )
            current += timedelta(days=1)
        return patterns

    def query_stage_keys_from_files(self, request: ComparisonRequest, files: list[str]) -> pd.DataFrame:
        if not files:
            return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])

        conn = self._connect(requires_s3=files[0].startswith("s3://"))
        source = f"read_parquet({_sql_array(files)}, union_by_name=true, hive_partitioning=true)"
        respondent_clause = ""
        if request.respondent_filter:
            escaped = request.respondent_filter.replace("'", "''")
            respondent_clause = f" and respondent = '{escaped}'"

        start_literal = request.start_utc.strftime("%Y-%m-%d %H:%M:%S+00:00")
        end_literal = request.end_utc.strftime("%Y-%m-%d %H:%M:%S+00:00")

        if request.stage == "bronze" and request.dataset_id == "electricity_region_data":
            query = f"""
            select
                date_trunc('hour', event_ts) as period_start_utc,
                json_extract_string(raw_json, '$.payload.respondent') as respondent,
                json_extract_string(raw_json, '$.payload.type') as dimension_value
            from {source}
            where event_ts >= timestamptz '{start_literal}'
              and event_ts < timestamptz '{end_literal}'
              and dataset_partition = 'electricity_region_data'
            """
            return _standardize(conn.execute(query).df().dropna(subset=["dimension_value"]))

        if request.stage == "bronze" and request.dataset_id == "electricity_fuel_type_data":
            query = f"""
            select
                date_trunc('hour', event_ts) as period_start_utc,
                json_extract_string(raw_json, '$.payload.respondent') as respondent,
                json_extract_string(raw_json, '$.payload.fueltype') as dimension_value
            from {source}
            where event_ts >= timestamptz '{start_literal}'
              and event_ts < timestamptz '{end_literal}'
              and dataset_partition = 'electricity_fuel_type_data'
            """
            return _standardize(conn.execute(query).df().dropna(subset=["dimension_value"]))

        if request.stage == "bronze" and request.dataset_id == "electricity_power_operational_data":
            query = f"""
            select
                cast(json_extract_string(raw_json, '$.payload.period') || '-01T00:00:00Z' as timestamptz) as period_start_utc,
                json_extract_string(raw_json, '$.payload.location') as respondent,
                concat(
                    json_extract_string(raw_json, '$.payload.sectorid'),
                    '|',
                    json_extract_string(raw_json, '$.payload.fueltypeid')
                ) as dimension_value
            from {source}
            where event_ts >= timestamptz '{start_literal}'
              and event_ts < timestamptz '{end_literal}'
              and dataset_partition = 'electricity_power_operational_data'
            """
            return _standardize(conn.execute(query).df().dropna(subset=["dimension_value"]))

        if request.stage == "silver" and request.dataset_id == "electricity_region_data":
            query = f"""
            select period as period_start_utc, respondent, type as dimension_value
            from {source}
            where period >= timestamptz '{start_literal}'
              and period < timestamptz '{end_literal}'
              {respondent_clause}
            """
            return _standardize(conn.execute(query).df().dropna(subset=["dimension_value"]))

        if request.stage == "silver" and request.dataset_id == "electricity_fuel_type_data":
            query = f"""
            select period as period_start_utc, respondent, fueltype as dimension_value
            from {source}
            where period >= timestamptz '{start_literal}'
              and period < timestamptz '{end_literal}'
              {respondent_clause}
            """
            return _standardize(conn.execute(query).df().dropna(subset=["dimension_value"]))

        if request.stage == "silver" and request.dataset_id == "electricity_power_operational_data":
            query = f"""
            select period as period_start_utc, location as respondent, concat(sector_id, '|', fueltype_id) as dimension_value
            from {source}
            where period >= timestamptz '{start_literal}'
              and period < timestamptz '{end_literal}'
              {respondent_clause}
            """
            return _standardize(conn.execute(query).df().dropna(subset=["dimension_value"]))

        if request.stage == "gold" and request.dataset_id == "electricity_region_data":
            query = f"""
            select period as period_start_utc, respondent, '' as dimension_value
            from {source}
            where period >= timestamptz '{start_literal}'
              and period < timestamptz '{end_literal}'
              {respondent_clause}
            """
            return _standardize(conn.execute(query).df())

        if request.stage == "gold" and request.dataset_id == "electricity_fuel_type_data":
            query = f"""
            select period as period_start_utc, respondent, fueltype as dimension_value
            from {source}
            where period >= timestamptz '{start_literal}'
              and period < timestamptz '{end_literal}'
              {respondent_clause}
            """
            return _standardize(conn.execute(query).df().dropna(subset=["dimension_value"]))

        if request.stage == "gold" and request.dataset_id == "electricity_power_operational_data":
            query = f"""
            select period as period_start_utc, location as respondent, concat(sector_id, '|', fueltype_id) as dimension_value
            from {source}
            where period >= timestamptz '{start_literal}'
              and period < timestamptz '{end_literal}'
              {respondent_clause}
            """
            return _standardize(conn.execute(query).df().dropna(subset=["dimension_value"]))

        return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])

    def fetch_stage_keys(self, request: ComparisonRequest, dataset: DatasetRegistryEntry) -> pd.DataFrame:
        if request.stage == "bronze":
            patterns = self._bronze_patterns(dataset.bronze_output_path.replace("s3a://", "s3://"), request.dataset_id, request)
            return self.query_stage_keys_from_files(request, self._list_files(patterns))
        if request.stage == "silver":
            patterns = self._date_patterns(dataset.silver_output_path.replace("s3a://", "s3://"), request)
            return self.query_stage_keys_from_files(request, self._list_files(patterns))
        if request.stage == "gold" and dataset.gold_output_path:
            patterns = self._date_patterns(dataset.gold_output_path.replace("s3a://", "s3://"), request)
            return self.query_stage_keys_from_files(request, self._list_files(patterns))
        return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])
