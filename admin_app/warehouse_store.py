from __future__ import annotations

from contextlib import closing

import pandas as pd
import psycopg2

from config import AppConfig
from models import ComparisonRequest


class WarehouseStore:
    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def _connection(self):
        return psycopg2.connect(
            host=self._config.postgres_host,
            port=self._config.postgres_port,
            dbname=self._config.postgres_db,
            user=self._config.postgres_user,
            password=self._config.postgres_password,
        )

    def read_sql(self, query: str, params: list[object] | tuple[object, ...] | None = None) -> pd.DataFrame:
        with closing(self._connection()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(query, tuple(params or ()))
                rows = cur.fetchall() if cur.description else []
                columns = [description[0] for description in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=columns)

    def fetch_platinum_keys(self, request: ComparisonRequest) -> pd.DataFrame:
        respondent_filter = ""
        params: list[object] = [request.start_utc, request.end_utc]
        if request.respondent_filter:
            respondent_filter = " and respondent = %s"
            params.append(request.respondent_filter)

        if request.dataset_id == "platinum.region_demand_daily":
            query = f"""
            select date::timestamp as period_start_utc, respondent, '' as dimension_value
            from platinum.region_demand_daily
            where date >= %s::date and date < %s::date
            {respondent_filter}
            """
        elif request.dataset_id == "platinum.grid_operations_hourly":
            query = f"""
            select period as period_start_utc, respondent, '' as dimension_value
            from platinum.grid_operations_hourly
            where period >= %s and period < %s
            {respondent_filter}
            """
        elif request.dataset_id == "platinum.resource_planning_daily":
            query = f"""
            select date::timestamp as period_start_utc, respondent, '' as dimension_value
            from platinum.resource_planning_daily
            where date >= %s::date and date < %s::date
            {respondent_filter}
            """
        else:
            return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])

        df = self.read_sql(query, params)
        if df.empty:
            return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])
        df["period_start_utc"] = pd.to_datetime(df["period_start_utc"], utc=True, errors="coerce")
        df["respondent"] = df["respondent"].astype("string")
        df["dimension_value"] = df["dimension_value"].astype("string")
        df["grain_key"] = (
            df["period_start_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            + "|"
            + df["respondent"].fillna("")
            + "|"
            + df["dimension_value"].fillna("")
        )
        return df[["grain_key", "period_start_utc", "respondent", "dimension_value"]]

    def get_platform_counts(self) -> pd.DataFrame:
        query = """
        select 'platinum.region_demand_daily' as target_name, count(*) as row_count from platinum.region_demand_daily
        union all
        select 'platinum.grid_operations_hourly', count(*) from platinum.grid_operations_hourly
        union all
        select 'platinum.resource_planning_daily', count(*) from platinum.resource_planning_daily
        """
        return self.read_sql(query)
