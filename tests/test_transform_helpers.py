from __future__ import annotations

from types import SimpleNamespace

from pipeline.gold import transform as gold_transform
from pipeline.silver import transform as silver_transform


def test_silver_helper_functions(fake_session, monkeypatch) -> None:
    assert silver_transform._full_dataset_id("electricity_generation") == "electricity_generation_hourly"
    assert silver_transform._normalized_partition_date("electricity_retail_sales", "2026-04-19") == "2026-04-01"

    fake_session.tables["DB.SILVER.TABLE"] = SimpleNamespace(schema=SimpleNamespace(names=["A", "B"]), limit=lambda _n: SimpleNamespace(collect=lambda: []))
    monkeypatch.setattr(silver_transform, "table_exists", lambda *args, **kwargs: True)
    assert silver_transform._table_needs_replace(fake_session, "DB.SILVER.TABLE", ["a", "b"]) is False
    assert silver_transform._table_needs_replace(fake_session, "DB.SILVER.TABLE", ["a"]) is True

    silver_transform._delete_partition_if_present(fake_session, "DB.SILVER.TABLE", "2026-04-09")
    assert "delete from DB.SILVER.TABLE where partition_date = '2026-04-09'" in fake_session.sql_calls[-1]

    df = SimpleNamespace(
        schema=SimpleNamespace(names=["period"]),
        count=lambda: 2,
        write=SimpleNamespace(
            mode=lambda mode_name: SimpleNamespace(
                save_as_table=lambda table_name, **kwargs: None
            )
        ),
        with_column=lambda *args, **kwargs: df,
    )
    monkeypatch.setattr(silver_transform, "_table_needs_replace", lambda *args, **kwargs: False)
    monkeypatch.setattr(silver_transform, "_delete_partition_if_present", lambda *args, **kwargs: fake_session.sql_calls.append("deleted"))
    assert silver_transform._write_partitioned_table(fake_session, df, "DB.TABLE", "2026-04-09", ["period"]) == 2


def test_silver_read_and_run_paths(monkeypatch) -> None:
    hourly_df = SimpleNamespace(filter=lambda expr: ("hourly", expr))
    monthly_df = SimpleNamespace(filter=lambda expr: ("monthly", expr))
    session = SimpleNamespace(table=lambda name: hourly_df if "RAW_GEN" in name else monthly_df)

    assert silver_transform.read_raw_for_business_date(session, "electricity_generation", "RAW_GEN", "2026-04-09")[0] == "hourly"
    assert silver_transform.read_raw_for_business_date(session, "electricity_retail_sales", "RAW_MONTH", "2026-04-19")[0] == "monthly"

    dedup_df = SimpleNamespace(schema=SimpleNamespace(names=["period"]), count=lambda: 3)
    monkeypatch.setattr(silver_transform, "read_raw_for_business_date", lambda *args, **kwargs: "raw")
    monkeypatch.setattr(silver_transform, "clean_generation", lambda df: "clean")
    monkeypatch.setattr(silver_transform, "_deduplicate", lambda df, dataset: dedup_df)
    monkeypatch.setattr(silver_transform, "_write_partitioned_table", lambda *args, **kwargs: 3)
    monkeypatch.setattr(silver_transform, "get_raw_table_name", lambda dataset_id: "RAW_GEN")
    monkeypatch.setattr(silver_transform, "get_silver_table_name", lambda dataset_id: "SILVER_GEN")

    assert silver_transform.run_silver(None, "electricity_generation", "2026-04-09", "EIA_PIPELINE") == 3

    raw_df = SimpleNamespace(count=lambda: 5)
    dedup_df2 = SimpleNamespace(schema=SimpleNamespace(names=["period"]), count=lambda: 3)
    monkeypatch.setattr(silver_transform, "read_raw_for_business_date", lambda *args, **kwargs: raw_df)
    monkeypatch.setattr(silver_transform, "clean_retail_sales", lambda df: "clean")
    monkeypatch.setattr(silver_transform, "_deduplicate", lambda df, dataset: dedup_df2)
    monkeypatch.setattr(silver_transform, "_write_partitioned_table", lambda *args, **kwargs: 3)
    results = silver_transform.run_silver_partitions(None, "electricity_retail_sales", ["2026-04-19"], "EIA_PIPELINE")
    assert results == [{"partition_date": "2026-04-01", "rows_read": 5, "rows_written": 3, "duplicates_removed": 2}]


def test_gold_helpers_and_run_paths(monkeypatch) -> None:
    assert gold_transform._silver_table("EIA_PIPELINE", "SILVER_X") == "EIA_PIPELINE.SILVER.SILVER_X"
    assert gold_transform._gold_table("EIA_PIPELINE", "FACT_X") == "EIA_PIPELINE.GOLD.FACT_X"

    fake_pd_df = SimpleNamespace(
        select=lambda *args: fake_pd_df,
        dropna=lambda subset=None: fake_pd_df,
        drop_duplicates=lambda cols: fake_pd_df,
        filter=lambda expr: fake_pd_df,
    )
    assert gold_transform._stabilize_generation(fake_pd_df) is fake_pd_df
    assert gold_transform._stabilize_demand(fake_pd_df) is fake_pd_df
    assert gold_transform._stabilize_monthly_sales(fake_pd_df) is fake_pd_df
    assert gold_transform._stabilize_monthly_ops(fake_pd_df) is fake_pd_df
    monkeypatch.setattr(gold_transform, "lit", lambda value: value)
    monkeypatch.setattr(gold_transform, "sf_round", lambda value, scale: ("round", value, scale))

    class _When:
        def __init__(self, condition, value):
            self.condition = condition
            self.value = value

        def otherwise(self, other):
            return ("when", self.condition, self.value, other)

    monkeypatch.setattr(gold_transform, "when", lambda condition, value: _When(condition, value))
    assert gold_transform._safe_pct(1, 2) == ("when", True, ("round", 50.0, 2), None)

    monkeypatch.setattr(gold_transform, "_build_hourly_generation_partition", lambda *args, **kwargs: {"fact_generation_hourly": 1})
    monkeypatch.setattr(gold_transform, "_build_hourly_demand_partition", lambda *args, **kwargs: {"fact_demand_hourly": 2})
    monkeypatch.setattr(gold_transform, "_build_monthly_operational_sales", lambda *args, **kwargs: {"fact_sales_monthly": 3})
    monkeypatch.setattr(gold_transform, "get_gold_table_names", lambda: {"fact_generation_hourly": "FG", "fact_demand_hourly": "FD", "fact_sales_monthly": "FSM"})
    monkeypatch.setattr(gold_transform, "refresh_gold_dimensions", lambda *args, **kwargs: {"dim_balancing_authority": 2})

    results = gold_transform.run_gold_partitions(None, database="EIA_PIPELINE", target_dates=["2026-04-09"], scope="all")
    assert results["2026-04-09"]["fact_sales_monthly"] == 3
    assert gold_transform.run_gold(None, "2026-04-09", "EIA_PIPELINE", scope="hourly")["fact_generation_hourly"] == 1


def test_gold_dimension_refresh_and_monthly_builder(monkeypatch) -> None:
    dim_frame = SimpleNamespace(
        select=lambda *args: dim_frame,
        drop_duplicates=lambda cols: dim_frame,
        union=lambda other: dim_frame,
    )
    session = SimpleNamespace(table=lambda name: dim_frame)
    monkeypatch.setattr(gold_transform, "table_exists", lambda session, table_name: True)
    monkeypatch.setattr(gold_transform, "_write_dimension_table", lambda df, name: 4)
    monkeypatch.setattr(
        gold_transform,
        "get_gold_table_names",
        lambda: {
            "fact_generation_hourly": "FACT_GENERATION_HOURLY",
            "fact_demand_hourly": "FACT_DEMAND_HOURLY",
            "dim_balancing_authority": "DIM_BALANCING_AUTHORITY",
            "dim_fuel_type": "DIM_FUEL_TYPE",
        },
    )
    refreshed = gold_transform.refresh_gold_dimensions(session, database="EIA_PIPELINE", scope="hourly")
    assert refreshed == {"dim_balancing_authority": 4, "dim_fuel_type": 4}

    empty_frame = SimpleNamespace(filter=lambda expr: empty_frame, count=lambda: 0)
    session2 = SimpleNamespace(table=lambda name: empty_frame)
    monkeypatch.setattr(gold_transform, "table_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr(gold_transform, "_stabilize_monthly_sales", lambda df: empty_frame)
    monkeypatch.setattr(gold_transform, "_stabilize_monthly_ops", lambda df: empty_frame)
    assert gold_transform._build_monthly_operational_sales(
        session2,
        "EIA_PIPELINE",
        "2026-04-19",
        {"fact_sales_monthly": "FACT_SALES_MONTHLY"},
    ) == {}
