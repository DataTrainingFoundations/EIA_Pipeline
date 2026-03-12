from jobs.gold_region_fuel_serving_hourly import parse_args as parse_gold_args
from jobs.platinum_region_demand_daily import parse_args as parse_platinum_args


def test_parse_gold_args_defaults(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("sys.argv", ["gold_region_fuel_serving_hourly.py"])
    args = parse_gold_args()
    assert args.silver_base_path.startswith("s3a://")
    assert args.region_fact_path.startswith("s3a://gold/facts/")
    assert args.fuel_fact_path.startswith("s3a://gold/facts/")
    assert args.respondent_dim_path.startswith("s3a://gold/dimensions/")
    assert args.fuel_dim_path.startswith("s3a://gold/dimensions/")
    assert args.start is None
    assert args.end is None


def test_parse_platinum_args_defaults(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("sys.argv", ["platinum_region_demand_daily.py"])
    args = parse_platinum_args()
    assert args.gold_input_path.startswith("s3a://gold/")
    assert args.platinum_table.startswith("platinum.")
    assert args.stage_table.startswith("platinum.")
