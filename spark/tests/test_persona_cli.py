from jobs.platinum_grid_operations_hourly import parse_args as parse_ops_args
from jobs.platinum_resource_planning_daily import parse_args as parse_planning_args


def test_parse_ops_args_defaults(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("sys.argv", ["platinum_grid_operations_hourly.py"])
    args = parse_ops_args()
    assert args.silver_base_path == "s3a://silver"
    assert args.region_fact_path.startswith("s3a://gold/facts/")
    assert args.fuel_fact_path.startswith("s3a://gold/facts/")
    assert args.ops_stage_table.startswith("platinum.")
    assert args.alerts_stage_table.startswith("platinum.")
    assert args.start is None
    assert args.end is None


def test_parse_planning_args_defaults(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("sys.argv", ["platinum_resource_planning_daily.py"])
    args = parse_planning_args()
    assert args.silver_base_path == "s3a://silver"
    assert args.region_fact_path.startswith("s3a://gold/facts/")
    assert args.fuel_fact_path.startswith("s3a://gold/facts/")
    assert args.fuel_dim_path.startswith("s3a://gold/dimensions/")
    assert args.planning_stage_table.startswith("platinum.")
