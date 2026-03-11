from jobs.silver_clean_transform import parse_args


def test_parse_args_defaults(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("sys.argv", ["silver_clean_transform.py"])
    args = parse_args()
    assert args.bronze_path == "s3a://bronze/events"
    assert args.silver_base_path == "s3a://silver"
    assert args.dataset is None
    assert args.start is None
    assert args.end is None
