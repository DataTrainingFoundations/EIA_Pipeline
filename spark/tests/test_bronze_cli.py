from jobs.bronze_kafka_to_minio import _normalize_batch_starting_offsets, parse_args


def test_parse_args_topic_required(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        "sys.argv",
        [
            "bronze_kafka_to_minio.py",
            "--topic",
            "eia_electricity_region_data",
            "--trigger-available-now",
        ],
    )
    args = parse_args()
    assert args.topic == "eia_electricity_region_data"
    assert args.trigger_available_now is True


def test_normalize_batch_starting_offsets_rewrites_latest() -> None:
    assert _normalize_batch_starting_offsets("latest") == "earliest"
    assert _normalize_batch_starting_offsets("LATEST") == "earliest"
    assert _normalize_batch_starting_offsets("earliest") == "earliest"
