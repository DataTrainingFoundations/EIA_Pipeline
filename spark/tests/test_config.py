from common.config import load_spark_app_config


def test_load_spark_app_config_defaults() -> None:
    cfg = load_spark_app_config()
    assert cfg.kafka_bootstrap_servers
    assert cfg.bronze_output_path.startswith("s3a://")
    assert cfg.bronze_checkpoint_path.startswith("s3a://")
