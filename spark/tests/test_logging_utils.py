from common.io import partitioned_parquet_glob
from common.logging_utils import format_log_fields


def test_partitioned_parquet_glob_defaults_to_event_date() -> None:
    assert (
        partitioned_parquet_glob("s3a://gold/facts/region")
        == "s3a://gold/facts/region/event_date=*/*.parquet"
    )


def test_format_log_fields_omits_empty_values() -> None:
    assert (
        format_log_fields(
            job_name="silver_clean_transform",
            dataset_id="electricity_region_data",
            start=None,
        )
        == "job_name=silver_clean_transform dataset_id=electricity_region_data"
    )
