import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


SPARK_ROOT = Path(__file__).resolve().parents[1]
if str(SPARK_ROOT) not in sys.path:
    sys.path.insert(0, str(SPARK_ROOT))


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("spark-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - environment-specific
        pytest.skip(f"Local Spark session unavailable in this environment: {exc}")

    yield spark
    spark.stop()
