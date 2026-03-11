from pyspark.sql.types import MapType, StringType, StructField, StructType

KAFKA_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("dataset", StringType(), True),
        StructField("source", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("metadata", MapType(StringType(), StringType(), True), True),
        StructField("payload", MapType(StringType(), StringType(), True), True),
    ]
)
