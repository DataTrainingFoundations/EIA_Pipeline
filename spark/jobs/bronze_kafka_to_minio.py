# creating the main Spark session
from pyspark.sql import SparkSession

# NEW: used to read environment variables like PIPELINE_MODE
import os

# using this for functions like col(), from_json(), year(), month(), etc.
from pyspark.sql import functions as F

# Import data types used to define the schema of incoming JSON data
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# Kafka broker address
# FIX: corrected variable name from KAFKA_BOOOTSTRAP_SERVERS to KAFKA_BOOTSTRAP_SERVERS
# NEW: first try your existing env name KAFKA_BROKER, then fallback to KAFKA_BOOTSTRAP_SERVERS, then Docker default
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BROKER",
    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
)

# NEW: Kafka topic can also be overridden from environment
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "eia_grid_operations")

# MinIO endpoint URL
# MinIO acts like S3-compatible object storage in Docker
# NEW: read from environment first, fallback to Docker default
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

# MinIO username
# NEW: use MINIO_ROOT_USER because that is what your env currently has
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")

# MinIO password
# NEW: use MINIO_ROOT_PASSWORD because that is what your env currently has
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

# Output path where Bronze data will be stored in MinIO
# s3a:// means Spark will use the S3A connector
BRONZE_OUTPUT_PATH = os.getenv("BRONZE_OUTPUT_PATH", "s3a://bronze/eia/grid_operations/")

# NEW: checkpoint location is also configurable from environment
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "s3a://bronze/checkpoints/eia_grid_operations/"
)

# NEW: pipeline mode is read from environment
# NEW: debug -> print to console, prod -> write parquet to MinIO
# NEW: default is debug if PIPELINE_MODE is not provided
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "debug").lower()

# NEW: startingOffsets can be controlled from env
# NEW: use earliest when you want replay/backfill
# NEW: use latest for normal real-time streaming
# NEW: keeping default as earliest because you already tested old code this way
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest").lower()

# NEW: control micro-batch trigger interval from env
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("BronzeKafkaToMinio")
        # Add external packages/jars needed by Spark
        .config(
            "spark.jars.packages",
            ",".join([
                # Spark-Kafka connector
                # Lets Spark Structured Streaming read from Kafka
                # FIX: changed from Spark 4.1.0 + Scala 2.13 back to Spark 3.5.1 + Scala 2.12
                # NEW: this should match a Spark 3.5.1 cluster much more safely
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",

                # Hadoop AWS package
                # Lets Spark talk to S3/MinIO via s3a://
                # FIX: changed from 3.4.2 back to 3.3.4 to align with Spark 3.5.x setups
                "org.apache.hadoop:hadoop-aws:3.3.4",

                # AWS SDK bundle
                # Required by Hadoop AWS for S3-compatible communication
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ])
        )
        # Set MinIO endpoint so Spark knows where object storage is running
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)

        # Set MinIO access key for authentication
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        
        # Set MinIO secret key for authentication
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        
        # Tell Spark to use path-style access for MinIO
        # MinIO usually works better with this set to true
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        
        # Specify the S3A filesystem implementation class
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # Disable SSL because local MinIO usually runs over HTTP, not HTTPS
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # Create Spark session if not already or return existing one
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# function to define the schema of incoming EIA JSON records
def get_eia_schema() -> StructType:
    
    # Return a StructType schema made of multiple StructField columns
    return StructType([
        # Example: "2025-03-07T00"
        StructField("period", StringType(), True),
        # Example: "AECI"
        StructField("respondent", StringType(), True),
        # respondent full name as string
        StructField("respondent_name", StringType(), True),
        # Example: D, DF, NG, TI
        StructField("type", StringType(), True),
        # Example: Demand, Day-ahead demand forecast
        StructField("type_name", StringType(), True),
        # Example: 2453.0
        StructField("value", DoubleType(), True),
        # Example: megawatthours
        StructField("value_units", StringType(), True),
        # timezone as string
        StructField("timezone", StringType(), True),
        # Example: EIA
        StructField("source", StringType(), True),
        # Example: electricity/rto/region-data
        StructField("dataset", StringType(), True),
        # Example: 2026-03-09T14:03:56.662535+00:00
        StructField("ingested_at", StringType(), True),

        # FIX: changed from raw_records to raw_record so it matches the actual Kafka JSON field name
        # FIX: raw_record is a nested dictionary, so we define it as a StructType instead of StringType
        StructField(
            "raw_record",
            StructType([
                StructField("period", StringType(), True),
                StructField("respondent", StringType(), True),
                StructField("respondent-name", StringType(), True),
                StructField("type", StringType(), True),
                StructField("type-name", StringType(), True),
                StructField("value", StringType(), True),
                StructField("value-units", StringType(), True),
            ]),
            True,
        ),
    ])


# Main function that runs the Bronze streaming pipeline
def main() -> None:
    spark = create_spark_session()
    
    # Get the schema for parsing Kafka JSON messages
    schema = get_eia_schema()

    # NEW: print current runtime config so you can confirm which branch is running
    print(f"Running bronze pipeline in mode: {PIPELINE_MODE}")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"Bronze output path: {BRONZE_OUTPUT_PATH}")
    print(f"Checkpoint location: {CHECKPOINT_LOCATION}")
    print(f"Starting offsets: {STARTING_OFFSETS}")
    print(f"Trigger interval: {TRIGGER_INTERVAL}")

    # NEW: validate PIPELINE_MODE early so typo does not silently go to wrong branch
    if PIPELINE_MODE not in {"debug", "prod"}:
        raise ValueError(
            f"Invalid PIPELINE_MODE='{PIPELINE_MODE}'. Use 'debug' or 'prod'."
        )

    # NEW: validate STARTING_OFFSETS early
    if STARTING_OFFSETS not in {"earliest", "latest"}:
        raise ValueError(
            f"Invalid STARTING_OFFSETS='{STARTING_OFFSETS}'. Use 'earliest' or 'latest'."
        )

    # Create a streaming DataFrame by reading from Kafka
    kafka_df = (
        spark.readStream
        # Tell Spark the source is Kafka
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )

    # Select useful Kafka columns and cast binary key/value into strings
    json_df = kafka_df.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("json_payload"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    # Parse the JSON payload column into a structured column called data
    parsed_df = json_df.select(
        "message_key",
        "json_payload",
        "kafka_timestamp",
        F.from_json(F.col("json_payload"), schema).alias("data"),
    )

    # NEW: keep only records where JSON was parsed successfully
    # NEW: malformed JSON or schema mismatch will result in data = null
    valid_parsed_df = parsed_df.filter(F.col("data").isNotNull())

    # Flatten the parsed nested data column into normal columns
    bronze_df = valid_parsed_df.select(
        "message_key",
        "json_payload",
        "kafka_timestamp",
        # Extract period from parsed JSON
        F.col("data.period").alias("period"),
        F.col("data.respondent").alias("respondent"),
        F.col("data.respondent_name").alias("respondent_name"),
        F.col("data.type").alias("type"),
        F.col("data.type_name").alias("type_name"),
        F.col("data.value").alias("value"),
        F.col("data.value_units").alias("value_units"),
        F.col("data.timezone").alias("timezone"),
        F.col("data.source").alias("source"),
        F.col("data.dataset").alias("dataset"),
        # Extract ingestion timestamp from producer side
        F.col("data.ingested_at").alias("ingested_at"),
        # Extract original raw_record field
        F.col("data.raw_record").alias("raw_record"),

        # NEW: also flatten nested raw_record fields for easier debugging and comparison later
        F.col("data.raw_record.period").alias("raw_period"),
        F.col("data.raw_record.respondent").alias("raw_respondent"),
        # NEW: backticks are needed because these nested field names contain hyphens
        F.col("data.raw_record.`respondent-name`").alias("raw_respondent_name"),
        F.col("data.raw_record.type").alias("raw_type"),
        F.col("data.raw_record.`type-name`").alias("raw_type_name"),
        F.col("data.raw_record.value").alias("raw_value"),
        F.col("data.raw_record.`value-units`").alias("raw_value_units"),
    )

    # adding additional Bronze metadata and partition columns
    bronze_df = (
        bronze_df
        # converting period string into actual timestamp column
        # format matches strings like 2025-03-07T00
        # FIX: to_timestamp needs both the source column and the timestamp format
        .withColumn("period_ts", F.to_timestamp(F.col("period"), "yyyy-MM-dd'T'HH"))
        # Extract year from period_ts for partitioning
        .withColumn("year", F.year("period_ts"))
        .withColumn("month", F.month("period_ts"))
        .withColumn("day", F.dayofmonth("period_ts"))
        # Add current timestamp showing when Bronze layer loaded the row
        .withColumn("bronze_loaded_at", F.current_timestamp())
    )

    # initialize query first so linters know it exists
    query = None

    # NEW: use debug mode for console output and prod mode for MinIO parquet writing
    if PIPELINE_MODE == "debug":
        query = (
            bronze_df.writeStream
            .format("console")
            # NEW: this shows full column values instead of cutting them short
            .option("truncate", False)
            # NEW: limit rows printed per micro-batch so terminal stays readable
            .option("numRows", 10)
            # Append new records only
            .outputMode("append")
            # NEW: process stream every few seconds instead of running continuously as fast as possible
            .trigger(processingTime=TRIGGER_INTERVAL)
            # Start the streaming query
            .start()
        )

    elif PIPELINE_MODE == "prod":
        # Start streaming write to Bronze storage in MinIO
        query = (
            bronze_df.writeStream
            .format("parquet")
            # Path where parquet files will be stored
            .option("path", BRONZE_OUTPUT_PATH)
            # Checkpoint for Spark stream state so Spark remembers progress and offsets
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            # Partition output folders by year/month/day
            .partitionBy("year", "month", "day")
            # Append new records only
            .outputMode("append")
            # NEW: process stream every few seconds
            .trigger(processingTime=TRIGGER_INTERVAL)
            # Start the streaming query
            .start()
        )

    # Keep the streaming job running continuously
    # Without this, the script would exit immediately
    query.awaitTermination()


if __name__ == "__main__":
    main()