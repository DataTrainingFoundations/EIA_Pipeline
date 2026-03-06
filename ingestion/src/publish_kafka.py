from kafka import KafkaConsumer
from minio import Minio
import json
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import io
import os

load_dotenv()
# Kafka configuration
KAFKA_TOPIC = "eia_energy"
KAFKA_BROKER = os.getenv('KAFKA_BROKER')

# MinIO configuration
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = "bronze"

spark = SparkSession.builder \
    .appName("EIABatchConsumer") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="eia-consumers"
)


minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

# Create bucket if it doesn't exist
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

print("Consumer running...")

for message in consumer:
    batch = message.value  # This is a list of dicts
    if not batch:
        continue

    # Convert list of dicts to Spark DataFrame
    df = spark.createDataFrame(batch)

    # Optional: you can enforce column types
    df = df.withColumn("value", df["value"].cast("double"))

    # Build path in MinIO (partition by date)
    dt_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_path = f"s3a://{BUCKET_NAME}/eia/{dt_str}/"

    # Write batch as Parquet
    df.write.mode("append").parquet(s3_path)

    print(f"Uploaded batch of {len(batch)} records to {s3_path}")
