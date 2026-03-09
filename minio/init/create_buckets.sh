#!/bin/sh
# create_buckets.sh
# Called by minio-init container to create all pipeline buckets.
# bronze → raw Kafka JSON
# silver → clean Parquet
# gold   → aggregated Parquet + platinum serving tables

mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

mc mb -p local/bronze
mc mb -p local/silver
mc mb -p local/gold

echo "Buckets created:"
mc ls local