"""
publish_kafka.py
----------------
Kafka producer utility. Serializes records as JSON and publishes
them to a given topic. Used by fetch_eia.py.
"""

import json
import logging
import os
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


def get_producer(broker: str | None = None) -> KafkaProducer:
    """Create and return a KafkaProducer instance."""
    broker = broker or os.environ["KAFKA_BROKER"]
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        retry_backoff_ms=500,
    )
    logger.info("KafkaProducer connected to %s", broker)
    return producer


def publish_records(
    producer: KafkaProducer,
    topic: str,
    records: list[dict[str, Any]],
    key_field: str | None = None,
) -> int:
    """
    Publish a list of record dicts to a Kafka topic.

    Args:
        producer:    An active KafkaProducer.
        topic:       Destination Kafka topic name.
        records:     List of dicts to publish.
        key_field:   Optional dict key whose value becomes the Kafka message key.

    Returns:
        Number of records successfully sent.
    """
    sent = 0
    futures = []

    for record in records:
        key = str(record.get(key_field)) if key_field and key_field in record else None
        future = producer.send(topic, value=record, key=key)
        futures.append(future)

    for future in futures:
        try:
            future.get(timeout=10)
            sent += 1
        except KafkaError as exc:
            logger.error("Failed to publish record: %s", exc)

    producer.flush()
    logger.info("Published %d/%d records to topic '%s'", sent, len(records), topic)
    return sent


def close_producer(producer: KafkaProducer) -> None:
    """Gracefully close the producer."""
    producer.close()
    logger.info("KafkaProducer closed.")