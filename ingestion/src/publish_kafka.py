"""Kafka publishing boundary for ingestion.

This module is the last ingestion step before data enters the Bronze pipeline.
`fetch_eia.py` builds event envelopes and hands them to these helpers for
delivery to Kafka.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable, Mapping

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


def _json_serializer(value: Mapping[str, Any]) -> bytes:
    """Serialize an event payload into compact UTF-8 JSON bytes."""

    return json.dumps(value, separators=(",", ":"), default=str).encode("utf-8")


def _key_serializer(value: bytes | str) -> bytes:
    """Serialize Kafka keys so event ids are always sent as bytes."""

    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def create_producer(
    broker: str | None = None,
    security_protocol: str | None = None,
) -> KafkaProducer:
    """Create the configured Kafka producer used by ingestion publishing."""

    bootstrap_servers = broker or os.getenv("KAFKA_BROKER", "kafka:9092")
    protocol = security_protocol or os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    logger.info("Creating Kafka producer bootstrap_servers=%s security_protocol=%s", bootstrap_servers, protocol)
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol=protocol,
        key_serializer=_key_serializer,
        value_serializer=_json_serializer,
        acks="all",
        retries=5,
        enable_idempotence=True,
        max_in_flight_requests_per_connection=1,
    )


def publish_events(
    topic: str,
    events: Iterable[Mapping[str, Any]],
    producer: KafkaProducer | None = None,
) -> int:
    """Publish a batch of ingestion events and wait for Kafka acknowledgements.

    Args:
        topic: Kafka topic name from the dataset registry.
        events: Event envelopes ready for Bronze consumption.
        producer: Optional injected producer for tests.

    Returns:
        The number of events sent to Kafka.

    Side effects:
        Produces Kafka messages and closes the producer when this function owns
        it.
    """

    owns_producer = producer is None
    producer = producer or create_producer()
    futures = []
    sent = 0
    logger.info("Publishing Kafka events topic=%s owns_producer=%s", topic, owns_producer)
    try:
        for event in events:
            event_id = event["event_id"]
            futures.append(producer.send(topic, key=event_id, value=dict(event)))
            sent += 1
        for future in futures:
            future.get(timeout=30)
        producer.flush()
    finally:
        if owns_producer:
            producer.close()
    logger.info("Published Kafka events topic=%s sent=%s", topic, sent)
    return sent
