# Import Python's built-in JSON library
# We use this to convert Python dictionaries into JSON strings
# because Kafka sends messages as bytes
import json

# Import the os module so we can read environment variables
# Example: reading KAFKA_BROKER from .env
import os

# Used only for type hints
# "Any" means a variable could be any data type
# The typing module is used for type hints.
# Type hints tell the reader (and tools) what type of data a variable or function should use.

from typing import Any

# Import KafkaProducer from kafka-python library
# This class allows Python to send messages to Kafka topics
from kafka import KafkaProducer

# Import the function we wrote earlier that fetches and normalizes EIA records
# This connects our API ingestion script to the Kafka publishing script
from fetch_eia import fetch_eia_records

# Define the Kafka topic name where we will publish the data
# Topics are like channels or streams in Kafka
TOPIC_NAME = "eia_grid_operations"


def get_kafka_broker() -> str:
    """
    Read Kafka broker address from environment.
    """

    # Read the KAFKA_BROKER environment variable
    # If it does not exist, default to "kafka:9092"
    # "kafka" is the container hostname inside Docker network
    return os.getenv("KAFKA_BROKER", "kafka:9092")


def create_producer() -> KafkaProducer:
    """
    Create Kafka producer with JSON serializer.
    """

    # Create and return a KafkaProducer object
    return KafkaProducer(

        # Address of Kafka broker to connect to
        bootstrap_servers = get_kafka_broker(),

        # Convert Python dictionary values into JSON strings
        # then encode them into bytes (Kafka requires bytes)
        value_serializer = lambda value: json.dumps(value).encode("utf-8"),

        # Convert message keys into UTF-8 encoded bytes
        # If key is None, Kafka will ignore the key
        key_serializer = lambda key: key.encode("utf-8") if key else None,
    )


def build_message_key(record: dict[str, Any]) -> str:
    """
    Build a stable Kafka message key.
    This helps keep similar records grouped by key if needed.
    """

    # Extract the timestamp of the data
    period = record.get("period", "unknown_period")

    # Extract the grid region / balancing authority
    respondent = record.get("respondent", "unknown_respondent")

    # Extract the metric type (demand, forecast, generation, etc)
    metric_type = record.get("type", "unknown_type")

    # Combine fields into a unique key string
    # Example: "PJM|D|2025-03-05T23"
    return f"{respondent}|{metric_type}|{period}"


def publish_records(
    topic: str = TOPIC_NAME,
    start: str = "2025-03-05T00",
    end: str = "2025-03-07T00",
    offset: int = 0,
    length: int = 5000,
) -> int:
    """
    Fetch records from EIA and publish them to Kafka.
    Returns number of published records.
    """

    # Create Kafka producer connection
    producer = create_producer()

    # Fetch normalized records from EIA API using the function from fetch_eia.py
    records = fetch_eia_records(
        start=start,
        end=end,
        offset=offset,
        length=length,
    )

    # Initialize counter to track how many messages were sent
    published_count = 0

    # Loop through each record returned from the API
    for record in records:

        # Generate a unique key for this message
        message_key = build_message_key(record)

        # Send message to Kafka
        producer.send(

            # Kafka topic where message will be stored
            topic=topic,

            # Message key used for partitioning/grouping
            key=message_key,

            # Actual message data (Python dictionary)
            value=record,
        )

        # Increase message counter
        published_count += 1

    # Ensure all buffered messages are sent to Kafka
    producer.flush()

    # Close the producer connection cleanly
    producer.close()

    # Print how many messages were sent
    print(f"Published{published_count} record to Kafka topic '{topic}'")

    # Return number of published records
    return published_count


# This block runs only when the script is executed directly
# Example: python publish_kafka.py
if __name__ == "__main__":

    # Call the publish_records function
    publish_records()