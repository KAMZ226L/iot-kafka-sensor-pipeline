"""
Kafka Producer.

Reads sensor CSV files and streams each reading to a Kafka topic,
simulating a real-time IoT data ingestion pipeline.
"""

import csv
import json
import os
import time
import logging
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Producer] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
DATA_DIR = "sensor_stream_data"
STREAM_DELAY = 0.1  # Seconds between messages (simulates real-time)


def create_producer() -> KafkaProducer:
    """Creates and returns a configured Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def stream_csv_files():
    """Reads all CSV files in order and streams each row to Kafka."""
    csv_files = sorted([
        f for f in os.listdir(DATA_DIR) if f.endswith(".csv")
    ])

    if not csv_files:
        logger.error("No CSV files found in '%s/'. Run generate_dataset.py first.", DATA_DIR)
        return

    logger.info("Found %d CSV files to stream", len(csv_files))
    producer = create_producer()
    total_sent = 0

    try:
        for filename in csv_files:
            filepath = os.path.join(DATA_DIR, filename)

            with open(filepath, "r") as f:
                reader = csv.DictReader(f)

                for row in reader:
                    message = {
                        "sensor_id": row["sensor_id"],
                        "temperatura": float(row["temperatura"]),
                        "unitat": row["unitat"],
                        "timestamp": row["timestamp"],
                        "ubicacio": row["ubicacio"],
                    }

                    # Use sensor_id as partition key for ordering
                    producer.send(
                        KAFKA_TOPIC,
                        key=row["sensor_id"],
                        value=message
                    )
                    total_sent += 1

                    if total_sent % 100 == 0:
                        logger.info("Sent %d messages", total_sent)

                    time.sleep(STREAM_DELAY)

        producer.flush()
        logger.info("Streaming complete. Total messages sent: %d", total_sent)

    except KeyboardInterrupt:
        logger.info("Interrupted. Sent %d messages before stopping.", total_sent)
    finally:
        producer.close()


if __name__ == "__main__":
    stream_csv_files()