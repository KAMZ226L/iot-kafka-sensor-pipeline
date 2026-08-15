"""
Kafka Consumer.

Reads sensor data from a Kafka topic in real time, calculates
rolling averages, and triggers alerts when temperatures exceed
configured thresholds per sensor zone.
"""

import json
import os
import logging
from collections import defaultdict, deque
from datetime import datetime
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Consumer] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")

# Temperature thresholds per sensor (min, max in Celsius)
THRESHOLDS = {
    "S001": {"min": 18.0, "max": 28.0},   # Office zone
    "S002": {"min": 10.0, "max": 45.0},   # Server room
    "S003": {"min": 2.0,  "max": 8.0},    # Cold storage
    "S004": {"min": 19.0, "max": 27.0},   # Warehouse
    "S005": {"min": 30.0, "max": 55.0},   # Industrial zone
}

# Rolling window for average calculation
HISTORY_SIZE = 10
history = defaultdict(lambda: deque(maxlen=HISTORY_SIZE))


def calculate_average(sensor_id: str) -> float:
    """Returns the rolling average temperature for a sensor."""
    readings = history[sensor_id]
    if not readings:
        return 0.0
    return round(sum(readings) / len(readings), 2)


def check_alert(sensor_id: str, temperature: float) -> str | None:
    """Returns an alert message if temperature is outside thresholds."""
    if sensor_id not in THRESHOLDS:
        return None

    threshold = THRESHOLDS[sensor_id]
    if temperature < threshold["min"]:
        return f"COLD ALERT: {temperature}°C (min: {threshold['min']}°C)"
    elif temperature > threshold["max"]:
        return f"HEAT ALERT: {temperature}°C (max: {threshold['max']}°C)"
    return None


def main():
    """Starts the consumer and processes incoming sensor data."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="sensor-monitoring-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    logger.info("Consumer started. Waiting for messages on topic '%s'...", KAFKA_TOPIC)
    print(f"\n{'TIME':<12} {'SENSOR':<8} {'TEMP':<10} {'AVG':<10} {'STATUS'}")
    print("-" * 60)

    try:
        for message in consumer:
            data = message.value
            sensor_id = data["sensor_id"]
            temperature = data["temperatura"]

            # Update rolling history
            history[sensor_id].append(temperature)
            avg = calculate_average(sensor_id)

            alert = check_alert(sensor_id, temperature)
            status = alert if alert else "Normal"

            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp:<12} {sensor_id:<8} {temperature:<10} {avg:<10} {status}")

            if alert:
                print(f"  └─ Partition {message.partition} | Offset {message.offset}")

    except KeyboardInterrupt:
        logger.info("Consumer stopped")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()