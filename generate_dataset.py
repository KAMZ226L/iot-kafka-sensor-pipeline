"""
Sensor Data Generator.

Generates simulated IoT temperature sensor data as CSV files.
Each file represents one minute of readings from 5 sensors,
with a 3% chance of anomalous temperature spikes.
"""

import csv
import os
import random
from datetime import datetime, timedelta

OUTPUT_DIR = "sensor_stream_data"

SENSORS = ["S001", "S002", "S003", "S004", "S005"]

BASELINE_TEMPS = {
    "S001": 22.0,  # Office zone
    "S002": 35.0,  # Server room
    "S003": 5.0,   # Cold storage
    "S004": 25.0,  # Warehouse
    "S005": 40.0,  # Industrial zone
}

ANOMALY_RATE = 0.03
ANOMALY_SPIKE = (5, 10)
READING_INTERVAL_SECONDS = 12
DURATION_MINUTES = 60


def generate_dataset():
    """Generates CSV files with simulated sensor readings."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    start_time = datetime(2026, 1, 15, 8, 0, 0)
    total_readings = 0

    for minute in range(DURATION_MINUTES):
        minute_timestamp = start_time + timedelta(minutes=minute)
        filename = os.path.join(
            OUTPUT_DIR,
            f"sensors_{minute_timestamp.strftime('%H%M')}.csv"
        )

        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["sensor_id", "temperatura", "unitat", "timestamp", "ubicacio"])

            for second in range(0, 60, READING_INTERVAL_SECONDS):
                ts = minute_timestamp + timedelta(seconds=second)

                for sensor_id in SENSORS:
                    base = BASELINE_TEMPS[sensor_id]
                    temp = base + random.uniform(-2.0, 2.0)

                    # Inject anomalies at configured rate
                    if random.random() < ANOMALY_RATE:
                        temp += random.uniform(*ANOMALY_SPIKE)

                    writer.writerow([
                        sensor_id,
                        round(temp, 2),
                        "Celsius",
                        ts.isoformat(),
                        f"Zona_{sensor_id[-1]}"
                    ])
                    total_readings += 1

    print(f"Generated {DURATION_MINUTES} CSV files with {total_readings} readings in '{OUTPUT_DIR}/'")


if __name__ == "__main__":
    generate_dataset()
    
