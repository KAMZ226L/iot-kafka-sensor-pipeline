# IoT Kafka Sensor Pipeline

A real-time IoT data streaming pipeline that simulates temperature sensors, streams readings through Apache Kafka, and monitors for anomalies with rolling averages and configurable alert thresholds.

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────┐
│ Dataset Generator│     │    Kafka     │     │    Consumer      │
│                 │     │             │     │                 │
│ Generates CSV   │────▶│  Topic:     │────▶│ Rolling averages │
│ sensor readings │     │ sensor-data │     │ Alert detection  │
│ with anomalies  │     │             │     │ Live monitoring  │
└─────────────────┘     └─────────────┘     └─────────────────┘
        │                      ▲
        ▼                      │
┌─────────────────┐            │
│    Producer      │───────────┘
│                 │
│ Reads CSVs and  │
│ streams to Kafka│
└─────────────────┘
```

## Components

### Dataset Generator (`generate_dataset.py`)
Simulates 5 IoT temperature sensors across different zones (office, server room, cold storage, warehouse, industrial). Generates 60 CSV files (one per minute) with readings every 12 seconds. Includes a 3% anomaly rate that injects temperature spikes to test the alerting system.

### Producer (`producer.py`)
Reads the generated CSV files and streams each reading to a Kafka topic as JSON messages. Uses `sensor_id` as the partition key to ensure readings from the same sensor are processed in order.

### Consumer (`consumer.py`)
Listens to the Kafka topic in real time and processes each reading:
- Maintains a **rolling average** (last 10 readings) per sensor
- Checks temperatures against **configurable thresholds** per zone
- Triggers **cold/heat alerts** when readings fall outside safe ranges
- Displays a live monitoring dashboard in the terminal

### Infrastructure (`docker-compose.yml`)
Spins up the full Kafka stack locally:
- **Zookeeper** — cluster coordination
- **Kafka Broker** — message streaming
- **Kafka UI** — web dashboard at `localhost:8080` for inspecting topics, partitions, and messages

## Setup

### Prerequisites
- Docker and Docker Compose
- Python 3.10+

### Installation

```bash
# Clone the repo
git clone https://github.com/KAMZ226L/iot-kafka-sensor-pipeline.git
cd iot-kafka-sensor-pipeline

# Start Kafka infrastructure
docker compose up -d

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt
```

### Run

```bash
# Step 1: Generate sensor data
python generate_dataset.py

# Step 2: Start the consumer (in one terminal)
python consumer.py

# Step 3: Start the producer (in another terminal)
python producer.py
```

The consumer will display a live monitoring table:

```
TIME         SENSOR   TEMP       AVG        STATUS
------------------------------------------------------------
14:23:01     S001     22.34      22.34      Normal
14:23:01     S003     11.45      11.45      HEAT ALERT: 11.45°C (max: 8.0°C)
  └─ Partition 0 | Offset 42
14:23:02     S005     41.87      41.87      Normal
```

You can also monitor messages in the Kafka UI at `http://localhost:8080`.

## Configuration

### Sensor Thresholds
Edit `THRESHOLDS` in `consumer.py` to adjust alert ranges:

```python
THRESHOLDS = {
    "S001": {"min": 18.0, "max": 28.0},  # Office
    "S003": {"min": 2.0,  "max": 8.0},   # Cold storage
}
```

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `sensor-data` | Topic name for sensor readings |

## Tech Stack

- **Python 3.10+**
- **Apache Kafka** — distributed message streaming
- **Docker Compose** — container orchestration
- **kafka-python** — Python Kafka client
- **Confluent Platform** — Kafka and Zookeeper images

## License

MIT
