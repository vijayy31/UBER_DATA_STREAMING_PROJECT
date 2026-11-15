# Uber Data Engineering Project

A Python-based event streaming project that simulates Uber-like ride activity, publishes lifecycle events to Kafka, consumes them, and stores the resulting JSON payloads in Amazon S3.

The project combines:

- A ride and driver simulation engine
- Kafka producers and consumers
- AWS S3 upload logic
- Docker-based Kafka broker setup
- Event-based data pipeline patterns for experimentation and learning

## Project purpose

This repository models a simplified ride-sharing event pipeline:

1. The simulation creates rider requests, driver assignments, trip start and completion events, payment events, and cancellation events.
2. These events are produced to a Kafka topic.
3. A consumer reads the topic and uploads each event to S3 using an event-driven file layout.
4. The data is organized by event type and date for downstream processing and analytics.

## Architecture overview

```text
Ride / driver simulator
         |
         v
Kafka producer (Confluent Kafka)
         |
         v
Kafka brokers (Docker Compose)
         |
         v
Kafka consumer
         |
         v
Amazon S3 object storage
```

## Tech stack

- Python 3.12+
- Confluent Kafka client
- boto3 for AWS S3
- Databricks / PySpark for Lakehouse transformations
- python-dotenv for environment configuration
- Docker Compose for Kafka brokers
- uv for dependency management

## Repository structure

```text
.
├── .env.example
├── .gitignore
├── .python-version
├── compose.yml
├── docker-commands.txt
├── pyproject.toml
├── uv.lock
├── databricks_pipeline/
│   └── Uber Project/
│       ├── UBER_BRONZE_PIPELINE/
│       │   └── transformations/
│       │       └── 01_Bronze_Ingestion.py
│       ├── UBER_SILVER_PIPELINE/
│       │   ├── transformations/
│       │   │   ├── ride_processing.py
│       │   │   └── payment_processing.py
│       │   └── utilities/
│       │       ├── common.py
│       │       ├── ride_params.py
│       │       └── payment_params.py
│       └── UBER_GOLD_PIPELINE/
│           └── transformations/
│               ├── gold_ride_summary.sql
│               └── gold_payment_summary.sql
├── src/
│   └── uber_de_project/
│       ├── __init__.py
│       ├── aws/
│       │   ├── __init__.py
│       │   ├── s3_client.py
│       │   ├── s3testing.py
│       │   └── utils.py
│       ├── kafka/
│       │   ├── consumer.py
│       │   └── producer.py
│       └── pysim/
│           ├── events_simulator.py
│           ├── final_drive_event_4.txt
│           ├── final_drive_event_5.txt
│           ├── final_ride_event_3.txt
│           ├── final_ride_event_4.txt
│           ├── final_ride_event_5.txt
│           └── utils/
│               ├── __init__.py
│               ├── driver_simulator.py
│               ├── ride_simulator.py
│               └── utils.py
└── test.py
```

## Databricks pipeline overview

The repository also includes a Databricks Lakehouse pipeline under `databricks_pipeline/Uber Project/`. It follows a bronze-silver-gold medallion pattern and is designed to transform the simulated ride events from raw S3/object storage into curated business-ready tables.

### Bronze layer

The bronze ingestion job is defined in `databricks_pipeline/Uber Project/UBER_BRONZE_PIPELINE/transformations/)1_Bronze_Ingestion.py`.

It:

- reads raw binary files from S3 under the bronze bucket (`s3://uber-project-31/bronze`)
- loads files using Databricks `cloudFiles` with the `binaryFile` source
- extracts `raw_payload`, `source_file`, file metadata, and ingestion timestamp
- exposes tables such as:
  - `bronze_ride_events`
  - `bronze_payment_events`

This is the ingestion stage where raw event files are captured as-is before validation and normalization.

### Silver layer

The silver transformations live in:

- `databricks_pipeline/Uber Project/UBER_SILVER_PIPELINE/transformations/ride_processing.py`
- `databricks_pipeline/Uber Project/UBER_SILVER_PIPELINE/transformations/payment_processing.py`

These jobs:

- read from the bronze tables using streaming reads (`spark.readStream.table(...)`)
- parse JSON payloads using typed schemas
- flatten nested structures such as `pickup` and `dropoff`
- normalize key fields like `event_type`, `city`, `currency`, `driver_id`, and timestamps
- validate rows against business rules and attach `validation_status` and `validation_errors`
- split valid rows to the silver tables and invalid rows into quarantine tables

The reusable validation metadata is centralized in:

- `UBER_SILVER_PIPELINE/utilities/common.py`
- `UBER_SILVER_PIPELINE/utilities/ride_params.py`
- `UBER_SILVER_PIPELINE/utilities/payment_params.py`

These files define:

- the expected schemas for ride and payment event payloads
- allowed event types
- mandatory field validations
- domain rules for ride lifecycle events, payment fields, cancellation conditions, and distance/fare checks

### Gold layer

The gold datasets are built with SQL in:

- `databricks_pipeline/Uber Project/UBER_GOLD_PIPELINE/transformations/gold_ride_summary.sql`
- `databricks_pipeline/Uber Project/UBER_GOLD_PIPELINE/transformations/gold_payment_summary.sql`

These create materialized summary views for analytics and reporting:

- `gold_ride_summary`: summarizes each ride by request, assignment, start, completion, cancellation status, and timing metrics like driver assignment delay and trip duration
- `gold_payment_summary`: exposes payment-level metadata such as payment timestamp, amount, method, currency, and payment status

### Databricks pipeline flow

```text
Raw S3 files
   |
   v
Bronze ingestion
   |
   v
Silver validation + normalization
   |
   v
Quarantine invalid rows
   |
   v
Gold summary tables
```

This pipeline complements the local Python/Kafka/S3 prototype by showing how the same event data can be processed in a Lakehouse environment with medallion architecture and SQL-based analytics.

## Key modules

### AWS integration

The AWS logic lives in `src/uber_de_project/aws/`.

- `s3_client.py` creates a boto3 S3 client using credentials loaded from the environment.
- `upload_to_s3(...)` sends JSON events to a configured bucket.
- `utils.py` generates a directory structure such as:

```text
parent_folder/ride_events/year=2026/month=11/day=14/ride_123.json
parent_folder/payment_events/year=2026/month=11/day=14/payment_456.json
```

### Kafka producer and consumer

The Kafka logic lives in `src/uber_de_project/kafka/`.

- `producer.py` publishes test messages to a topic using a Confluent `Producer`.
- `consumer.py` subscribes to a Kafka topic and uploads every consumed event to S3.
- The code expects environment variables for the broker IP and topic name.

### Ride simulation

The simulation logic lives in `src/uber_de_project/pysim/`.

- `events_simulator.py` creates rides, moves drivers, advances ride state transitions, produces events, and writes them to Kafka.
- `ride_simulator.py` builds event payloads such as:
  - `ride_requested`
  - `driver_assigned`
  - `ride_started`
  - `ride_completed`
  - `ride_cancelled`
  - `payment_event`
- `driver_simulator.py` manages driver state, movement, and location events.
- `utils.py` provides helper functions such as UUID generation, timestamps, location generation, and distance calculations.

## Prerequisites

Before running the project, make sure you have:

- Python 3.12 or newer
- Docker and Docker Compose installed
- An AWS account with S3 bucket access
- A Kafka broker or the included Docker Compose setup
- A `.env` file with required credentials

## Environment configuration

Create a `.env` file in the project root using `.env.example` as a template:

```env
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_DEFAULT_REGION=your_aws_region
S3_BUCKET_NAME=your_s3_bucket_name

KAFKA_INSTANCE_IP=your_kafka_instance_ip
KAFKA_TOPIC=uber-events
EC2_PUBLIC_IP=your_ec2_public_ip
KAFKA_CLUSTER_ID=your_kafka_cluster_id
```

Notes:

- `KAFKA_INSTANCE_IP` is used by the Python producer and consumer to connect to the Kafka brokers.
- `KAFKA_TOPIC` is the topic name the simulation publishes to.
- `EC2_PUBLIC_IP` is used in the Docker Kafka brokers configuration when advertising listeners externally.
- `KAFKA_CLUSTER_ID` must match the cluster configuration used by the broker setup.

## Kafka setup with Docker

The project includes a two-broker Kafka cluster configuration in `compose.yml`.

Start Kafka:

```bash
docker compose up -d
```

Check running containers:

```bash
docker compose ps
```

Stop the stack:

```bash
docker compose down -v
```

Useful Kafka admin commands are listed in `docker-commands.txt`, including creating, describing, and deleting the topic.

Example topic creation:

```bash
kafka-topics --bootstrap-server localhost:29092 --create --topic uber-events --partitions 2 --replication-factor 2
```

## Dependency installation

Using uv:

```bash
uv sync
```

Or with pip:

```bash
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e .
```

## Running the simulator

Start the Kafka producer and simulator:

```bash
python src/uber_de_project/pysim/events_simulator.py
```

This script creates simulated ride events and sends them to Kafka.

Run the Kafka consumer:

```bash
python src/uber_de_project/kafka/consumer.py
```

This process subscribes to the configured topic, consumes each event, and uploads it to S3.

## Producing a sample Kafka event

You can also run the simple producer directly:

```bash
python src/uber_de_project/kafka/producer.py
```

This sends a small set of example messages to the configured Kafka topic.

## Example event types

The simulation emits JSON payloads similar to:

- `ride_requested`
- `driver_assigned`
- `ride_started`
- `ride_completed`
- `ride_cancelled`
- `payment_event`
- `driver_location`

These payloads include ride and driver metadata, pickup/drop-off coordinates, fare information, timestamps, and city names.

## Notes and caveats

- The project is primarily an event-pipeline prototype and experimentation setup.
- Kafka broker settings are environment-dependent, especially when running on EC2 or another remote host.
- S3 uploads require valid AWS credentials and a reachable bucket.
- The simulation is designed to run continuously and writes event snapshots to files in the `pysim` directory while it is active.

## License

This project does not include a separate license file. Treat it as a learning and experimentation project unless otherwise specified by the repository owner.
