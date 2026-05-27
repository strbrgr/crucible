# Crucible

A simulated IoT sensor data pipeline built in Rust. Sensors produce readings at configurable intervals and stream them to a central gateway over TCP. The gateway publishes readings to Apache Iggy, and a consumer writes them to QuestDB for time-series storage.

## Architecture

```mermaid
flowchart LR
    classDef sensor   fill:#3b82f6,stroke:#1d4ed8,color:#fff
    classDef gateway  fill:#8b5cf6,stroke:#6d28d9,color:#fff
    classDef broker   fill:#f59e0b,stroke:#b45309,color:#fff
    classDef consumer fill:#10b981,stroke:#047857,color:#fff
    classDef db       fill:#ef4444,stroke:#b91c1c,color:#fff

    S1([Sensor\ntemperature]):::sensor -->|TCP :8080| GW
    S2([Sensor\nhumidity]):::sensor    -->|TCP :8080| GW

    GW[Gateway]:::gateway -->|publish :8090| IGG

    subgraph Docker
        IGG[(Apache Iggy)]:::broker
        QDB[(QuestDB)]:::db
    end

    IGG -->|poll| C([Consumer]):::consumer
    C -->|ILP :9009| QDB
```

- **Sensor** — simulates a sensor of a given type, generates randomized readings at a set frequency, and streams them to the gateway using a length-prefixed binary protocol.
- **Gateway** — TCP server that accepts connections from multiple sensors concurrently. Validates incoming payloads and acts as an [Apache Iggy](https://iggy.apache.org) producer, forwarding sensor readings into a message stream.
- **Consumer** — polls the Iggy topic and writes readings to [QuestDB](https://questdb.io) via the InfluxDB line protocol.

## Sensor types

| Type          | Output fields                        | Range            |
|---------------|--------------------------------------|------------------|
| `temperature` | `id`, `value`, `unit`, `created_at`  | 10 °C to 42 °C   |
| `humidity`    | `id`, `value`, `created_at`          | 0% to 99.99%     |

## Prerequisites

- Rust (edition 2024 — toolchain 1.85+)
- Docker + Docker Compose (for Iggy and QuestDB)

## Setup

1. Create a `.env` file in the repo root:

   ```bash
   # Iggy server credentials — used by docker-compose to bootstrap the root user
   # and by the gateway/consumer at runtime via dotenvy
   IGGY_ROOT_USERNAME=iggy
   IGGY_ROOT_PASSWORD=dev-iggy-password

   # Iggy stream/topic topology — shared by gateway and consumer
   IGGY_STREAM_NAME=sample-stream
   IGGY_TOPIC_NAME=sample-topic
   IGGY_PARTITION_ID=0

   # QuestDB connection string — used by the consumer
   QDB_CLIENT_CONF=http::addr=localhost:9000;
   ```

   `.env` is gitignored.

2. Start Iggy and QuestDB:

   ```bash
   docker-compose up -d
   ```

   | Service  | Endpoint                                      |
   |----------|-----------------------------------------------|
   | Iggy TCP | `127.0.0.1:8090`                              |
   | Iggy HTTP API | `127.0.0.1:3000`                         |
   | QuestDB console | [http://localhost:9000](http://localhost:9000) |
   | QuestDB ILP | `127.0.0.1:9009`                          |

## Running

Run each in a separate terminal:

```bash
# 1. Gateway — listens on 127.0.0.1:8080 for producers, forwards to Iggy
cargo run --bin gateway

# 2. Consumer — polls the Iggy topic and writes to QuestDB
cargo run --bin consumer

# 3. Sensors — connect to the gateway and emit readings
# Usage: sensor <sensor_type> <frequency_seconds>
cargo run --bin sensor temperature 5
cargo run --bin sensor humidity 10
```

The gateway batches every 10 sensor readings into a single Iggy publish. The consumer writes each reading to the `temperature` or `humidity` table in QuestDB.

## Resetting state

To wipe all persisted data (Iggy stream history and QuestDB tables):

```bash
docker-compose down -v
docker-compose up -d
```
