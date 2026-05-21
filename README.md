# Crucible

A simulated IoT sensor data pipeline built in Rust. Sensors produce readings at configurable intervals and stream them to a central gateway over TCP. The gateway then publishes those readings to consumers via Apache Iggy.

## Architecture

```mermaid
flowchart LR
    classDef sensor   fill:#3b82f6,stroke:#1d4ed8,color:#fff
    classDef gateway  fill:#8b5cf6,stroke:#6d28d9,color:#fff
    classDef broker   fill:#f59e0b,stroke:#b45309,color:#fff
    classDef consumer fill:#10b981,stroke:#047857,color:#fff

    S1([Sensor\ntemperature]):::sensor -->|TCP :8080| GW
    S2([Sensor\nhumidity]):::sensor    -->|TCP :8080| GW

    GW[Gateway]:::gateway -->|publish :8090| IGG

    subgraph Docker
        IGG[(Apache Iggy)]:::broker
    end

    IGG -->|poll| C([Consumer]):::consumer
```

- **Sensor** — simulates a sensor of a given type, generates randomized readings at a set frequency, and streams them to the gateway using a length-prefixed binary protocol.
- **Gateway** — TCP server that accepts connections from multiple sensors concurrently. Acts as an [Apache Iggy](https://iggy.apache.org) producer, forwarding sensor readings into a message stream.
- **Consumer** — subscribes to the Iggy topic and prints incoming messages.

## Sensor types

| Type          | Output fields                  | Range            |
|---------------|--------------------------------|------------------|
| `temperature` | `id`, `temp`, `unit`           | -10 °C to 42 °C  |
| `humidity`    | `id`, `humidity`               | 0% to 99.99%     |

## Prerequisites

- Rust (edition 2024 — toolchain 1.85+)
- Docker + Docker Compose (for the Iggy server)

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
   ```

   `.env` is gitignored.

2. Start the Iggy server:

   ```bash
   docker-compose up -d
   ```

   This exposes TCP on `127.0.0.1:8090` and the HTTP API on `127.0.0.1:3000`.

## Running

Run each in a separate terminal:

```bash
# 1. Gateway — listens on 127.0.0.1:8080 for producers, forwards to Iggy
cargo run --bin gateway

# 2. Consumer — polls the Iggy topic and logs messages
cargo run --bin consumer

# 3. Sensors — connect to the gateway and emit readings
# Usage: sensor <sensor_type> <frequency_seconds>
cargo run --bin sensor temperature 5
cargo run --bin sensor humidity 10
```

The gateway batches every 10 sensor readings into a single Iggy publish.

## Resetting state

To wipe the Iggy data volume (e.g. to re-bootstrap with a new root password):

```bash
docker-compose down -v
docker-compose up -d
```
