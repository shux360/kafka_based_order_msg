# Kafka Avro Order Processing Assignment

This project implements an order producer and consumer using Apache Kafka and Avro. It provides real-time running-price aggregation, exponential-backoff retries for temporary failures, and a Dead Letter Queue (DLQ) for permanent failures or exhausted retries.

## Architecture

```text
Python producer --Avro--> orders topic --Avro--> consumer --> running average
                                             |       |
                                             |       +-- temporary error: retry 3 times
                                             +---------- permanent/exhausted: orders-dlq
```

Kafka runs in single-node KRaft mode for a lightweight local demonstration. Schema Registry stores and validates both `order.avsc` and `dlq.avsc`. The consumer disables auto-commit and commits an offset only after processing succeeds or the corresponding DLQ record is confirmed delivered.

## Requirements

- Docker Desktop with Docker Compose
- Python 3.9 or newer

## Run the live demonstration

Open PowerShell in this directory.

```powershell
docker compose up -d
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m order_system.consumer
```

In a second PowerShell window, activate the same environment and produce ten messages. The final message deliberately fails and is sent to the DLQ:

```powershell
.\.venv\Scripts\Activate.ps1
python -m order_system.producer --count 10 --interval 0.5 --include-failure
```

The consumer prints the successful order count and updated running average after every valid message. It prints a `DLQ:` record for the deliberately failed message.

To visibly demonstrate retry behavior, restart the consumer with a 50% simulated temporary downstream-failure rate and short backoff:

```powershell
$env:TRANSIENT_FAILURE_RATE="0.5"
$env:RETRY_BACKOFF_SECONDS="0.25"
python -m order_system.consumer
```

If all retry attempts fail, the order goes to `orders-dlq`. Clear those environment variables after the demo if desired.

## Inspect Kafka and Schema Registry

List topics and consume the DLQ from inside the Kafka container:

```powershell
docker exec order-kafka kafka-topics --bootstrap-server kafka:29092 --list
docker exec order-kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic orders-dlq --from-beginning --property print.key=true
```

The console consumer displays Confluent's binary Avro wire format. To verify registered schemas in a readable form:

```powershell
Invoke-RestMethod http://localhost:8081/subjects
Invoke-RestMethod http://localhost:8081/subjects/orders-value/versions/latest
Invoke-RestMethod http://localhost:8081/subjects/orders-dlq-value/versions/latest
```

## Tests

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

The unit tests cover aggregation, exponential retry timing, exhausted retries, and permanent validation failures without requiring a running Kafka broker.

## Configuration

| Environment variable | Default | Purpose |
|---|---:|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry URL |
| `ORDERS_TOPIC` | `orders` | Source topic |
| `DLQ_TOPIC` | `orders-dlq` | Failed-message topic |
| `CONSUMER_GROUP` | `order-processor-v1` | Consumer group |
| `MAX_RETRIES` | `3` | Retries after the first attempt |
| `RETRY_BACKOFF_SECONDS` | `1` | Initial exponential delay |
| `TRANSIENT_FAILURE_RATE` | `0` | Demo-only failure probability from 0 to 1 |

## Submission checklist

```powershell
git add .
git commit -m "Implement Kafka Avro order processing assignment"
git log --oneline
```

- Show the producer creating Avro orders.
- Show the consumer's running average.
- Show retry attempts by using `TRANSIENT_FAILURE_RATE`.
- Show a permanent failure entering the DLQ with `--include-failure`.
- Show the registered schemas and Git history.

Stop and remove the local containers with `docker compose down`.
