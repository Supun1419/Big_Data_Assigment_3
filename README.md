# Kafka Order Processing System

A Docker Compose project that produces and consumes Avro order messages using
Apache Kafka. It demonstrates real-time price aggregation, exponential retry
handling, and a dead-letter queue (DLQ).

## Architecture

```text
Avro producer ---> orders --------> consumer ---> running averages
                                      |
                                      +-- temporary failure --> orders-retry
                                      |                           |
                                      |                           +--> consumer
                                      |
                                      +-- permanent/exhausted --> orders-dlq
```

Kafka runs as a single combined broker/controller node in KRaft mode. The
producer, consumer, DLQ reader, and tests run in Python containers.

## Order schema

Every order is encoded as Avro binary data using
[`schemas/order.avsc`](schemas/order.avsc):

| Field | Avro type | Meaning |
|---|---|---|
| `orderId` | `string` | Unique order identifier |
| `product` | `string` | Purchased product |
| `price` | `float` | Randomized product price |

The retry and DLQ topics retain the original Avro payload. Failure details are
stored in Kafka headers (`x-retry-count`, `x-error-reason`, and
`x-original-topic`).

## Prerequisite

- Docker Desktop with Linux containers enabled

No local Python, Java, Kafka, or ZooKeeper installation is required.

## Start the system

From PowerShell:

```powershell
cd D:\Big_data\Big_Data_Assignment_3
docker compose up --build -d
docker compose ps
```

Wait until `kafka` is healthy and `consumer` is running. Follow the consumer's
live output in a terminal:

```powershell
docker compose logs -f consumer
```

Press `Ctrl+C` to stop following logs; the containers continue running.

## Produce normal orders

Open a second PowerShell terminal in the project directory:

```powershell
docker compose --profile tools run --rm producer --count 10 --interval 0.3
```

The consumer prints the updated global and per-product average after every
successfully processed order.

## Demonstrate retry and DLQ behavior

Produce a deterministic mixture of normal and failure-demo orders:

```powershell
docker compose --profile tools run --rm producer --count 10 --interval 0.3 --include-failures
```

The special products make the failure paths repeatable during assessment:

| Product | Demonstrated behavior |
|---|---|
| `RetryItem` | Fails twice, then succeeds on its third attempt |
| `InvalidItem` | Permanent validation failure; goes directly to the DLQ |
| `AlwaysFailItem` | Retries three times, then goes to the DLQ |

Retry delays use exponential backoff: 1, 2, and 4 seconds. The consumer commits
the source offset only after an order succeeds or Kafka acknowledges its retry
or DLQ record.

After the retry sequence finishes, inspect the DLQ:

```powershell
docker compose --profile tools run --rm dlq-reader --max-messages 10 --idle-timeout 10
```

## Run automated tests

```powershell
docker compose --profile tools run --build --rm tests
```

The tests cover Avro serialization, schema enforcement, overall and per-product
averages, temporary failures, and permanent failures.

## Useful checks

List topics:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
```

Show recent consumer output:

```powershell
docker compose logs --tail 100 consumer
```

Show the repository history:

```powershell
git log --oneline
```

## Stop or reset

Stop and remove the containers while retaining Kafka data:

```powershell
docker compose down
```

To repeat the demonstration from a completely clean Kafka state, also delete
the named data volume:

```powershell
docker compose down -v
```

`down -v` permanently removes the local Kafka messages and consumer offsets.

## Assignment requirement checklist

- [x] Kafka order producer and consumer
- [x] Avro serialization using the supplied order fields
- [x] Randomized order prices
- [x] Real-time running average of prices
- [x] Retry logic for temporary failures
- [x] Dead Letter Queue for permanent and exhausted failures
- [x] Docker Compose environment
- [x] Automated tests and repeatable live demonstration
- [x] Git commit history
