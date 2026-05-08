# Message Queue Benchmarking

A distributed benchmarking platform for comparing **Apache Kafka**, **RabbitMQ**, and **PGMQ** across throughput and end-to-end latency, using three communication patterns.

## Overview

This platform measures how different message brokers perform under controlled conditions. It uses a centralized orchestrator coordinating multiple worker processes over SignalR, allowing scalable distributed tests with precise timestamp-based latency measurement.

Brokers are constrained to fixed resources (CPU/memory) while workers run unconstrained, ensuring the benchmark measures broker performance rather than client-side bottlenecks.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Orchestrator                       │
│         (ASP.NET + SignalR Hub + REST API)           │
│   - Assigns roles (Producer/Consumer/Janitor)       │
│   - Generates routing plans & group assignments     │
│   - Aggregates timestamps → computes results        │
└──────────────┬──────────────────────┬───────────────┘
               │ SignalR              │ SignalR
       ┌───────▼───────┐     ┌───────▼───────┐
       │   Worker (P)  │     │   Worker (C)  │
       │   Producer    │     │   Consumer    │
       └───────┬───────┘     └───────┬───────┘
               │                     │
               └──────► Broker ◄─────┘
                  (Kafka/RabbitMQ/PGMQ)
```

The orchestrator is **broker-agnostic** — it generates logical targets like `"group_0"`, `"group_1"`. Each broker implementation translates these to concrete infrastructure (topics, queues, exchanges, routing keys) via static naming classes (`KafkaNaming`, `RabbitMqNaming`, `PgMqNaming`). Infrastructure names are never specified in config.

### Test Lifecycle

1. **Initialize** — HTTP `POST /api/benchmarking/initialize` with test config JSON
2. **Janitor** — One worker prepares broker infrastructure (create/purge topics, queues, exchanges)
3. **Role Assignment** — Orchestrator assigns remaining workers as producers or consumers, sends configs (including routing plans for producers and group names for consumers)
4. **Start** — HTTP `POST /api/benchmarking/start` triggers `StartTest` signal
5. **Produce** — Producers send messages according to routing plan, round-robin across targets (optionally rate-limited), recording send timestamps
6. **ProducersDone** — Orchestrator signals consumers that all messages have been sent
7. **Drain** — Consumers continue receiving until idle timeout expires (`consumerIdleTimeoutSeconds`)
8. **Results** — Workers submit compressed timestamp batches; orchestrator computes per-group metrics and returns CSV

## Communication Modes

|                  | PointToPoint | PubSub | Streaming |
|------------------|:---:|:---:|:---:|
| **Kafka**        | ✓ | ✓ | ✓ |
| **RabbitMQ**     | ✓ | ✓ | ✓ |
| **PGMQ**         | ✓ | ✓ | ✓ |

### PointToPoint

Each consumer group receives a **distinct subset** of messages. The producer round-robins messages across groups according to a routing plan.

- Use `messagesPerConsumerGroup` to specify how many messages each group receives (e.g., `[5000, 3000]`). If omitted, `messageCount` is divided equally across groups.
- Consumer groups have **competing consumers** — multiple consumers within a group share the work.

| Broker | Implementation |
|--------|----------------|
| Kafka | Separate topic per group (`benchmark_group_0`, `benchmark_group_1`). Consumers in a group share a consumer group ID. |
| RabbitMQ | Topic exchange (`benchmark_topic`) with routing keys per group. Each group binds its queue to its routing key. |
| PGMQ | Topic routing per group. Each group has its own queue with a routing key subscription. |

### PubSub

Every message is delivered to **all consumer groups**. Each group independently receives the full message stream.

- Use `messageCount` to specify total messages produced (each group receives all of them).
- Consumer groups have **competing consumers** within each group.

| Broker | Implementation |
|--------|----------------|
| Kafka | Shared topic (`benchmark`). Each group gets a unique `group.id`, so Kafka delivers all messages to every group independently. |
| RabbitMQ | Fanout exchange (`benchmark_fanout`). Each group has its own queue bound to the exchange. |
| PGMQ | Broadcast routing key. All group queues subscribe to the broadcast key, each receiving a copy. |

### Streaming

Each consumer group independently reads from the **same persistent log** using offset-based consumption.

- Use `messageCount` to specify total messages produced (each group reads all of them).
- **RabbitMQ and PGMQ**: Only 1 consumer per group is allowed (no competing consumers on streams). Configs with >1 consumer per group are rejected at validation.
- **Kafka**: Multiple consumers per group are allowed (Kafka natively supports competing consumers on partitions).

| Broker | Implementation |
|--------|----------------|
| Kafka | Shared topic with unique `group.id` per group. Standard consumer group semantics with partition assignment. |
| RabbitMQ | Stream queue (`benchmark_stream`). Each consumer reads independently with its own offset. |
| PGMQ | Stream queue (`benchmark_stream`). Offset-based reads — each consumer tracks its own read position. |

## Latency Measurement

Timestamps are recorded in the worker process:

- **Producer**: `DateTime.UtcNow.Ticks` captured **before** `SendAsync` — includes any buffering/batching time, which reflects real application-perceived latency
- **Consumer**: `DateTime.UtcNow.Ticks` captured in the message handler upon receipt

End-to-end latency = consumer timestamp − producer timestamp. This measures the full path: client buffer → broker → delivery to consumer.

Results are aggregated **per consumer group**, with accurate `messagesLost` calculation based on expected messages for each group (not total sent).

## Configuration

Test configs are JSON files passed to the `/api/benchmarking/initialize` endpoint. Infrastructure names (queue names, exchange names, topic names, group IDs, routing keys) are **auto-generated** — only connection details and performance tuning knobs are configured.

### PointToPoint Example

```json
{
  "producersCount": 1,
  "consumerGroups": [2, 1],
  "communicationMode": "PointToPoint",
  "messagesPerConsumerGroup": [5000, 3000],
  "messageSizeInBytes": 256,
  "sendFrequencyMps": 5000,
  "consumerIdleTimeoutSeconds": 15,
  "mqConfig": {
    "implementation": "Kafka",
    "additionalSettings": {
      "BootstrapServers": "kafka:9092",
      "Acks": "Leader",
      "LingerMs": "5",
      "BatchSize": "65536",
      "UseBufferedProducer": "true",
      "NumPartitions": "2"
    }
  }
}
```

### PubSub / Streaming Example

```json
{
  "producersCount": 2,
  "consumerGroups": [2, 2],
  "communicationMode": "PubSub",
  "messageCount": 10000,
  "messageSizeInBytes": 256,
  "sendFrequencyMps": 5000,
  "consumerIdleTimeoutSeconds": 15,
  "mqConfig": {
    "implementation": "RabbitMQ",
    "additionalSettings": {
      "Hostname": "rabbitmq",
      "Port": "5672",
      "Username": "guest",
      "Password": "guest",
      "PrefetchCount": "200"
    }
  }
}
```

### Top-Level Fields

| Field | Description |
|-------|-------------|
| `producersCount` | Number of workers assigned as producers |
| `consumerGroups` | Array where each element is the consumer count for that group, e.g. `[2, 3]` |
| `communicationMode` | `PointToPoint`, `PubSub`, or `Streaming` |
| `messageCount` | Total messages to produce (used for PubSub/Streaming; optional for PointToPoint if `messagesPerConsumerGroup` is set) |
| `messagesPerConsumerGroup` | Per-group message counts for PointToPoint (e.g. `[5000, 3000]`). If omitted, `messageCount` is split equally. Ignored for PubSub/Streaming. |
| `messageSizeInBytes` | Payload size per message |
| `sendFrequencyMps` | Optional rate limit in messages/second (omit for max throughput) |
| `consumerIdleTimeoutSeconds` | Seconds of inactivity before consumer stops (default 15) |

### Broker-Specific Settings (`additionalSettings`)

**Kafka**: `BootstrapServers`, `Acks`, `LingerMs`, `BatchSize`, `UseBufferedProducer`, `EnableIdempotence`, `EnableAutoCommit`, `NumPartitions`, `AutoOffsetReset`

**RabbitMQ**: `Hostname`, `Port`, `Username`, `Password`, `DurableMode`, `PrefetchCount`, `ConsumerDispatchConcurrency`, `PublisherConfirms`

**PGMQ**: `ConnectionString`, `VisibilityTimeout`, `QueueMode`, `MessageReadMode`, `ConsumerMode`, `PollIntervalMs`, `MaxPollSeconds`, `UsePop`, `UseBufferedProducer`, `ProducerBatchSize`, `ProducerLingerMs`, `ConsumerBatchSize`

## Running

### Prerequisites

- Docker & Docker Compose

### Start a Benchmark

```bash
# Start infrastructure + workers (e.g. Kafka with 6 workers)
docker compose --profile kafka up --build --scale worker=6

# Initialize a test
curl -X POST http://localhost:8080/api/benchmarking/initialize \
  -H "Content-Type: application/json" \
  -d @examples/kafka.json

# Run the test (blocks until complete, returns results)
curl -X POST http://localhost:8080/api/benchmarking/start
```

Available profiles: `kafka`, `rabbitmq`, `pgmq`. Scale workers to at least `producersCount + sum(consumerGroups) + 1` (the extra one is the janitor).

### Check Connected Workers

```bash
curl http://localhost:8080/api/benchmarking/workers
```

## Project Structure

```
src/
├── MqBenchmark.Core/              # Shared interfaces, config models, metrics
│   ├── MqImplementation/          # IMqImplementation, IMqProducer, IMqConsumer, IMqJanitor
│   ├── Config/                    # MqConfig, WorkerConfig, RoutingPlan, CommunicationMode
│   └── Metrics/                   # MessageTimestamp, RateLimiter, TimestampBatchTransfer
├── MqBenchmark.Implementations/   # Broker implementations
│   ├── Kafka/                     # KafkaProducer, KafkaConsumer, KafkaJanitor, KafkaNaming
│   ├── RabbitMq/                  # RabbitMqProducer, RabbitMqConsumer, RabbitMqJanitor, RabbitMqNaming
│   └── PgMq/                      # PgMqProducer, PgMqConsumer, PgMqJanitor, PgMqNaming
├── MqBenchmark.PgMq.Client/       # Custom PGMQ client (raw SQL to pgmq functions)
├── MqBenchmark.Worker/            # Worker process (receives role, produces/consumes)
└── MqBenchmark.Orchestrator/      # REST API + SignalR hub, test coordination, routing plan generation

examples/                           # Ready-to-use test configuration files
result-analysis/                    # Python tool for generating charts & tables from CSV results
```

## Example Configs

The `examples/` directory contains configs for common scenarios:

| Config | Mode | Broker | Description |
|--------|------|--------|-------------|
| `kafka.json` | PointToPoint | Kafka | 1P/1C, 1000 msgs at 200 MPS |
| `kafka-pubsub.json` | PubSub | Kafka | 2P, 2 groups of 2 consumers |
| `kafka-streaming.json` | Streaming | Kafka | 2P, 2 groups of 2 consumers |
| `rabbitmq.json` | PointToPoint | RabbitMQ | 1P/1C, 1000 msgs at 200 MPS |
| `rabbitmq-pubsub.json` | PubSub | RabbitMQ | 2P, 2 groups of 2 consumers |
| `rabbitmq-streaming.json` | Streaming | RabbitMQ | 2P, 2 groups of 2 consumers |
| `pgmq.json` | PointToPoint | PGMQ | 1P/1C, 1000 msgs at 200 MPS |
| `pgmq-pubsub.json` | PubSub | PGMQ | 2P, 2 groups of 2 consumers |
| `pgmq-streaming.json` | Streaming | PGMQ | 2P, 2 groups of 2 consumers |
| `throughput-kafka-fast.json` | PointToPoint | Kafka | Max throughput, buffered producer |
| `throughput-kafka-durable.json` | PointToPoint | Kafka | Durable mode with idempotence |
| `throughput-rabbitmq-fast.json` | PointToPoint | RabbitMQ | Max throughput config |
| `throughput-rabbitmq-durable.json` | PointToPoint | RabbitMQ | Durable + publisher confirms |
| `throughput-pgmq-fast.json` | PointToPoint | PGMQ | Batch send/pop, unlogged queue |
| `throughput-pgmq-durable.json` | PointToPoint | PGMQ | Durable with archive mode |

## Result Analysis

The `result-analysis/` directory contains a Python tool for visualizing benchmark results:

```bash
cd result-analysis
pip install -e .
python main.py --mode all
```

Generates:
- Throughput bar charts (msg/s and MB/s) grouped by message size
- Latency-over-time smoothed line plots
- Latency distribution plots (KDE on log scale)
- Comparison tables (avg, median, P50, P95, P99)

Results are read from `result-sets/` (organized as `{test-type}/{group}/{broker}/results.csv`) and output to `output/`.

## Key Design Decisions

- **Auto-generated infrastructure names** — Queue names, exchange names, topic names, group IDs, and routing keys are generated by static `*Naming` classes in each broker implementation. Config only contains connection details and tuning knobs.
- **Broker-agnostic orchestrator** — The orchestrator works with logical group names (`group_0`, `group_1`) and routing plans. Broker implementations translate these to concrete infrastructure.
- **Routing plans for PointToPoint** — The orchestrator generates a `RoutingPlan` that maps each logical group target to a message count. Producers round-robin across targets using `RoundRobinRoutingIterator`. When multiple producers exist, the plan is split proportionally via `TestScheduler.SplitRoutingPlan`.
- **Per-group metrics** — Results (including `messagesLost`) are calculated per consumer group against expected counts, not against total messages sent.
- **Kafka buffered producer** with queue-full backpressure — `Produce()` with retry on `Local_QueueFull` for high throughput without hanging
- **RabbitMQ publisher confirms** — optional config to ensure `BasicPublishAsync` blocks until broker confirms, giving accurate durable-mode latency
- **PGMQ batch operations** — `send_batch()` and batch pop/read/delete to minimize round-trips
- **Consumer stop via idle timeout** — consumers don't know total message count; they drain until no messages arrive for N seconds
- **Janitor phase** — dedicated pre-test cleanup ensures consistent starting state across runs
- **No CPU limits on workers** — workers shouldn't be the bottleneck; brokers are constrained (2 CPU / 1 GB for Kafka & RabbitMQ, 4 CPU / 2 GB for PGMQ)
- **Rate limiting** — hybrid sleep + spin-wait (`RateLimiter`) for precise send frequency control
- **Custom PGMQ client** — wraps raw SQL calls to `pgmq.*` functions rather than using an external library, enabling full control over batch APIs and topic operations
