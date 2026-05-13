# Result Analysis

This directory contains a Python script that processes benchmark CSV results and generates comparison plots and summary tables for message queue systems (RabbitMQ, Kafka, PGMQ).

## Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) package manager

## Usage

```bash
cd result-analysis
uv sync
uv run python main.py
```

Charts are saved as PNG files in the `output/` directory. Summary tables are saved as `.txt` files alongside them.

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `--mode` | Analysis mode: `throughput`, `latency`, `messaging-modes`, `pgmq-modes`, or `all` | `all` |
| `--warmup` | Number of initial messages to skip as warmup (0 to disable) | 1% of message count |
| `--rolling` | Rolling window size for latency-over-time smoothing (0 to disable) | `100` |

## Analysis Modes

- **throughput** — Bar charts comparing messages/s and MB/s across different message sizes.
- **latency** — Latency histograms, box plots, and rolling latency-over-time plots at various send rates.
- **messaging-modes** — Compares communication patterns (point-to-point, pub/sub, streaming) across brokers.
- **pgmq-modes** — Compares PGMQ-specific polling strategies (ClientPoll, ServerPoll, ListenNotify).

## Result Sets Directory Structure

Place benchmark outputs under `result-sets/`. The script supports two layouts:

### Nested layout (used by throughput, latency, messaging-modes)

```
result-sets/<mode>/
  <group>/                   # e.g. "16B", "1000MPS", "PubSub"
    testConfig.json
    <MqName>/                # e.g. "RabbitMq", "Kafka", "Pgmq"
      results.csv
```

### Flat layout (used by pgmq-modes)

```
result-sets/<mode>/
  testConfig.json
  <ColumnName>/              # e.g. "ClientPoll", "ServerPoll"
    results.csv
```

### testConfig.json format

```json
{
  "title": "Chart title",
  "testConfig": {
    "messageSizeInBytes": "1024",
    "messageCount": "100000",
    "producersCount": "1",
    "consumersCount": "1"
  }
}
```

### CSV format

Each `results.csv` must contain at least `SendTimestampTicks` and `ReceiveTimestampTicks` columns (in .NET tick units — 10,000 ticks per millisecond).

## Configuration

`config.json` maps each mode to its plot title and result set directory path.
