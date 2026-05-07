import json
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

TICKS_PER_MS = 10_000
TICKS_PER_SEC = 10_000_000

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def load_config():
    config_path = os.path.join(SCRIPT_DIR, "plot-throughput-config.json")
    with open(config_path) as f:
        return json.load(f)


def load_results(csv_path):
    df = pd.read_csv(csv_path)
    df["LatencyMs"] = (
        df["ReceiveTimestampTicks"] - df["SendTimestampTicks"]
    ) / TICKS_PER_MS
    df = df.sort_values("SendTimestampTicks").reset_index(drop=True)
    return df


def compute_metrics(df, message_size_bytes):
    duration_ticks = df["ReceiveTimestampTicks"].max() - df["SendTimestampTicks"].min()
    duration_sec = duration_ticks / TICKS_PER_SEC
    msg_count = len(df)
    latency = df["LatencyMs"]

    return {
        "messages": msg_count,
        "duration_sec": round(duration_sec, 4),
        "throughput_msgs": round(msg_count / duration_sec, 2),
        "throughput_mbs": round(
            (msg_count * message_size_bytes) / duration_sec / 1_000_000, 4
        ),
        "avg_latency_ms": round(latency.mean(), 4),
        "median_latency_ms": round(latency.median(), 4),
        "min_latency_ms": round(latency.min(), 4),
        "max_latency_ms": round(latency.max(), 4),
        "p50_ms": round(latency.quantile(0.50), 4),
        "p95_ms": round(latency.quantile(0.95), 4),
        "p99_ms": round(latency.quantile(0.99), 4),
    }


def discover_groups(results_dir):
    """Discover column groups (message size dirs) and their columns (MQ systems)."""
    groups = []
    for group_name in sorted(os.listdir(results_dir)):
        group_path = os.path.join(results_dir, group_name)
        if not os.path.isdir(group_path):
            continue

        test_config_path = os.path.join(group_path, "testConfig.json")
        if not os.path.exists(test_config_path):
            continue

        with open(test_config_path) as f:
            test_config = json.load(f)

        message_size = int(test_config["testConfig"]["messageSizeInBytes"])

        columns = []
        for col_name in sorted(os.listdir(group_path)):
            col_path = os.path.join(group_path, col_name)
            if not os.path.isdir(col_path):
                continue
            csv_path = os.path.join(col_path, "results.csv")
            if os.path.exists(csv_path):
                columns.append({"name": col_name, "csv_path": csv_path})

        groups.append(
            {
                "name": group_name,
                "message_size_bytes": message_size,
                "columns": columns,
            }
        )

    # Sort by message size
    groups.sort(key=lambda g: g["message_size_bytes"])
    return groups


def plot_throughput(groups, all_metrics, output_dir, title):
    group_names = [g["name"] for g in groups]
    mq_names = []
    for g in groups:
        for col in g["columns"]:
            if col["name"] not in mq_names:
                mq_names.append(col["name"])

    color_map = {
        "RabbitMq": "#F28C28",
        "Pgmq": "#3B7DD8",
        "Kafka": "#4CAF50",
    }
    colors = [color_map.get(mq, "gray") for mq in mq_names]
    x = np.arange(len(group_names))
    width = 0.8 / len(mq_names)

    # Plot 1: msg/s
    fig, ax = plt.subplots(figsize=(12, 6))
    for i, mq in enumerate(mq_names):
        values = []
        for g in groups:
            key = (g["name"], mq)
            if key in all_metrics:
                values.append(all_metrics[key]["throughput_msgs"])
            else:
                values.append(0)
        offset = (i - len(mq_names) / 2 + 0.5) * width
        bars = ax.bar(x + offset, values, width, label=mq, color=colors[i])
        for bar, val in zip(bars, values):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height(),
                    f"{val:,.0f}",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                )

    ax.set_xlabel("Message Size")
    ax.set_ylabel("Messages / second")
    ax.set_title(f"{title} - Throughput (msg/s)")
    ax.set_xticks(x)
    ax.set_xticklabels(group_names)
    ax.legend()
    plt.tight_layout()
    path = os.path.join(output_dir, "throughput_msgs.png")
    plt.savefig(path, dpi=150)
    print(f"Saved: {path}")
    plt.show()

    # Plot 2: MB/s
    fig, ax = plt.subplots(figsize=(12, 6))
    for i, mq in enumerate(mq_names):
        values = []
        for g in groups:
            key = (g["name"], mq)
            if key in all_metrics:
                values.append(all_metrics[key]["throughput_mbs"])
            else:
                values.append(0)
        offset = (i - len(mq_names) / 2 + 0.5) * width
        bars = ax.bar(x + offset, values, width, label=mq, color=colors[i])
        for bar, val in zip(bars, values):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height(),
                    f"{val:.2f}",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                )

    ax.set_yscale("log")
    ax.set_xlabel("Message Size")
    ax.set_ylabel("MB / second (log scale)")
    ax.set_title(f"{title} - Throughput (MB/s)")
    ax.set_xticks(x)
    ax.set_xticklabels(group_names)
    ax.legend()
    plt.tight_layout()
    path = os.path.join(output_dir, "throughput_mbs.png")
    plt.savefig(path, dpi=150)
    print(f"Saved: {path}")
    plt.show()


def save_comparison_table(groups, all_metrics, output_dir):
    mq_names = []
    for g in groups:
        for col in g["columns"]:
            if col["name"] not in mq_names:
                mq_names.append(col["name"])

    metric_labels = [
        ("duration_sec", "Duration (s)"),
        ("throughput_msgs", "Throughput (msg/s)"),
        ("throughput_mbs", "Throughput (MB/s)"),
        ("avg_latency_ms", "Avg Latency (ms)"),
        ("median_latency_ms", "Median Latency (ms)"),
        ("min_latency_ms", "Min Latency (ms)"),
        ("max_latency_ms", "Max Latency (ms)"),
        ("p50_ms", "P50 (ms)"),
        ("p95_ms", "P95 (ms)"),
        ("p99_ms", "P99 (ms)"),
    ]

    lines = []

    for g in groups:
        lines.append(f"\n{'=' * 70}")
        lines.append(f"  {g['name']} (message size: {g['message_size_bytes']} bytes)")
        lines.append(f"{'=' * 70}")

        label_w = max(len(ml[1]) for ml in metric_labels) + 2
        col_w = max(len(mq) for mq in mq_names) + 4

        header = f"{'Metric':<{label_w}}" + "".join(f"{mq:>{col_w}}" for mq in mq_names)
        lines.append(header)
        lines.append("-" * len(header))

        for key, label in metric_labels:
            row = f"{label:<{label_w}}"
            for mq in mq_names:
                mk = (g["name"], mq)
                if mk in all_metrics:
                    row += f"{all_metrics[mk][key]:>{col_w}}"
                else:
                    row += f"{'N/A':>{col_w}}"
            lines.append(row)

    output = "\n".join(lines) + "\n"
    path = os.path.join(output_dir, "throughput_comparison_table.txt")
    with open(path, "w") as f:
        f.write(output)
    print(f"Saved: {path}")
    print(output)


def main():
    config = load_config()
    title = config["title"]
    output_dir = os.path.join(SCRIPT_DIR, config["outputPath"])
    results_dir = os.path.join(SCRIPT_DIR, config["resultsSetsDir"])

    os.makedirs(output_dir, exist_ok=True)

    print(f"=== {title} ===\n")

    groups = discover_groups(results_dir)
    all_metrics = {}

    for g in groups:
        for col in g["columns"]:
            df = load_results(col["csv_path"])
            metrics = compute_metrics(df, g["message_size_bytes"])
            all_metrics[(g["name"], col["name"])] = metrics

    save_comparison_table(groups, all_metrics, output_dir)
    plot_throughput(groups, all_metrics, output_dir, title)


if __name__ == "__main__":
    main()
