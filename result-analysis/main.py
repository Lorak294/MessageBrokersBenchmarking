import argparse
import json
import os
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde

# ── Constants ──────────────────────────────────────────────────────────────────

TICKS_PER_MS = 10_000
TICKS_PER_SEC = 10_000_000

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

COLOR_MAP = {
    "RabbitMq": "#F28C28",
    "Pgmq": "#3B7DD8",
    "Kafka": "#4CAF50",
    "ClientPoll": "#3B7DD8",
    "ListenNotify": "#7B1FA2",
    "ServerPoll": "#00897B",
}

# ── Shared helpers ─────────────────────────────────────────────────────────────


def load_config():
    config_path = os.path.join(SCRIPT_DIR, "config.json")
    with open(config_path) as f:
        return json.load(f)


def find_csv(directory):
    """Find CSV file in directory, handling inconsistent naming."""
    for name in ("results.csv", "result.csv"):
        path = os.path.join(directory, name)
        if os.path.exists(path):
            return path
    return None


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
    """Discover column groups and their columns (MQ systems).

    Supports two layouts:
    - Flat: testConfig.json at results_dir root, subdirs are columns
    - Nested: subdirs each have their own testConfig.json with column subdirs
    """
    # Flat structure: testConfig.json at root level
    root_config = os.path.join(results_dir, "testConfig.json")
    if os.path.exists(root_config):
        with open(root_config) as f:
            test_config = json.load(f)
        message_size = int(test_config["testConfig"]["messageSizeInBytes"])
        columns = []
        for col_name in sorted(os.listdir(results_dir)):
            col_path = os.path.join(results_dir, col_name)
            if not os.path.isdir(col_path):
                continue
            csv_path = find_csv(col_path)
            if csv_path:
                columns.append({"name": col_name, "csv_path": csv_path})
        return [
            {
                "name": os.path.basename(results_dir),
                "title": test_config.get("title", os.path.basename(results_dir)),
                "message_size_bytes": message_size,
                "test_config": test_config["testConfig"],
                "columns": columns,
            }
        ]

    # Nested structure
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
            csv_path = find_csv(col_path)
            if csv_path:
                columns.append({"name": col_name, "csv_path": csv_path})

        groups.append(
            {
                "name": group_name,
                "title": test_config.get("title", group_name),
                "message_size_bytes": message_size,
                "test_config": test_config["testConfig"],
                "columns": columns,
            }
        )

    groups.sort(key=lambda g: g["message_size_bytes"])
    return groups


def collect_all_mq_names(groups):
    """Get unique MQ names in order across all groups."""
    names = []
    for g in groups:
        for col in g["columns"]:
            if col["name"] not in names:
                names.append(col["name"])
    return names


def load_all_metrics(groups):
    """Load results and compute metrics for all groups. Returns (all_metrics, all_datasets)."""
    all_metrics = {}
    all_datasets = {}

    for g in groups:
        datasets = {}
        for col in g["columns"]:
            df = load_results(col["csv_path"])
            datasets[col["name"]] = df
            all_metrics[(g["name"], col["name"])] = compute_metrics(
                df, g["message_size_bytes"]
            )
        all_datasets[g["name"]] = datasets

    return all_metrics, all_datasets


def format_table(metric_labels, mq_names, values_fn):
    """Format a comparison table and return lines."""
    label_w = max(len(ml[1]) for ml in metric_labels) + 2
    col_w = max(max(len(mq) for mq in mq_names), 15) + 2

    lines = []
    header = f"{'Metric':<{label_w}}" + "".join(f"{mq:>{col_w}}" for mq in mq_names)
    lines.append(header)
    lines.append("-" * len(header))

    for key, label in metric_labels:
        row = f"{label:<{label_w}}"
        for mq in mq_names:
            val = values_fn(key, mq)
            if val is not None:
                row += f"{val:>{col_w}}"
            else:
                row += f"{'N/A':>{col_w}}"
        lines.append(row)

    return lines


# ── Throughput mode ────────────────────────────────────────────────────────────

THROUGHPUT_METRIC_LABELS = [
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


def plot_throughput(groups, all_metrics, output_dir, title):
    group_names = [g["name"] for g in groups]
    mq_names = collect_all_mq_names(groups)
    colors = [COLOR_MAP.get(mq, "gray") for mq in mq_names]
    x = np.arange(len(group_names))
    width = 0.8 / len(mq_names)

    # Plot 1: msg/s
    fig, ax = plt.subplots(figsize=(12, 6))
    for i, mq in enumerate(mq_names):
        values = [
            all_metrics.get((g["name"], mq), {}).get("throughput_msgs", 0)
            for g in groups
        ]
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
        values = [
            all_metrics.get((g["name"], mq), {}).get("throughput_mbs", 0)
            for g in groups
        ]
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


def save_throughput_table(groups, all_metrics, output_dir):
    mq_names = collect_all_mq_names(groups)
    all_lines = []

    for g in groups:
        all_lines.append(f"\n{'=' * 70}")
        all_lines.append(
            f"  {g['name']} (message size: {g['message_size_bytes']} bytes)"
        )
        all_lines.append(f"{'=' * 70}")

        def values_fn(key, mq, _g=g):
            return all_metrics.get((_g["name"], mq), {}).get(key)

        all_lines.extend(format_table(THROUGHPUT_METRIC_LABELS, mq_names, values_fn))

    output = "\n".join(all_lines) + "\n"
    path = os.path.join(output_dir, "throughput_comparison_table.txt")
    with open(path, "w") as f:
        f.write(output)
    print(f"Saved: {path}")
    print(output)


def run_throughput(mode_config, output_dir):
    title = mode_config["title"]
    results_dir = os.path.join(SCRIPT_DIR, mode_config["resultsSetsDir"])

    print(f"=== {title} ===\n")

    groups = discover_groups(results_dir)
    all_metrics, _ = load_all_metrics(groups)

    save_throughput_table(groups, all_metrics, output_dir)
    plot_throughput(groups, all_metrics, output_dir, title)


# ── Latency mode ──────────────────────────────────────────────────────────────

LATENCY_METRIC_LABELS = [
    ("messages", "Messages"),
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


def plot_latency_over_time(datasets, group_name, title, output_dir):
    plt.figure(figsize=(14, 6))
    for name, df in datasets.items():
        color = COLOR_MAP.get(name, "gray")
        smoothed = df["LatencyMs"].rolling(window=500, min_periods=1).mean()
        plt.plot(df.index, smoothed, label=name, alpha=0.7, linewidth=1.2, color=color)
    plt.xlabel("Message Index (ordered by send time)")
    plt.ylabel("Latency (ms)")
    plt.title(f"{title} - Latency Over Time")
    plt.legend()
    plt.tight_layout()
    path = os.path.join(output_dir, f"{group_name}_latency_over_time.png")
    plt.savefig(path, dpi=150)
    print(f"Saved: {path}")
    plt.show()


def plot_latency_distribution(datasets, group_name, title, output_dir):
    plt.figure(figsize=(14, 6))
    for name, df in datasets.items():
        color = COLOR_MAP.get(name, "gray")
        latency = df["LatencyMs"].values
        kde = gaussian_kde(latency)
        x_min = max(latency.min(), 1e-3)
        x = np.logspace(np.log10(x_min), np.log10(latency.max()), 500)
        plt.plot(x, kde(x), label=name, linewidth=2, color=color)
    plt.xscale("log")
    plt.xlabel("Latency (ms, log scale)")
    plt.ylabel("Density")
    plt.title(f"{title} - Latency Distribution")
    plt.legend()
    plt.tight_layout()
    path = os.path.join(output_dir, f"{group_name}_latency_distribution.png")
    plt.savefig(path, dpi=150)
    print(f"Saved: {path}")
    plt.show()


def save_latency_table(metrics_by_name, group_name, output_dir):
    mq_names = list(metrics_by_name.keys())

    def values_fn(key, mq):
        return metrics_by_name.get(mq, {}).get(key)

    lines = format_table(LATENCY_METRIC_LABELS, mq_names, values_fn)
    table_text = "\n".join(lines)
    print(table_text)
    print()

    path = os.path.join(output_dir, f"{group_name}_comparison_table.txt")
    with open(path, "w") as f:
        f.write(table_text + "\n")
    print(f"Saved: {path}")


def run_latency(mode_config, output_dir):
    title = mode_config["title"]
    results_dir = os.path.join(SCRIPT_DIR, mode_config["resultsSetsDir"])

    print(f"=== {title} ===\n")

    groups = discover_groups(results_dir)
    all_metrics, all_datasets = load_all_metrics(groups)

    for g in groups:
        tc = g["test_config"]
        print(f"--- {g['name']} ---")
        print(
            f"Message size: {g['message_size_bytes']} B | "
            f"Count: {tc['messageCount']} | "
            f"Producers: {tc['producersCount']} | "
            f"Consumers: {tc['consumersCount']}\n"
        )

        metrics_by_name = {
            col["name"]: all_metrics[(g["name"], col["name"])] for col in g["columns"]
        }

        save_latency_table(metrics_by_name, g["name"], output_dir)
        plot_latency_over_time(
            all_datasets[g["name"]], g["name"], g["title"], output_dir
        )
        plot_latency_distribution(
            all_datasets[g["name"]], g["name"], g["title"], output_dir
        )
        print()


# ── Messaging modes ────────────────────────────────────────────────────────────

SHADE_MAP = {
    "Kafka": ["#A5D6A7", "#4CAF50", "#2E7D32"],  # light/mid/dark green
    "Pgmq": ["#90CAF9", "#3B7DD8", "#1565C0"],  # light/mid/dark blue
    "RabbitMq": ["#FFCC80", "#F28C28", "#E65100"],  # light/mid/dark orange
}

MESSAGING_METRIC_LABELS = [
    ("messages", "Messages"),
    ("avg_latency_ms", "Avg Latency (ms)"),
    ("median_latency_ms", "Median Latency (ms)"),
    ("min_latency_ms", "Min Latency (ms)"),
    ("max_latency_ms", "Max Latency (ms)"),
    ("p50_ms", "P50 (ms)"),
    ("p95_ms", "P95 (ms)"),
    ("p99_ms", "P99 (ms)"),
]


def discover_messaging_groups(results_dir):
    """Discover messaging mode groups and their brokers."""
    groups = []
    for mode_name in sorted(os.listdir(results_dir)):
        mode_path = os.path.join(results_dir, mode_name)
        if not os.path.isdir(mode_path):
            continue

        test_config_path = os.path.join(mode_path, "testConfig.json")
        if not os.path.exists(test_config_path):
            continue

        with open(test_config_path) as f:
            test_config = json.load(f)

        message_size = int(test_config["testConfig"]["messageSizeInBytes"])

        brokers = []
        for broker_name in sorted(os.listdir(mode_path)):
            broker_path = os.path.join(mode_path, broker_name)
            if not os.path.isdir(broker_path):
                continue
            csv_path = find_csv(broker_path)
            if csv_path:
                brokers.append({"name": broker_name, "csv_path": csv_path})

        groups.append(
            {
                "name": mode_name,
                "title": test_config.get("title", mode_name),
                "message_size_bytes": message_size,
                "test_config": test_config["testConfig"],
                "brokers": brokers,
            }
        )

    return groups


def load_messaging_data(group):
    """Load CSV for each broker and split by ConsumerGroup.

    Returns dict: {broker_name: {group_id: DataFrame}}
    All expected groups (0, 1, 2) are included even if empty.
    """
    data = {}
    for broker in group["brokers"]:
        df = load_results(broker["csv_path"])
        by_group = {}
        for gid in range(3):
            sub = df[df["ConsumerGroup"] == gid].reset_index(drop=True)
            by_group[gid] = sub
        data[broker["name"]] = by_group
    return data


def plot_messaging_latency_over_time(data, mode_name, title, output_dir):
    plt.figure(figsize=(14, 6))
    for broker_name, groups_data in data.items():
        shades = SHADE_MAP.get(broker_name, ["#888", "#555", "#333"])
        for gid, df in sorted(groups_data.items()):
            if len(df) == 0:
                continue
            smoothed = df["LatencyMs"].rolling(window=500, min_periods=1).mean()
            plt.plot(
                df.index,
                smoothed,
                label=f"{broker_name} (Group {gid})",
                alpha=0.7,
                linewidth=1.2,
                color=shades[gid % len(shades)],
            )
    plt.xlabel("Message Index (ordered by send time)")
    plt.yscale("log")
    plt.ylabel("Latency (ms, log scale)")
    plt.title(f"{title} - Latency Over Time")
    plt.legend(fontsize=8)
    plt.tight_layout()
    path = os.path.join(output_dir, f"{mode_name}_messaging_latency_over_time.png")
    plt.savefig(path, dpi=150)
    print(f"Saved: {path}")
    plt.show()


def plot_messaging_latency_distribution(data, mode_name, title, output_dir):
    plt.figure(figsize=(14, 6))
    for broker_name, groups_data in data.items():
        shades = SHADE_MAP.get(broker_name, ["#888", "#555", "#333"])
        for gid, df in sorted(groups_data.items()):
            if len(df) == 0:
                continue
            latency = df["LatencyMs"].values
            kde = gaussian_kde(latency)
            x_min = max(latency.min(), 1e-3)
            x = np.logspace(np.log10(x_min), np.log10(latency.max()), 500)
            plt.plot(
                x,
                kde(x),
                label=f"{broker_name} (Group {gid})",
                linewidth=2,
                color=shades[gid % len(shades)],
            )
    plt.xscale("log")
    plt.xlabel("Latency (ms, log scale)")
    plt.ylabel("Density")
    plt.title(f"{title} - Latency Distribution")
    plt.legend(fontsize=8)
    plt.tight_layout()
    path = os.path.join(output_dir, f"{mode_name}_messaging_latency_distribution.png")
    plt.savefig(path, dpi=150)
    print(f"Saved: {path}")
    plt.show()


def save_messaging_table(data, group, output_dir):
    """Save comparison table with per-group metrics and fairness diff columns."""
    mode_name = group["name"]
    message_size = group["message_size_bytes"]
    all_lines = []

    for broker_name, groups_data in data.items():
        all_lines.append(f"\n--- {broker_name} ---")

        # Compute metrics per consumer group
        group_ids = sorted(groups_data.keys())
        group_metrics = {}
        for gid in group_ids:
            df = groups_data[gid]
            if len(df) == 0:
                group_metrics[gid] = None
            else:
                group_metrics[gid] = compute_metrics(df, message_size)

        col_names = [f"Group {gid}" for gid in group_ids] + ["Max Diff", "Max Diff %"]
        label_w = max(len(ml[1]) for ml in MESSAGING_METRIC_LABELS) + 2
        col_w = max(max(len(c) for c in col_names), 12) + 2

        header = f"{'Metric':<{label_w}}" + "".join(f"{c:>{col_w}}" for c in col_names)
        all_lines.append(header)
        all_lines.append("-" * len(header))

        for key, label in MESSAGING_METRIC_LABELS:
            row = f"{label:<{label_w}}"
            values = []
            for gid in group_ids:
                m = group_metrics[gid]
                if m is None:
                    row += f"{0 if key == 'messages' else 'N/A':>{col_w}}"
                else:
                    row += f"{m[key]:>{col_w}}"
                    values.append(m[key])

            # Compute diff
            if len(values) >= 2 and key != "messages":
                max_val = max(values)
                min_val = min(values)
                diff = round(max_val - min_val, 4)
                if min_val != 0:
                    diff_pct = f"{round((max_val - min_val) / min_val * 100, 2)}%"
                else:
                    diff_pct = "N/A"
                row += f"{diff:>{col_w}}"
                row += f"{diff_pct:>{col_w}}"
            elif key == "messages" and len(values) >= 2:
                max_val = max(values)
                min_val = min(values)
                diff = max_val - min_val
                if min_val != 0:
                    diff_pct = f"{round((max_val - min_val) / min_val * 100, 2)}%"
                else:
                    diff_pct = "N/A"
                row += f"{diff:>{col_w}}"
                row += f"{diff_pct:>{col_w}}"
            else:
                row += f"{'N/A':>{col_w}}" * 2

            all_lines.append(row)

    output = "\n".join(all_lines) + "\n"
    print(output)

    path = os.path.join(output_dir, f"{mode_name}_messaging_comparison_table.txt")
    with open(path, "w") as f:
        f.write(output)
    print(f"Saved: {path}")


def run_messaging_modes(mode_config, output_dir):
    title = mode_config["title"]
    results_dir = os.path.join(SCRIPT_DIR, mode_config["resultsSetsDir"])

    print(f"=== {title} ===\n")

    groups = discover_messaging_groups(results_dir)

    for g in groups:
        tc = g["test_config"]
        print(f"--- {g['name']} ({g['title']}) ---")
        print(
            f"Message size: {g['message_size_bytes']} B | "
            f"Count: {tc['messageCount']} | "
            f"Producers: {tc['producersCount']} | "
            f"Consumers: {tc['consumersCount']}\n"
        )

        data = load_messaging_data(g)

        save_messaging_table(data, g, output_dir)
        plot_messaging_latency_over_time(data, g["name"], g["title"], output_dir)
        plot_messaging_latency_distribution(data, g["name"], g["title"], output_dir)
        print()


# ── Main ───────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Message Queue Benchmark Analysis")
    parser.add_argument(
        "--mode",
        choices=["throughput", "latency", "messaging-modes", "pgmq-modes", "all"],
        default="all",
        help="Analysis mode: throughput, latency, messaging-modes, pgmq-modes, or all (default: all)",
    )
    args = parser.parse_args()

    config = load_config()
    output_dir = os.path.join(SCRIPT_DIR, config["outputPath"])
    os.makedirs(output_dir, exist_ok=True)

    if args.mode in ("throughput", "all"):
        run_throughput(config["throughput"], output_dir)

    if args.mode in ("latency", "all"):
        run_latency(config["latency"], output_dir)

    if args.mode in ("messaging-modes", "all"):
        run_messaging_modes(config["messaging-modes"], output_dir)

    if args.mode in ("pgmq-modes", "all"):
        run_latency(config["pgmq-modes"], output_dir)


if __name__ == "__main__":
    main()
