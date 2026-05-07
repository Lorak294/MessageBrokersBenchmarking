import json
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde

TICKS_PER_MS = 10_000
TICKS_PER_SEC = 10_000_000

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

COLOR_MAP = {
    "RabbitMq": "#F28C28",
    "Pgmq": "#3B7DD8",
    "Kafka": "#4CAF50",
}


def load_config():
    config_path = os.path.join(SCRIPT_DIR, "plot-latency-config.json")
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
        "Messages": msg_count,
        "Duration (s)": round(duration_sec, 4),
        "Throughput (msg/s)": round(msg_count / duration_sec, 2),
        "Throughput (MB/s)": round(
            (msg_count * message_size_bytes) / duration_sec / 1_000_000, 4
        ),
        "Avg Latency (ms)": round(latency.mean(), 4),
        "Median Latency (ms)": round(latency.median(), 4),
        "Min Latency (ms)": round(latency.min(), 4),
        "Max Latency (ms)": round(latency.max(), 4),
        "P50 (ms)": round(latency.quantile(0.50), 4),
        "P95 (ms)": round(latency.quantile(0.95), 4),
        "P99 (ms)": round(latency.quantile(0.99), 4),
    }


def discover_groups(results_dir):
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
                "title": test_config.get("title", group_name),
                "message_size_bytes": message_size,
                "test_config": test_config["testConfig"],
                "columns": columns,
            }
        )

    return groups


def print_and_save_table(metrics_by_name, group_name, output_dir):
    metric_keys = list(next(iter(metrics_by_name.values())).keys())
    names = list(metrics_by_name.keys())

    label_w = max(len(k) for k in metric_keys) + 2
    col_w = max(max(len(n) for n in names), 15) + 2

    lines = []
    header = f"{'Metric':<{label_w}}" + "".join(f"{n:>{col_w}}" for n in names)
    lines.append(header)
    lines.append("-" * len(header))

    for key in metric_keys:
        row = f"{key:<{label_w}}"
        for name in names:
            row += f"{metrics_by_name[name][key]:>{col_w}}"
        lines.append(row)

    table_text = "\n".join(lines)
    print(table_text)
    print()

    path = os.path.join(output_dir, f"{group_name}_comparison_table.txt")
    with open(path, "w") as f:
        f.write(table_text + "\n")
    print(f"Saved: {path}")


def plot_latency_over_time(datasets, group_name, title, output_dir):
    plt.figure(figsize=(14, 6))
    for name, df in datasets.items():
        color = COLOR_MAP.get(name, "gray")
        plt.plot(
            df.index, df["LatencyMs"], label=name, alpha=0.7, linewidth=1.2, color=color
        )
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
        x = np.linspace(latency.min(), latency.max(), 500)
        plt.plot(x, kde(x), label=name, linewidth=2, color=color)
    plt.xlabel("Latency (ms)")
    plt.ylabel("Density")
    plt.title(f"{title} - Latency Distribution")
    plt.legend()
    plt.tight_layout()
    path = os.path.join(output_dir, f"{group_name}_latency_distribution.png")
    plt.savefig(path, dpi=150)
    print(f"Saved: {path}")
    plt.show()


def main():
    config = load_config()
    title = config["title"]
    output_dir = os.path.join(SCRIPT_DIR, config["outputPath"])
    results_dir = os.path.join(SCRIPT_DIR, config["resultsSetsDir"])

    os.makedirs(output_dir, exist_ok=True)

    print(f"=== {title} ===\n")

    groups = discover_groups(results_dir)

    for g in groups:
        tc = g["test_config"]
        print(f"--- {g['name']} ---")
        print(
            f"Message size: {g['message_size_bytes']} B | "
            f"Count: {tc['messageCount']} | "
            f"Producers: {tc['producersCount']} | "
            f"Consumers: {tc['consumersCount']}\n"
        )

        datasets = {}
        metrics_by_name = {}

        for col in g["columns"]:
            df = load_results(col["csv_path"])
            datasets[col["name"]] = df
            metrics_by_name[col["name"]] = compute_metrics(df, g["message_size_bytes"])

        print_and_save_table(metrics_by_name, g["name"], output_dir)
        plot_latency_over_time(datasets, g["name"], g["title"], output_dir)
        plot_latency_distribution(datasets, g["name"], g["title"], output_dir)
        print()


if __name__ == "__main__":
    main()
