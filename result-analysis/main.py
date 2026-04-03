import json
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

TICKS_PER_MS = 10_000
TICKS_PER_SEC = 10_000_000

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def load_config():
    config_path = os.path.join(SCRIPT_DIR, "config.json")
    with open(config_path) as f:
        return json.load(f)


def load_result_set(path):
    full_path = os.path.join(SCRIPT_DIR, path)
    df = pd.read_csv(full_path)
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
            (msg_count * message_size_bytes) / duration_sec / 1_000_000, 2
        ),
        "Avg Latency (ms)": round(latency.mean(), 4),
        "Median Latency (ms)": round(latency.median(), 4),
        "Min Latency (ms)": round(latency.min(), 4),
        "Max Latency (ms)": round(latency.max(), 4),
        "P50 (ms)": round(latency.quantile(0.50), 4),
        "P95 (ms)": round(latency.quantile(0.95), 4),
        "P99 (ms)": round(latency.quantile(0.99), 4),
    }


def print_comparison_table(metrics_by_name):
    metric_keys = list(next(iter(metrics_by_name.values())).keys())
    names = list(metrics_by_name.keys())

    # Column widths
    label_w = max(len(k) for k in metric_keys) + 2
    col_w = max(max(len(n) for n in names), 15) + 2

    # Header
    header = f"{'Metric':<{label_w}}" + "".join(f"{n:>{col_w}}" for n in names)
    print(header)
    print("-" * len(header))

    for key in metric_keys:
        row = f"{key:<{label_w}}"
        for name in names:
            val = metrics_by_name[name][key]
            row += f"{val:>{col_w}}"
        print(row)


def save_comparison_table(metrics_by_name, output_dir):
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
            val = metrics_by_name[name][key]
            row += f"{val:>{col_w}}"
        lines.append(row)

    path = os.path.join(output_dir, "comparison_table.txt")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\nSaved: {path}")


def plot_latency_over_time(datasets, output_dir):
    plt.figure(figsize=(14, 6))
    for name, df in datasets.items():
        plt.plot(df.index, df["LatencyMs"], label=name, alpha=0.7, linewidth=1.2)
    plt.xlabel("Message Index (ordered by send time)")
    plt.ylabel("Latency (ms)")
    plt.title("Message Latency Over Time")
    plt.legend()
    plt.tight_layout()
    path = os.path.join(output_dir, "latency_over_time.png")
    plt.savefig(path, dpi=150)
    print(f"\nSaved: {path}")
    plt.show()


def plot_latency_distribution(datasets, output_dir):
    from scipy.stats import gaussian_kde

    plt.figure(figsize=(14, 6))
    for name, df in datasets.items():
        latency = df["LatencyMs"].values
        kde = gaussian_kde(latency)
        x = np.linspace(latency.min(), latency.max(), 500)
        plt.plot(x, kde(x), label=name, linewidth=2)
    plt.xlabel("Latency (ms)")
    plt.ylabel("Density")
    plt.title("Message Latency Distribution")
    plt.legend()
    plt.tight_layout()
    path = os.path.join(output_dir, "latency_distribution.png")
    plt.savefig(path, dpi=150)
    print(f"Saved: {path}")
    plt.show()


def main():
    config = load_config()
    test_config = config["testConfig"]
    message_size = int(test_config["messageSizeInBytes"])

    print(f"=== {config['title']} ===")
    print(
        f"Message size: {message_size} B | "
        f"Count: {test_config['messageCount']} | "
        f"Producers: {test_config['producersCount']} | "
        f"Consumers: {test_config['consumersCount']}\n"
    )

    datasets = {}
    metrics_by_name = {}

    for rs in config["resultSets"]:
        name = rs["name"]
        df = load_result_set(rs["path"])
        datasets[name] = df
        metrics_by_name[name] = compute_metrics(df, message_size)

    output_dir = os.path.join(SCRIPT_DIR, "output")
    os.makedirs(output_dir, exist_ok=True)

    print_comparison_table(metrics_by_name)
    save_comparison_table(metrics_by_name, output_dir)

    plot_latency_over_time(datasets, output_dir)
    plot_latency_distribution(datasets, output_dir)


if __name__ == "__main__":
    main()
