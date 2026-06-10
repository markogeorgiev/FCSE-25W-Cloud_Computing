import json
from pathlib import Path

import pandas as pd
import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
APP_DIR = BASE_DIR.parent
DATA_DIR = APP_DIR / "gcp_stats_last_run"
RESULTS_DIR = APP_DIR / "results" / "workload" / "20260609T153533529553Z"
OUTPUT_DIR = BASE_DIR / "gcp_stats_plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOCAL_TIMEZONE = "Europe/Budapest"
LINE_MARKER_SIZE = 3
TIME_TICK_INTERVAL_MINUTES = 2
TIME_TICK_LABEL_SIZE = 7
TIME_TICK_LABEL_ROTATION = 60


def parse_gcp_timestamp(series):
    cleaned = series.astype(str).str.replace(r" \(.+\)$", "", regex=True)
    timestamps = pd.to_datetime(
        cleaned,
        format="%a %b %d %Y %H:%M:%S GMT%z",
        errors="coerce",
        utc=True,
    )
    return timestamps.dt.tz_convert(LOCAL_TIMEZONE).dt.tz_localize(None)


def parse_metadata_timestamp(value):
    return (
        pd.to_datetime(value, utc=True)
        .tz_convert(LOCAL_TIMEZONE)
        .tz_localize(None)
    )


def read_metric(path, value_column):
    df = pd.read_csv(
        path,
        skiprows=2,
        header=None,
        names=["time", value_column],
    )
    df["time"] = parse_gcp_timestamp(df["time"])
    df[value_column] = pd.to_numeric(df[value_column], errors="coerce")
    return df.dropna(subset=["time", value_column]).sort_values("time")


def shade_benchmark(ax):
    ax.axvspan(
        benchmark_start,
        benchmark_end,
        color="tab:blue",
        alpha=0.08,
        label="Benchmark window",
    )


def format_time_axis(ax, *, start=None, end=None, tick_interval_minutes=TIME_TICK_INTERVAL_MINUTES):
    ax.set_xlim(start or plot_start, end or plot_end)
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=tick_interval_minutes))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.tick_params(axis="x", labelsize=TIME_TICK_LABEL_SIZE)
    for label in ax.get_xticklabels():
        label.set_rotation(TIME_TICK_LABEL_ROTATION)
        label.set_horizontalalignment("right")
    ax.set_xlabel(f"Time ({LOCAL_TIMEZONE})")
    ax.grid(True)


def add_legend(*axes, **kwargs):
    handles = []
    labels = []

    for ax in axes:
        axis_handles, axis_labels = ax.get_legend_handles_labels()
        handles.extend(axis_handles)
        labels.extend(axis_labels)

    if handles:
        axes[0].legend(handles, labels, **kwargs)


def color_axis(ax, color):
    ax.tick_params(axis="y", labelcolor=color)
    ax.yaxis.label.set_color(color)


def plot_metric(ax, df, column, label, **kwargs):
    if df.empty:
        return

    ax.plot(
        df["time"],
        df[column],
        marker="o",
        markersize=LINE_MARKER_SIZE,
        label=label,
        **kwargs,
    )


def get_change_points(df, column):
    if df.empty:
        return df

    return df[df[column].ne(df[column].shift())].copy()


def get_focused_change_window(*change_dfs, padding_minutes=2):
    event_times = []

    for df in change_dfs:
        if df.empty:
            continue

        visible_events = df[
            (df["time"] >= plot_start)
            & (df["time"] <= plot_end)
        ]
        event_times.extend(visible_events["time"].tolist())

    if not event_times:
        return plot_start, plot_end

    padding = pd.Timedelta(minutes=padding_minutes)
    return (
        max(plot_start, min(event_times) - padding),
        min(plot_end, max(event_times) + padding),
    )


def plot_step_metric(ax, df, column, label, *, mark_changes=False, **kwargs):
    if df.empty:
        return

    line = ax.step(
        df["time"],
        df[column],
        where="post",
        label=label,
        **kwargs,
    )[0]

    if mark_changes:
        changes = get_change_points(df, column)
        color = kwargs.get("color", line.get_color())
        ax.scatter(
            changes["time"],
            changes[column],
            color=color,
            s=28,
            zorder=4,
        )


def save_plot(filename, **kwargs):
    path = OUTPUT_DIR / filename
    plt.tight_layout()
    plt.savefig(path, dpi=300, **kwargs)
    plt.close()
    saved_paths.append(path)


with (RESULTS_DIR / "experiment_metadata.json").open(encoding="utf-8") as file:
    metadata = json.load(file)

benchmark_start = parse_metadata_timestamp(metadata["started_at"])
benchmark_end = parse_metadata_timestamp(metadata["finished_at"])
plot_start = benchmark_start.replace(minute=35, second=0, microsecond=0)
plot_end = plot_start + pd.Timedelta(hours=1)
network_plot_start = plot_start
network_plot_end = plot_start + pd.Timedelta(minutes=16)

summary_df = pd.read_csv(RESULTS_DIR / "run_summaries.csv")
summary_columns = [
    "jobs_submitted",
    "min_vm_count_observed",
    "max_vm_count_observed",
    "avg_vm_count_observed",
]

for column in summary_columns:
    summary_df[column] = pd.to_numeric(summary_df[column], errors="coerce")

summary_df = summary_df.dropna(
    subset=[
        "jobs_submitted",
        "avg_vm_count_observed",
    ]
).sort_values("jobs_submitted")

configured_min_size = read_metric(DATA_DIR / "Group_size_1.csv", "configured_min_size")
configured_max_size = read_metric(DATA_DIR / "Group_size_2.csv", "configured_max_size")
recommended_size = read_metric(DATA_DIR / "Group_size_3.csv", "recommended_size")
actual_group_size = read_metric(DATA_DIR / "Group_size_4.csv", "actual_group_size")

autoscaler_cpu_utilization = read_metric(
    DATA_DIR / "Autoscaler_utilization_(CPU)_1.csv",
    "autoscaler_cpu_utilization",
)
autoscaler_cpu_capacity = read_metric(
    DATA_DIR / "Autoscaler_utilization_(CPU)_2.csv",
    "autoscaler_cpu_capacity",
)
cpu_utilization = read_metric(DATA_DIR / "CPU_utilization.csv", "cpu_utilization")

network_received_bytes = read_metric(
    DATA_DIR / "Network_bytes_1.csv",
    "network_received_bytes_per_second",
)
network_sent_bytes = read_metric(
    DATA_DIR / "Network_bytes_2.csv",
    "network_sent_bytes_per_second",
)
network_received_packets = read_metric(
    DATA_DIR / "Network_packets_1.csv",
    "network_received_packets_per_second",
)
network_sent_packets = read_metric(
    DATA_DIR / "Network_packets_2.csv",
    "network_sent_packets_per_second",
)

disk_read_bytes = read_metric(
    DATA_DIR / "Disk_I" / "O_(bytes)_1.csv",
    "disk_read_bytes_per_second",
)
disk_write_bytes = read_metric(
    DATA_DIR / "Disk_I" / "O_(bytes)_2.csv",
    "disk_write_bytes_per_second",
)
disk_read_ops = read_metric(
    DATA_DIR / "Disk_I" / "O_(operations)_1.csv",
    "disk_read_ops_per_second",
)
disk_write_ops = read_metric(
    DATA_DIR / "Disk_I" / "O_(operations)_2.csv",
    "disk_write_ops_per_second",
)

cpu_utilization["cpu_utilization_percent"] = cpu_utilization["cpu_utilization"] * 100
autoscaler_cpu_utilization["autoscaler_cpu_utilization_percent"] = (
    autoscaler_cpu_utilization["autoscaler_cpu_utilization"] * 100
)
autoscaler_cpu_capacity["autoscaler_cpu_capacity_percent"] = (
    autoscaler_cpu_capacity["autoscaler_cpu_capacity"] * 100
)

network_received_bytes["network_received_kib_per_second"] = (
    network_received_bytes["network_received_bytes_per_second"] / 1024
)
network_sent_bytes["network_sent_kib_per_second"] = (
    network_sent_bytes["network_sent_bytes_per_second"] / 1024
)
disk_read_bytes["disk_read_kib_per_second"] = (
    disk_read_bytes["disk_read_bytes_per_second"] / 1024
)
disk_write_bytes["disk_write_kib_per_second"] = (
    disk_write_bytes["disk_write_bytes_per_second"] / 1024
)

saved_paths = []

fig, ax = plt.subplots(figsize=(8, 5))
if not summary_df.empty:
    ax.plot(
        summary_df["jobs_submitted"],
        summary_df["avg_vm_count_observed"],
        marker="o",
        markersize=LINE_MARKER_SIZE,
        label="Average observed MIG size",
    )
    range_df = summary_df.dropna(
        subset=[
            "min_vm_count_observed",
            "max_vm_count_observed",
        ]
    )
    if not range_df.empty:
        ax.fill_between(
            range_df["jobs_submitted"],
            range_df["min_vm_count_observed"],
            range_df["max_vm_count_observed"],
            alpha=0.2,
            label="Observed min-max range",
        )

ax.set_xlabel("Submitted requests")
ax.set_ylabel("Observed MIG group size (VMs)")
ax.set_title("Observed MIG Group Size vs Submitted Requests")
ax.set_ylim(bottom=0)
ax.grid(True)
add_legend(ax)
save_plot("group_size_vs_submitted_requests.png")

actual_group_size_changes = get_change_points(actual_group_size, "actual_group_size")
recommended_size_changes = get_change_points(recommended_size, "recommended_size")
visible_size_change_times = pd.concat(
    [
        actual_group_size_changes["time"],
        recommended_size_changes["time"],
    ],
    ignore_index=True,
)
visible_size_change_times = visible_size_change_times[
    (visible_size_change_times >= plot_start)
    & (visible_size_change_times <= plot_end)
]

scale_up_times = visible_size_change_times[visible_size_change_times <= benchmark_end]
scale_down_times = visible_size_change_times[visible_size_change_times > benchmark_end]
scale_up_start = max(plot_start, scale_up_times.min() - pd.Timedelta(minutes=2))
scale_up_end = min(plot_end, scale_up_times.max() + pd.Timedelta(minutes=2))
scale_down_start = max(plot_start, scale_down_times.min() - pd.Timedelta(minutes=2))
scale_down_end = min(plot_end, scale_down_times.max() + pd.Timedelta(minutes=2))


def plot_mig_size_panel(ax):
    plot_step_metric(
        ax,
        configured_min_size,
        "configured_min_size",
        "Configured min size",
        color="tab:blue",
        linestyle="--",
        linewidth=1.4,
        alpha=0.45,
    )
    plot_step_metric(
        ax,
        configured_max_size,
        "configured_max_size",
        "Configured max size",
        color="tab:orange",
        linestyle="--",
        linewidth=1.4,
        alpha=0.45,
    )
    plot_step_metric(
        ax,
        recommended_size,
        "recommended_size",
        "Autoscaler recommended size",
        color="tab:green",
        linewidth=2,
        mark_changes=True,
    )
    plot_step_metric(
        ax,
        actual_group_size,
        "actual_group_size",
        "Actual MIG size",
        color="tab:red",
        linewidth=2.5,
        mark_changes=True,
    )
    shade_benchmark(ax)
    ax.set_ylim(0.75, 6.25)


fig, axes = plt.subplots(
    1,
    2,
    figsize=(10, 4.8),
    sharey=True,
    gridspec_kw={"width_ratios": [1.15, 1]},
)
plot_mig_size_panel(axes[0])
plot_mig_size_panel(axes[1])
axes[0].set_ylabel("VM count")
fig.suptitle("Managed Instance Group Size Over Time")
format_time_axis(
    axes[0],
    start=scale_up_start,
    end=scale_up_end,
    tick_interval_minutes=2,
)
format_time_axis(
    axes[1],
    start=scale_down_start,
    end=scale_down_end,
    tick_interval_minutes=2,
)
axes[0].set_title("Scale-up")
axes[1].set_title("Scale-down")
axes[1].tick_params(axis="y", left=False, labelleft=False)
axes[0].spines["right"].set_visible(False)
axes[1].spines["left"].set_visible(False)
axes[1].yaxis.tick_right()

break_mark_style = {
    "marker": [(-1, -0.5), (1, 0.5)],
    "markersize": 12,
    "linestyle": "none",
    "color": "black",
    "mec": "black",
    "mew": 1,
    "clip_on": False,
}
axes[0].plot([1, 1], [0, 1], transform=axes[0].transAxes, **break_mark_style)
axes[1].plot([0, 0], [0, 1], transform=axes[1].transAxes, **break_mark_style)
add_legend(axes[0], fontsize=8, loc="upper left")
save_plot("mig_group_size_over_time.png")

fig, ax = plt.subplots(figsize=(12, 5))
plot_metric(ax, recommended_size, "recommended_size", "Autoscaler recommended size")
plot_metric(ax, actual_group_size, "actual_group_size", "Actual MIG size")
shade_benchmark(ax)
ax.set_ylabel("VM count")
ax.set_title("Autoscaler Recommended Size vs Actual MIG Size")
format_time_axis(ax)
add_legend(ax)
save_plot("autoscaler_recommended_vs_actual_size.png")

fig, ax = plt.subplots(figsize=(12, 5))
size_ax = ax.twinx()
plot_metric(ax, cpu_utilization, "cpu_utilization_percent", "CPU utilization")
plot_metric(
    size_ax,
    actual_group_size,
    "actual_group_size",
    "Actual VM count",
    color="tab:orange",
)
plot_metric(
    size_ax,
    recommended_size,
    "recommended_size",
    "Recommended VM count",
    color="tab:green",
    linestyle="--",
)
ax.set_ylabel("CPU utilization (%)")
size_ax.set_ylabel("VM count")
ax.set_title("CPU Utilization and MIG Size Over Time")
ax.set_ylim(bottom=0)
size_ax.set_ylim(bottom=0)
color_axis(size_ax, "tab:orange")
format_time_axis(ax)
add_legend(
    ax,
    size_ax,
    loc="lower right",
    fontsize=8,
    framealpha=0.9,
)
save_plot("cpu_utilization_and_mig_size.png")

fig, ax = plt.subplots(figsize=(12, 5))
plot_metric(
    ax,
    autoscaler_cpu_utilization,
    "autoscaler_cpu_utilization_percent",
    "Autoscaler observed CPU signal",
)
plot_metric(
    ax,
    autoscaler_cpu_capacity,
    "autoscaler_cpu_capacity_percent",
    "Autoscaler CPU capacity signal",
)
shade_benchmark(ax)
ax.set_ylabel("Summed CPU utilization signal (%-points)")
ax.set_title("Autoscaler CPU Signal vs Capacity")
ax.set_ylim(bottom=0)
format_time_axis(ax)
add_legend(ax)
save_plot("autoscaler_cpu_utilization_vs_target.png")

fig, ax = plt.subplots(figsize=(12, 5))
plot_metric(
    ax,
    network_received_bytes,
    "network_received_kib_per_second",
    "Received",
)
plot_metric(ax, network_sent_bytes, "network_sent_kib_per_second", "Sent")
shade_benchmark(ax)
ax.set_ylabel("Network throughput (KiB/sec)")
ax.set_title("Network Bytes Rate Over Time")
ax.set_ylim(bottom=0)
format_time_axis(
    ax,
    start=network_plot_start,
    end=network_plot_end,
    tick_interval_minutes=1,
)
add_legend(ax)
save_plot("network_bytes_rate_over_time.png")

fig, ax = plt.subplots(figsize=(12, 5))
plot_metric(
    ax,
    network_received_packets,
    "network_received_packets_per_second",
    "Received",
)
plot_metric(ax, network_sent_packets, "network_sent_packets_per_second", "Sent")
shade_benchmark(ax)
ax.set_ylabel("Network packets/sec")
ax.set_title("Network Packet Rate Over Time")
ax.set_ylim(bottom=0)
format_time_axis(
    ax,
    start=network_plot_start,
    end=network_plot_end,
    tick_interval_minutes=1,
)
add_legend(ax)
save_plot("network_packets_rate_over_time.png")

fig, ax = plt.subplots(figsize=(12, 5))
plot_metric(ax, disk_read_bytes, "disk_read_kib_per_second", "Read")
plot_metric(ax, disk_write_bytes, "disk_write_kib_per_second", "Write")
shade_benchmark(ax)
ax.set_ylabel("Disk throughput (KiB/sec)")
ax.set_title("Disk Bytes Rate Over Time")
ax.set_ylim(bottom=0)
format_time_axis(ax)
add_legend(ax)
save_plot("disk_bytes_rate_over_time.png")

fig, ax = plt.subplots(figsize=(12, 5))
plot_metric(ax, disk_read_ops, "disk_read_ops_per_second", "Read")
plot_metric(ax, disk_write_ops, "disk_write_ops_per_second", "Write")
shade_benchmark(ax)
ax.set_ylabel("Disk operations/sec")
ax.set_title("Disk I/O Operations Rate Over Time")
ax.set_ylim(bottom=0)
format_time_axis(ax)
add_legend(ax)
save_plot("disk_ops_rate_over_time.png")

fig, axes = plt.subplots(4, 1, figsize=(12, 9), sharex=True)
plot_metric(axes[0], actual_group_size, "actual_group_size", "Actual MIG size")
axes[0].set_ylabel("VMs")
axes[0].set_title("GCP Resource Activity Overview")

plot_metric(axes[1], cpu_utilization, "cpu_utilization_percent", "Mean VM CPU")
axes[1].set_ylabel("CPU (%)")

plot_metric(
    axes[2],
    network_received_bytes,
    "network_received_kib_per_second",
    "Network received",
)
plot_metric(axes[2], network_sent_bytes, "network_sent_kib_per_second", "Network sent")
axes[2].set_ylabel("KiB/sec")

plot_metric(axes[3], disk_read_ops, "disk_read_ops_per_second", "Disk reads")
plot_metric(axes[3], disk_write_ops, "disk_write_ops_per_second", "Disk writes")
axes[3].set_ylabel("Ops/sec")

for ax in axes:
    shade_benchmark(ax)
    ax.grid(True)
    add_legend(ax)

format_time_axis(axes[-1])
save_plot("gcp_resource_activity_overview.png")

print(f"Benchmark window ({LOCAL_TIMEZONE}): {benchmark_start} to {benchmark_end}")
for path in saved_paths:
    print(path)
