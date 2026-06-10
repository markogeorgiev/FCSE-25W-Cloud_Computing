import pandas as pd
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "throughput_plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

results_dir = BASE_DIR.parent / "results" / "workload" / "20260609T153533529553Z"
summary_path = results_dir / "run_summaries.csv"
raw_requests_path = results_dir / "raw_requests.csv"

df = pd.read_csv(summary_path)
raw_df = pd.read_csv(raw_requests_path)


def safe_divide(numerator, denominator):
    denominator = denominator.where(denominator != 0)
    return numerator.divide(denominator)


numeric_columns = [
    "target_rps",
    "jobs_submitted",
    "jobs_completed",
    "submission_duration_seconds",
    "completed_throughput_rps",
    "avg_execution_time_ms",
    "avg_vm_count_observed",
    "worker_count_per_vm",
]

for column in numeric_columns:
    df[column] = pd.to_numeric(df[column], errors="coerce")

df = df.sort_values("jobs_submitted")

raw_numeric_columns = [
    "target_rps",
    "end_to_end_time_ms",
    "enqueue_time_ms",
]

for column in raw_numeric_columns:
    raw_df[column] = pd.to_numeric(raw_df[column], errors="coerce")

raw_df["submit_started_at"] = pd.to_datetime(
    raw_df["submit_started_at"],
    errors="coerce",
    utc=True,
)
raw_df["post_response_at"] = pd.to_datetime(
    raw_df["post_response_at"],
    errors="coerce",
    utc=True,
)

completed_raw_df = raw_df[raw_df["job_status"] == "completed"].copy()
completed_raw_df["post_response_or_estimate_at"] = completed_raw_df["post_response_at"]
missing_post_response = completed_raw_df["post_response_or_estimate_at"].isna()
completed_raw_df.loc[missing_post_response, "post_response_or_estimate_at"] = (
    completed_raw_df.loc[missing_post_response, "submit_started_at"]
    + pd.to_timedelta(
        completed_raw_df.loc[missing_post_response, "enqueue_time_ms"],
        unit="ms",
    )
)

# raw_requests.csv does not store the server's finished_at timestamp directly.
# The app records end_to_end_time_ms as database finished_at - database created_at,
# so this estimates request-sent-to-DB-finished time while including queue wait.
completed_raw_df["estimated_db_finished_at"] = (
    completed_raw_df["post_response_or_estimate_at"]
    + pd.to_timedelta(completed_raw_df["end_to_end_time_ms"], unit="ms")
)

completed_raw_df = completed_raw_df.dropna(
    subset=[
        "target_rps",
        "submit_started_at",
        "estimated_db_finished_at",
    ]
)

completion_windows = (
    completed_raw_df.groupby("target_rps")
    .agg(
        first_request_sent_at=("submit_started_at", "min"),
        last_db_finished_at=("estimated_db_finished_at", "max"),
        raw_jobs_completed=("estimated_db_finished_at", "count"),
    )
    .reset_index()
)
completion_windows["request_to_db_finish_makespan_seconds"] = (
    completion_windows["last_db_finished_at"]
    - completion_windows["first_request_sent_at"]
).dt.total_seconds()

completion_windows["request_to_db_finish_throughput_rps"] = safe_divide(
    completion_windows["raw_jobs_completed"],
    completion_windows["request_to_db_finish_makespan_seconds"],
)

df = df.merge(
    completion_windows[
        [
            "target_rps",
            "request_to_db_finish_makespan_seconds",
            "request_to_db_finish_throughput_rps",
        ]
    ],
    on="target_rps",
    how="left",
)

# Throughput is measured as completed jobs per second, not as the number of local worker processes.
# Completed throughput is measured from request send time to DB-finished time, so queue wait is included.
worker_count = df["avg_vm_count_observed"] * df["worker_count_per_vm"]
avg_execution_time_seconds = df["avg_execution_time_ms"] / 1000

df["submitted_throughput_rps"] = safe_divide(
    df["jobs_submitted"],
    df["submission_duration_seconds"],
)
df["ideal_throughput_rps"] = safe_divide(
    worker_count,
    avg_execution_time_seconds,
)
df["per_worker_completed_throughput_rps"] = safe_divide(
    df["request_to_db_finish_throughput_rps"],
    worker_count,
)
df["linear_submitted_load_rps"] = df["jobs_submitted"]

saved_paths = []


def plot_line(ax, x_column, y_column, label, **kwargs):
    plot_df = df.dropna(subset=[x_column, y_column])
    if plot_df.empty:
        return

    ax.plot(plot_df[x_column], plot_df[y_column], marker="o", label=label, **kwargs)


def add_legend(*axes):
    handles = []
    labels = []

    for ax in axes:
        axis_handles, axis_labels = ax.get_legend_handles_labels()
        handles.extend(axis_handles)
        labels.extend(axis_labels)

    if handles:
        axes[0].legend(handles, labels)


def color_axis(ax, color):
    ax.tick_params(axis="y", labelcolor=color)
    ax.yaxis.label.set_color(color)


def save_plot(filename):
    path = OUTPUT_DIR / filename
    plt.tight_layout()
    plt.savefig(path, dpi=300)
    plt.close()
    saved_paths.append(path)


fig, ax = plt.subplots(figsize=(8, 5))
load_ax = ax.twinx()
plot_line(
    ax,
    "jobs_submitted",
    "request_to_db_finish_throughput_rps",
    "Completed throughput including queue",
)
plot_line(ax, "jobs_submitted", "ideal_throughput_rps", "Ideal worker-capacity throughput")
plot_line(
    load_ax,
    "jobs_submitted",
    "submitted_throughput_rps",
    "Submitted throughput (right axis)",
    linestyle="--",
    color="tab:green",
)
ax.set_xlabel("Submitted requests")
ax.set_ylabel("Throughput (jobs/sec)")
load_ax.set_ylabel("Submitted throughput (jobs/sec)")
ax.set_title("Throughput vs Submitted Requests")
ax.set_ylim(bottom=0)
load_ax.set_ylim(bottom=0)
color_axis(load_ax, "tab:green")
ax.grid(True)
add_legend(ax, load_ax)
save_plot("throughput_vs_submitted_requests.png")

fig, ax = plt.subplots(figsize=(8, 5))
load_ax = ax.twinx()
plot_line(
    ax,
    "target_rps",
    "request_to_db_finish_throughput_rps",
    "Completed throughput including queue",
)
plot_line(
    load_ax,
    "target_rps",
    "target_rps",
    "Target RPS reference (right axis)",
    linestyle="--",
    color="tab:orange",
)
ax.set_xlabel("Target request rate (requests/sec)")
ax.set_ylabel("Completed throughput (jobs/sec)")
load_ax.set_ylabel("Target request rate (requests/sec)")
ax.set_title("Completed Throughput vs Target Request Rate")
ax.set_ylim(bottom=0)
load_ax.set_ylim(bottom=0)
color_axis(load_ax, "tab:orange")
ax.grid(True)
add_legend(ax, load_ax)
save_plot("completed_throughput_vs_target_rps.png")

fig, ax = plt.subplots(figsize=(8, 5))
plot_line(
    ax,
    "jobs_submitted",
    "per_worker_completed_throughput_rps",
    "Per-worker completed throughput",
)
ax.set_xlabel("Submitted requests")
ax.set_ylabel("Completed throughput per worker (jobs/sec/worker)")
ax.set_title("Per-Worker Completed Throughput")
ax.set_ylim(bottom=0)
ax.grid(True)
add_legend(ax)
save_plot("per_worker_throughput.png")

fig, ax = plt.subplots(figsize=(8, 5))
load_ax = ax.twinx()
plot_line(
    ax,
    "jobs_submitted",
    "request_to_db_finish_throughput_rps",
    "Actual completed throughput including queue",
)
plot_line(ax, "jobs_submitted", "ideal_throughput_rps", "Ideal worker-capacity throughput")
plot_line(
    load_ax,
    "jobs_submitted",
    "linear_submitted_load_rps",
    "Linear submitted-load ideal (right axis)",
    linestyle="--",
    color="tab:green",
)
ax.set_xlabel("Submitted requests")
ax.set_ylabel("Throughput (jobs/sec)")
load_ax.set_ylabel("Linear submitted-load ideal (jobs/sec)")
ax.set_title("Ideal Throughput vs Actual")
ax.set_ylim(bottom=0)
load_ax.set_ylim(bottom=0)
color_axis(load_ax, "tab:green")
ax.grid(True)
add_legend(ax, load_ax)
save_plot("ideal_throughput_vs_actual.png")

for path in saved_paths:
    print(path)
