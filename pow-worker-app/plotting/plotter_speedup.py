import pandas as pd
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "speedup_plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

results_dir = BASE_DIR.parent / "results" / "workload" / "20260609T153533529553Z"
summary_path = results_dir / "run_summaries.csv"

df = pd.read_csv(summary_path)

required_columns = [
    "target_rps",
    "jobs_submitted",
    "completed_throughput_rps",
]

for column in required_columns:
    df[column] = pd.to_numeric(df[column], errors="coerce")

df = df.dropna(subset=required_columns)
df = df.sort_values("jobs_submitted")

baseline_rows = df[df["target_rps"] == 1]
if baseline_rows.empty:
    raise ValueError("Missing baseline row where target_rps == 1")

baseline_row = baseline_rows.iloc[0]
baseline_completed_throughput_rps = baseline_row["completed_throughput_rps"]
baseline_jobs_submitted = baseline_row["jobs_submitted"]

if baseline_completed_throughput_rps <= 0:
    raise ValueError("Baseline completed_throughput_rps must be positive")

if baseline_jobs_submitted <= 0:
    raise ValueError("Baseline jobs_submitted must be positive")


def safe_divide(numerator, denominator):
    denominator = denominator.where(denominator != 0)
    return numerator.divide(denominator)


# Speedup is based on completed throughput, not request submission rate or worker count.
df["speedup"] = df["completed_throughput_rps"] / baseline_completed_throughput_rps
df["ideal_speedup"] = df["jobs_submitted"] / baseline_jobs_submitted
df["scaling_efficiency"] = safe_divide(df["speedup"], df["ideal_speedup"])

summary_columns = [
    "target_rps",
    "jobs_submitted",
    "completed_throughput_rps",
    "speedup",
    "ideal_speedup",
    "scaling_efficiency",
]

print(df[summary_columns].to_string(index=False))

saved_paths = []


def plot_line(x_column, y_column, label):
    plot_df = df.dropna(subset=[x_column, y_column])
    if plot_df.empty:
        return

    plt.plot(plot_df[x_column], plot_df[y_column], marker="o", label=label)


def save_plot(filename):
    path = OUTPUT_DIR / filename
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(path, dpi=300)
    plt.close()
    saved_paths.append(path)


plt.figure(figsize=(8, 5))
plot_line("jobs_submitted", "speedup", "Measured speedup")
plot_line("jobs_submitted", "ideal_speedup", "Ideal linear speedup")
plt.xlabel("Submitted requests")
plt.ylabel("Speedup relative to 1 RPS baseline")
plt.title("Speedup vs Submitted Requests")
save_plot("speedup_vs_submitted_requests.png")

plt.figure(figsize=(8, 5))
plot_line("jobs_submitted", "scaling_efficiency", "Scaling efficiency")
plt.xlabel("Submitted requests")
plt.ylabel("Scaling efficiency")
plt.title("Speedup Efficiency vs Submitted Requests")
save_plot("speedup_efficiency_vs_submitted_requests.png")

for path in saved_paths:
    print(path)
