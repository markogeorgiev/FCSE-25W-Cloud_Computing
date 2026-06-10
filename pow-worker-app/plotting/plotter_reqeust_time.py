import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "request_time_plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

results_dir = BASE_DIR.parent / "results" / "workload" / "20260609T153533529553Z"
summary_path = results_dir / "run_summaries.csv"

df = pd.read_csv(summary_path)
df = df.sort_values("jobs_submitted")

plt.figure(figsize=(8, 5))
plt.plot(df["jobs_submitted"], df["avg_server_end_to_end_time_ms"] / 1000, marker="o", label="Average")
plt.plot(df["jobs_submitted"], df["p50_server_end_to_end_time_ms"] / 1000, marker="o", label="P50")
plt.plot(df["jobs_submitted"], df["p95_server_end_to_end_time_ms"] / 1000, marker="o", label="P95")
plt.xlabel("Number of requests submitted")
plt.ylabel("Time to completion (seconds)")
plt.title("Request-to-Completion Time vs Submitted Requests")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "request_to_completion_time.png", dpi=300)
plt.show()

plt.figure(figsize=(8, 5))
plt.plot(df["jobs_submitted"], df["avg_execution_time_ms"] / 1000, marker="o", label="Average")
plt.plot(df["jobs_submitted"], df["p50_execution_time_ms"] / 1000, marker="o", label="P50")
plt.plot(df["jobs_submitted"], df["p95_execution_time_ms"] / 1000, marker="o", label="P95")
plt.xlabel("Number of requests submitted")
plt.ylabel("Computation time (seconds)")
plt.title("Proof-of-Work Computation Time vs Submitted Requests")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "pow_computation_time.png", dpi=300)
plt.show()