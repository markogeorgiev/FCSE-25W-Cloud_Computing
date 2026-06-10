# Cloud Proof-of-Work Job Processing Service

This is a Python 3.11+ FastAPI service for queueing and processing proof-of-work jobs on a single VM while storing all queue state, results, and benchmark metrics in a remote PostgreSQL DBaaS.

The VM runs both the HTTP API and the worker manager. The API inserts jobs into PostgreSQL with `status = 'queued'`, and local CPU worker processes claim jobs safely with PostgreSQL row locks, compute proof-of-work, and write the result back to the same database. PostgreSQL is the shared source of truth, so multiple worker processes do not process the same job twice.

## Project Layout

```text
pow-worker-app/
├── app/
│   ├── main.py
│   ├── database.py
│   ├── models.py
│   ├── pow.py
│   ├── worker.py
│   ├── stats.py
│   └── config.py
├── sql/
│   └── schema.sql
├── tests/
│   ├── test_pow.py
│   └── test_api.py
├── requirements.txt
├── README.md
└── .env.example
```

## Setup

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

On Linux or macOS:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Configure PostgreSQL

Copy the example environment file and edit it with your remote DBaaS credentials:

```powershell
Copy-Item .env.example .env
```

Set `DATABASE_URL` in this format:

```text
postgresql://USER:PASSWORD@HOST:PORT/DATABASE
```

Never commit real database credentials. The app reads configuration from environment variables, including:

```text
DATABASE_URL
WORKER_COUNT
POLL_INTERVAL_SECONDS
DEFAULT_DIFFICULTY
API_HOST
API_PORT
```

## Create The Schema

Run the schema against your PostgreSQL database:

```powershell
psql "$env:DATABASE_URL" -f sql/schema.sql
```

On Linux or macOS:

```bash
psql "$DATABASE_URL" -f sql/schema.sql
```

The schema creates:

- `pow_jobs` for queued, running, completed, and failed proof-of-work jobs.
- `benchmark_runs` for later load-test and speedup experiment summaries.
- Indexes on `pow_jobs(status)`, `pow_jobs(created_at)`, and `pow_jobs(status, created_at)`.

## Run The API And Workers

Start the app from the project directory:

```powershell
python -m app.main
```

The FastAPI API and the local worker manager start together. Keep `uvicorn --workers` at `1`; use `WORKER_COUNT` to control proof-of-work concurrency instead.

For example, run four CPU worker processes:

```powershell
$env:WORKER_COUNT = "4"
python -m app.main
```

## Submit A Proof-Of-Work Job

```bash
curl -X POST http://localhost:8000/pow \
  -H "Content-Type: application/json" \
  -d '{"input_data":"some payload","difficulty":6}'
```

Response:

```json
{
  "job_id": "00000000-0000-0000-0000-000000000000",
  "status": "queued"
}
```

If `difficulty` is omitted, the app uses `DEFAULT_DIFFICULTY`. The default difficulty is `6`, which is intentionally CPU-bound and is suitable for multi-second worker experiments on a typical VM.

## Check Job Status

```bash
curl http://localhost:8000/pow/00000000-0000-0000-0000-000000000000
```

Completed jobs include:

- `nonce`
- `result_hash`
- `execution_time_ms`
- `end_to_end_time_ms`
- `finished_at`

## Health And Stats

```bash
curl http://localhost:8000/health
curl http://localhost:8000/stats
```

`/stats` returns queued, running, completed, and failed job counts plus average execution and end-to-end times.

## Worker Safety

Workers claim jobs with:

```sql
SELECT id
FROM pow_jobs
WHERE status = 'queued'
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

That row lock pattern lets multiple worker processes safely compete for work without processing the same job twice.

## Speedup Experiments

Change `WORKER_COUNT` to run experiments with `1`, `2`, `4`, `8`, or another VM-appropriate value:

```powershell
$env:WORKER_COUNT = "8"
python -m app.main
```

For later plots, use these fields:

- `execution_time_ms` from `pow_jobs`
- `end_to_end_time_ms` from `pow_jobs`
- `throughput_rps` from `benchmark_runs`
- `speedup` from `benchmark_runs`

## Run Tests

```powershell
pytest
```

The proof-of-work tests run without a database. The API tests use mocked database access so they can verify request handling and queued job creation without a live DBaaS connection.

## Workload Evaluation

The standalone async workload generator is in:

```text
evaluation_scripts/workload_generator.py
```

Run the full official benchmark against the GCP load balancer:

```powershell
python evaluation_scripts/workload_generator.py --base-url http://8.233.134.116 --difficulty 6 --jobs-per-rate 120 --worker-count-per-vm 2
```

Preview the schedule without sending requests:

```powershell
python evaluation_scripts/workload_generator.py --dry-run
```

By default, it tests the official RPS levels `1,10,50,100,200,300,400,500,600,700,800,900,1000` with exactly `120` submitted jobs per rate. It submits asynchronously through the load balancer, polls `GET /pow/{job_id}` until jobs finish or time out, and writes results under `results/workload/<timestamp>/`.

To run each target RPS for a fixed submission duration instead, use the additional duration-based generator:

```powershell
python.exe evaluation_scripts/duration_workload_generator.py --base-url http://8.233.134.116 --difficulty 5 --duration-per-rate-seconds 5 --worker-count-per-vm 2 --sample-vm-count --mig-name pow-worker-mig --mig-zone europe-west1-b
```

With `--duration-per-rate-seconds 5`, the script submits `target_rps * 5` jobs at each level. For example, `1` RPS submits `5` jobs, `100` RPS submits `500` jobs, and `1000` RPS submits `5000` jobs. The submission window is fixed; total completion time can be longer because the script still waits for every submitted job to complete, fail, or time out.

Output files:

- `raw_requests.csv` and `raw_requests.jsonl` contain one row/object per submitted job.
- `run_summaries.csv` and `run_summaries.json` contain one summary per target RPS.
- `experiment_metadata.json` records configuration and methodology notes.

Optional VM count sampling:

```powershell
python evaluation_scripts/workload_generator.py --sample-vm-count --mig-name YOUR_MIG_NAME --mig-zone YOUR_ZONE
```

The generator benchmarks the live autoscaling Managed Instance Group as deployed behind one load balancer. It intentionally does not run fixed 1 VM vs 2 VM vs 3 VM experiments.

How to use the results:

- Workload Generator: `raw_requests.csv` proves every request went through POST, polling, and terminal job state collection.
- Speedup: calculate later from completed throughput or p95 end-to-end time across autoscaling behavior.
- Execution Time: plot `p95_client_end_to_end_time_ms` or `p95_server_end_to_end_time_ms` by `target_rps`.
- Throughput: plot `completed_throughput_rps` by `target_rps`.
- Scalability: discuss throughput, p95 latency, queue behavior, and observed VM count as RPS increases.
- Elasticity: use observed VM count changes, backlog behavior, and cooldown recovery.
- Cost Analysis: combine VM counts/runtime with GCP pricing, then relate cost to throughput and latency.
