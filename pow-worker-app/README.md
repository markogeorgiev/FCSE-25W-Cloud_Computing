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

