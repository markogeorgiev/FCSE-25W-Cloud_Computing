from __future__ import annotations

"""
Standalone asynchronous workload generator for the Cloud Proof-of-Work Job Processing Service.

The benchmark targets one live autoscaling Managed Instance Group behind one load
balancer. It does not run fixed 1 VM vs 2 VM vs 3 VM trials. The CSV and JSON
outputs are intended for later analysis of speedup, execution time, throughput,
scalability, elasticity, and cost.
"""

import argparse
import asyncio
import csv
import json
import os
import secrets
import shutil
import statistics
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


OFFICIAL_TARGET_RPS = [1, 10, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
VM_SAMPLE_INTERVAL_SECONDS = 5.0


RAW_REQUEST_FIELDS = [
    "request_id",
    "job_id",
    "experiment_id",
    "target_rps",
    "run_label",
    "request_seq",
    "vm_count",
    "worker_count_per_vm",
    "submit_started_at",
    "post_response_at",
    "job_completed_at",
    "http_status",
    "job_status",
    "execution_time_ms",
    "end_to_end_time_ms",
    "worker_id",
    "nonce",
    "result_hash",
    "enqueue_time_ms",
    "client_end_to_end_time_ms",
    "poll_count",
    "error_message",
]


RUN_SUMMARY_FIELDS = [
    "experiment_id",
    "run_label",
    "target_rps",
    "jobs_submitted",
    "jobs_completed",
    "jobs_failed",
    "jobs_timed_out",
    "success_rate",
    "failed_rate",
    "first_submission_time",
    "last_submission_time",
    "first_completion_time",
    "last_completion_time",
    "submission_duration_seconds",
    "completion_makespan_seconds",
    "completed_throughput_rps",
    "avg_enqueue_time_ms",
    "p50_enqueue_time_ms",
    "p95_enqueue_time_ms",
    "avg_client_end_to_end_time_ms",
    "p50_client_end_to_end_time_ms",
    "p95_client_end_to_end_time_ms",
    "avg_execution_time_ms",
    "p50_execution_time_ms",
    "p95_execution_time_ms",
    "avg_server_end_to_end_time_ms",
    "p50_server_end_to_end_time_ms",
    "p95_server_end_to_end_time_ms",
    "min_vm_count_observed",
    "max_vm_count_observed",
    "avg_vm_count_observed",
    "worker_count_per_vm",
]


@dataclass
class RequestResult:
    request_id: str
    job_id: str | None
    experiment_id: str
    target_rps: int
    run_label: str
    request_seq: int
    vm_count: int | None
    worker_count_per_vm: int
    submit_started_at: str | None
    post_response_at: str | None
    job_completed_at: str | None
    http_status: int | None
    job_status: str
    execution_time_ms: int | None
    end_to_end_time_ms: int | None
    worker_id: str | None
    nonce: int | None
    result_hash: str | None
    enqueue_time_ms: float | None
    client_end_to_end_time_ms: float | None
    poll_count: int
    error_message: str | None


@dataclass
class RunSummary:
    experiment_id: str
    run_label: str
    target_rps: int
    jobs_submitted: int
    jobs_completed: int
    jobs_failed: int
    jobs_timed_out: int
    success_rate: float
    failed_rate: float
    first_submission_time: str | None
    last_submission_time: str | None
    first_completion_time: str | None
    last_completion_time: str | None
    submission_duration_seconds: float | None
    completion_makespan_seconds: float | None
    completed_throughput_rps: float | None
    avg_enqueue_time_ms: float | None
    p50_enqueue_time_ms: float | None
    p95_enqueue_time_ms: float | None
    avg_client_end_to_end_time_ms: float | None
    p50_client_end_to_end_time_ms: float | None
    p95_client_end_to_end_time_ms: float | None
    avg_execution_time_ms: float | None
    p50_execution_time_ms: float | None
    p95_execution_time_ms: float | None
    avg_server_end_to_end_time_ms: float | None
    p50_server_end_to_end_time_ms: float | None
    p95_server_end_to_end_time_ms: float | None
    min_vm_count_observed: int | None
    max_vm_count_observed: int | None
    avg_vm_count_observed: float | None
    worker_count_per_vm: int


@dataclass
class VmObservation:
    observed_at: str
    observed_at_monotonic: float
    vm_count: int


@dataclass(frozen=True)
class DbMetadataUpdate:
    job_id: str
    submitted_at: str
    post_response_at: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return format_utc(utc_now())


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def milliseconds_between(start: str | None, end: str | None) -> float | None:
    start_dt = parse_utc(start)
    end_dt = parse_utc(end)
    if start_dt is None or end_dt is None:
        return None
    return (end_dt - start_dt).total_seconds() * 1000.0


def seconds_between(start: str | None, end: str | None) -> float | None:
    ms = milliseconds_between(start, end)
    if ms is None:
        return None
    return ms / 1000.0


def parse_target_rps(value: str | None) -> list[int]:
    if value is None or value.strip() == "":
        return OFFICIAL_TARGET_RPS

    rates: list[int] = []
    for item in value.split(","):
        rate = int(item.strip())
        if rate <= 0:
            raise argparse.ArgumentTypeError("target RPS values must be positive")
        rates.append(rate)
    return rates


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def average(values: list[float]) -> float | None:
    if not values:
        return None
    return statistics.fmean(values)


def percentile(values: list[float], percentile_value: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]

    ordered = sorted(values)
    rank = (len(ordered) - 1) * (percentile_value / 100.0)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def as_csv_row(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 3)
    return value


class OptionalDbMetadataUpdater:
    def __init__(self) -> None:
        self.enabled = False
        self._queue: asyncio.Queue[DbMetadataUpdate | None] = asyncio.Queue()
        self._conn: Any | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            print("DATABASE_URL not set; skipping optional pow_jobs timestamp updates.")
            return

        try:
            import psycopg  # type: ignore[import-not-found]
        except ImportError:
            print("psycopg is not installed; skipping optional pow_jobs timestamp updates.")
            return

        try:
            self._conn = await psycopg.AsyncConnection.connect(database_url)
            self.enabled = True
            self._task = asyncio.create_task(self._run())
            print("Optional DATABASE_URL metadata updater enabled.")
        except Exception as exc:
            print(f"Could not enable optional DB metadata updates: {exc}")

    def enqueue(self, update: DbMetadataUpdate) -> None:
        if self.enabled:
            self._queue.put_nowait(update)

    async def flush(self) -> None:
        if self.enabled:
            await self._queue.join()

    async def stop(self) -> None:
        if not self.enabled:
            return

        await self.flush()
        await self._queue.put(None)

        if self._task is not None:
            await self._task

        if self._conn is not None:
            await self._conn.close()

    async def _run(self) -> None:
        while True:
            update = await self._queue.get()
            try:
                if update is None:
                    return
                await self._write_update(update)
            finally:
                self._queue.task_done()

    async def _write_update(self, update: DbMetadataUpdate) -> None:
        if self._conn is None:
            return

        submitted_at = parse_utc(update.submitted_at)
        post_response_at = parse_utc(update.post_response_at)
        if submitted_at is None or post_response_at is None:
            return

        submitted_at = submitted_at.replace(tzinfo=None)
        post_response_at = post_response_at.replace(tzinfo=None)

        try:
            async with self._conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE pow_jobs
                    SET submitted_at = %s,
                        post_response_at = %s
                    WHERE id = %s
                    """,
                    (submitted_at, post_response_at, update.job_id),
                )
            await self._conn.commit()
        except Exception as exc:
            await self._conn.rollback()
            print(f"Optional DB metadata update failed for job {update.job_id}: {exc}")


class VmCountSampler:
    def __init__(self, mig_name: str | None, mig_zone: str | None) -> None:
        self.mig_name = mig_name
        self.mig_zone = mig_zone
        self.current_count: int | None = None
        self.observations: list[VmObservation] = []
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    def can_start(self) -> bool:
        return bool(self.mig_name and self.mig_zone and self._find_gcloud())

    async def start(self) -> None:
        if not self.mig_name or not self.mig_zone:
            print("VM sampling requested, but --mig-name and --mig-zone are required.")
            return

        if not self._find_gcloud():
            print("VM sampling requested, but gcloud was not found. Continuing without VM counts.")
            return

        self._task = asyncio.create_task(self._run())
        print(f"VM count sampling enabled for MIG {self.mig_name} in zone {self.mig_zone}.")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task

    def observations_between(self, start_monotonic: float, end_monotonic: float) -> list[VmObservation]:
        return [
            observation
            for observation in self.observations
            if start_monotonic <= observation.observed_at_monotonic <= end_monotonic
        ]

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            count = await self._sample_once()
            if count is not None:
                self.current_count = count
                self.observations.append(
                    VmObservation(
                        observed_at=utc_now_iso(),
                        observed_at_monotonic=time.monotonic(),
                        vm_count=count,
                    )
                )
            await asyncio.sleep(VM_SAMPLE_INTERVAL_SECONDS)

    async def _sample_once(self) -> int | None:
        gcloud = self._find_gcloud()
        if not gcloud or not self.mig_name or not self.mig_zone:
            return None

        command = [
            gcloud,
            "compute",
            "instance-groups",
            "managed",
            "list-instances",
            self.mig_name,
            "--zone",
            self.mig_zone,
            "--format=json",
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
        except Exception as exc:
            print(f"VM count sampling failed: {exc}")
            return None

        if process.returncode != 0:
            message = stderr.decode("utf-8", errors="replace").strip()
            print(f"VM count sampling command failed: {message}")
            return None

        try:
            instances = json.loads(stdout.decode("utf-8"))
        except json.JSONDecodeError as exc:
            print(f"VM count sampling returned invalid JSON: {exc}")
            return None

        return count_running_healthy_instances(instances)

    @staticmethod
    def _find_gcloud() -> str | None:
        return shutil.which("gcloud") or shutil.which("gcloud.cmd")


def count_running_healthy_instances(instances: list[dict[str, Any]]) -> int:
    count = 0
    for instance in instances:
        status = instance.get("status") or instance.get("instanceStatus")
        if status and status != "RUNNING":
            continue

        health = instance.get("healthState") or instance.get("healthStatus")
        if isinstance(health, list):
            health_values = [
                item.get("healthState") or item.get("healthStatus")
                for item in health
                if isinstance(item, dict)
            ]
            if health_values and any(value != "HEALTHY" for value in health_values):
                continue
        elif isinstance(health, str) and health and health != "HEALTHY":
            continue

        count += 1
    return count


def build_payload(
    *,
    experiment_id: str,
    target_rps: int,
    run_label: str,
    request_seq: int,
    difficulty: int,
    submitted_at: str,
    include_metadata: bool,
) -> dict[str, Any]:
    input_data = secrets.token_hex(3)[:5]

    payload: dict[str, Any] = {
        "input_data": input_data,
        "difficulty": difficulty,
    }

    if include_metadata:
        payload.update(
            {
                "experiment_id": experiment_id,
                "target_rps": target_rps,
                "run_label": run_label,
                "submitted_at": submitted_at,
            }
        )

    return payload


async def submit_one_job(
    *,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    experiment_id: str,
    target_rps: int,
    run_label: str,
    request_seq: int,
    difficulty: int,
    worker_count_per_vm: int,
    vm_sampler: VmCountSampler | None,
    db_updater: OptionalDbMetadataUpdater,
) -> RequestResult:
    request_id = str(uuid.uuid4())
    submit_started_at = utc_now_iso()
    submit_started_monotonic = time.monotonic()
    vm_count = vm_sampler.current_count if vm_sampler is not None else None

    result = RequestResult(
        request_id=request_id,
        job_id=None,
        experiment_id=experiment_id,
        target_rps=target_rps,
        run_label=run_label,
        request_seq=request_seq,
        vm_count=vm_count,
        worker_count_per_vm=worker_count_per_vm,
        submit_started_at=submit_started_at,
        post_response_at=None,
        job_completed_at=None,
        http_status=None,
        job_status="submitted",
        execution_time_ms=None,
        end_to_end_time_ms=None,
        worker_id=None,
        nonce=None,
        result_hash=None,
        enqueue_time_ms=None,
        client_end_to_end_time_ms=None,
        poll_count=0,
        error_message=None,
    )

    rich_payload = build_payload(
        experiment_id=experiment_id,
        target_rps=target_rps,
        run_label=run_label,
        request_seq=request_seq,
        difficulty=difficulty,
        submitted_at=submit_started_at,
        include_metadata=True,
    )

    minimal_payload = build_payload(
        experiment_id=experiment_id,
        target_rps=target_rps,
        run_label=run_label,
        request_seq=request_seq,
        difficulty=difficulty,
        submitted_at=submit_started_at,
        include_metadata=False,
    )

    async with semaphore:
        try:
            response = await client.post("/pow", json=rich_payload)
            if response.status_code in {400, 422}:
                response = await client.post("/pow", json=minimal_payload)
        except httpx.HTTPError as exc:
            result.post_response_at = utc_now_iso()
            result.enqueue_time_ms = (time.monotonic() - submit_started_monotonic) * 1000.0
            result.job_status = "failed"
            result.error_message = f"POST /pow failed: {exc}"
            return result

    result.post_response_at = utc_now_iso()
    result.enqueue_time_ms = (time.monotonic() - submit_started_monotonic) * 1000.0
    result.http_status = response.status_code

    if response.status_code < 200 or response.status_code >= 300:
        result.job_status = "failed"
        result.error_message = f"POST /pow returned HTTP {response.status_code}: {response.text[:500]}"
        return result

    try:
        body = response.json()
    except json.JSONDecodeError as exc:
        result.job_status = "failed"
        result.error_message = f"POST /pow returned malformed JSON: {exc}"
        return result

    job_id = body.get("job_id") or body.get("id")
    if not job_id:
        result.job_status = "failed"
        result.error_message = "POST /pow response did not include job_id or id"
        return result

    result.job_id = str(job_id)
    result.job_status = body.get("status", "queued")

    db_updater.enqueue(
        DbMetadataUpdate(
            job_id=result.job_id,
            submitted_at=submit_started_at,
            post_response_at=result.post_response_at,
        )
    )

    return result


async def submit_jobs_at_rate(
    *,
    client: httpx.AsyncClient,
    experiment_id: str,
    target_rps: int,
    run_label: str,
    jobs_per_rate: int,
    difficulty: int,
    worker_count_per_vm: int,
    max_in_flight: int,
    vm_sampler: VmCountSampler | None,
    db_updater: OptionalDbMetadataUpdater,
) -> list[RequestResult]:
    semaphore = asyncio.Semaphore(max_in_flight)
    interval = 1.0 / float(target_rps)
    schedule_start = time.monotonic()
    tasks: list[asyncio.Task[RequestResult]] = []

    for request_seq in range(1, jobs_per_rate + 1):
        scheduled_at = schedule_start + ((request_seq - 1) * interval)
        delay = scheduled_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)

        tasks.append(
            asyncio.create_task(
                submit_one_job(
                    client=client,
                    semaphore=semaphore,
                    experiment_id=experiment_id,
                    target_rps=target_rps,
                    run_label=run_label,
                    request_seq=request_seq,
                    difficulty=difficulty,
                    worker_count_per_vm=worker_count_per_vm,
                    vm_sampler=vm_sampler,
                    db_updater=db_updater,
                )
            )
        )

    return await asyncio.gather(*tasks)


async def poll_one_job(
    *,
    client: httpx.AsyncClient,
    result: RequestResult,
    poll_interval: float,
    job_timeout_seconds: float,
    vm_sampler: VmCountSampler | None,
) -> None:
    if result.job_id is None:
        return

    deadline = time.monotonic() + job_timeout_seconds
    network_error_count = 0
    last_error: str | None = None

    while time.monotonic() < deadline:
        try:
            response = await client.get(f"/pow/{result.job_id}")
            result.poll_count += 1
            network_error_count = 0

            if response.status_code == 404:
                last_error = "GET /pow/{job_id} returned 404"
                await asyncio.sleep(poll_interval)
                continue

            if response.status_code < 200 or response.status_code >= 300:
                last_error = f"GET /pow/{result.job_id} returned HTTP {response.status_code}"
                await asyncio.sleep(poll_interval)
                continue

            body = response.json()
            status = str(body.get("status", "unknown"))
            result.job_status = status

            if vm_sampler is not None and vm_sampler.current_count is not None:
                result.vm_count = vm_sampler.current_count

            if status in {"completed", "failed"}:
                result.job_completed_at = utc_now_iso()
                result.execution_time_ms = safe_int(body.get("execution_time_ms"))
                result.end_to_end_time_ms = safe_int(body.get("end_to_end_time_ms"))
                result.worker_id = body.get("worker_id")
                result.nonce = safe_int(body.get("nonce"))
                result.result_hash = body.get("result_hash")
                result.client_end_to_end_time_ms = milliseconds_between(
                    result.submit_started_at,
                    result.job_completed_at,
                )
                result.error_message = body.get("error_message") or last_error
                return

            await asyncio.sleep(poll_interval)
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            network_error_count += 1
            last_error = f"poll error: {exc}"
            backoff = min(poll_interval * network_error_count, 10.0)
            await asyncio.sleep(backoff)

    result.job_status = "timed_out"
    result.job_completed_at = utc_now_iso()
    result.client_end_to_end_time_ms = milliseconds_between(
        result.submit_started_at,
        result.job_completed_at,
    )
    result.error_message = last_error or f"job timed out after {job_timeout_seconds} seconds"


async def poll_until_terminal(
    *,
    client: httpx.AsyncClient,
    results: list[RequestResult],
    poll_interval: float,
    job_timeout_seconds: float,
    vm_sampler: VmCountSampler | None,
) -> None:
    pollable_results = [result for result in results if result.job_id is not None]
    tasks = [
        asyncio.create_task(
            poll_one_job(
                client=client,
                result=result,
                poll_interval=poll_interval,
                job_timeout_seconds=job_timeout_seconds,
                vm_sampler=vm_sampler,
            )
        )
        for result in pollable_results
    ]

    if not tasks:
        return

    pending: set[asyncio.Task[None]] = set(tasks)
    while pending:
        done, pending = await asyncio.wait(pending, timeout=10.0)
        for task in done:
            task.result()

        completed = count_by_status(results, "completed")
        failed = count_by_status(results, "failed")
        timed_out = count_by_status(results, "timed_out")
        remaining = len(pollable_results) - completed - failed - timed_out
        print(
            "Polling progress: "
            f"completed={completed}, failed={failed}, timed_out={timed_out}, remaining={remaining}"
        )


def count_by_status(results: list[RequestResult], status: str) -> int:
    return sum(1 for result in results if result.job_status == status)


async def fetch_json(
    client: httpx.AsyncClient,
    path: str,
) -> tuple[int | None, dict[str, Any] | None, str | None]:
    try:
        response = await client.get(path)
    except httpx.HTTPError as exc:
        return None, None, str(exc)

    try:
        body = response.json()
    except json.JSONDecodeError as exc:
        return response.status_code, None, f"malformed JSON: {exc}"

    return response.status_code, body, None


def extract_queue_counts(stats_body: dict[str, Any] | None) -> tuple[int, int] | None:
    if stats_body is None:
        return None

    queued = stats_body.get("queued_count", stats_body.get("queued"))
    running = stats_body.get("running_count", stats_body.get("running"))
    queued_count = safe_int(queued)
    running_count = safe_int(running)

    if queued_count is None or running_count is None:
        return None
    return queued_count, running_count


async def wait_for_stats_idle(
    *,
    client: httpx.AsyncClient,
    poll_interval: float,
) -> None:
    status_code, stats_body, error = await fetch_json(client, "/stats")
    if error or status_code is None or status_code >= 400:
        print(f"/stats unavailable after run; continuing to cooldown. status={status_code}, error={error}")
        return

    counts = extract_queue_counts(stats_body)
    if counts is None:
        print("/stats does not expose queued/running counts; continuing to cooldown.")
        return

    queued_count, running_count = counts
    last_print = 0.0
    while queued_count != 0 or running_count != 0:
        now = time.monotonic()
        if now - last_print >= 10.0:
            print(f"Waiting for /stats idle: queued={queued_count}, running={running_count}")
            last_print = now

        await asyncio.sleep(poll_interval)
        status_code, stats_body, error = await fetch_json(client, "/stats")
        if error or status_code is None or status_code >= 400:
            print(f"/stats became unavailable while waiting for idle: status={status_code}, error={error}")
            return

        counts = extract_queue_counts(stats_body)
        if counts is None:
            print("/stats no longer exposes queued/running counts; continuing to cooldown.")
            return
        queued_count, running_count = counts

    print("/stats reports queued=0 and running=0.")


async def cooldown(seconds: float) -> None:
    if seconds <= 0:
        return

    remaining = seconds
    while remaining > 0:
        sleep_for = min(10.0, remaining)
        print(f"Cooldown: {remaining:.0f}s remaining")
        await asyncio.sleep(sleep_for)
        remaining -= sleep_for


def calculate_summary(
    *,
    experiment_id: str,
    run_label: str,
    target_rps: int,
    worker_count_per_vm: int,
    results: list[RequestResult],
    vm_observations: list[VmObservation],
) -> RunSummary:
    jobs_submitted = len(results)
    jobs_completed = count_by_status(results, "completed")
    jobs_failed = count_by_status(results, "failed")
    jobs_timed_out = count_by_status(results, "timed_out")

    submit_times = sorted(
        value for value in (result.submit_started_at for result in results) if value is not None
    )
    completion_times = sorted(
        value for value in (result.job_completed_at for result in results) if value is not None
    )

    first_submission_time = submit_times[0] if submit_times else None
    last_submission_time = submit_times[-1] if submit_times else None
    first_completion_time = completion_times[0] if completion_times else None
    last_completion_time = completion_times[-1] if completion_times else None

    submission_duration_seconds = None
    raw_submission_duration = seconds_between(first_submission_time, last_submission_time)
    if raw_submission_duration is not None:
        submission_duration_seconds = raw_submission_duration + (1.0 / float(target_rps))

    completion_makespan_seconds = seconds_between(first_submission_time, last_completion_time)
    completed_throughput_rps = None
    if completion_makespan_seconds and completion_makespan_seconds > 0:
        completed_throughput_rps = jobs_completed / completion_makespan_seconds

    enqueue_values = [
        value for value in (safe_float(result.enqueue_time_ms) for result in results) if value is not None
    ]
    client_e2e_values = [
        value
        for value in (safe_float(result.client_end_to_end_time_ms) for result in results)
        if value is not None
    ]
    execution_values = [
        value for value in (safe_float(result.execution_time_ms) for result in results) if value is not None
    ]
    server_e2e_values = [
        value for value in (safe_float(result.end_to_end_time_ms) for result in results) if value is not None
    ]
    vm_counts = [observation.vm_count for observation in vm_observations]

    return RunSummary(
        experiment_id=experiment_id,
        run_label=run_label,
        target_rps=target_rps,
        jobs_submitted=jobs_submitted,
        jobs_completed=jobs_completed,
        jobs_failed=jobs_failed,
        jobs_timed_out=jobs_timed_out,
        success_rate=jobs_completed / jobs_submitted if jobs_submitted else 0.0,
        failed_rate=jobs_failed / jobs_submitted if jobs_submitted else 0.0,
        first_submission_time=first_submission_time,
        last_submission_time=last_submission_time,
        first_completion_time=first_completion_time,
        last_completion_time=last_completion_time,
        submission_duration_seconds=submission_duration_seconds,
        completion_makespan_seconds=completion_makespan_seconds,
        completed_throughput_rps=completed_throughput_rps,
        avg_enqueue_time_ms=average(enqueue_values),
        p50_enqueue_time_ms=percentile(enqueue_values, 50),
        p95_enqueue_time_ms=percentile(enqueue_values, 95),
        avg_client_end_to_end_time_ms=average(client_e2e_values),
        p50_client_end_to_end_time_ms=percentile(client_e2e_values, 50),
        p95_client_end_to_end_time_ms=percentile(client_e2e_values, 95),
        avg_execution_time_ms=average(execution_values),
        p50_execution_time_ms=percentile(execution_values, 50),
        p95_execution_time_ms=percentile(execution_values, 95),
        avg_server_end_to_end_time_ms=average(server_e2e_values),
        p50_server_end_to_end_time_ms=percentile(server_e2e_values, 50),
        p95_server_end_to_end_time_ms=percentile(server_e2e_values, 95),
        min_vm_count_observed=min(vm_counts) if vm_counts else None,
        max_vm_count_observed=max(vm_counts) if vm_counts else None,
        avg_vm_count_observed=average([float(value) for value in vm_counts]),
        worker_count_per_vm=worker_count_per_vm,
    )


def write_raw_requests(output_dir: Path, results: list[RequestResult]) -> None:
    csv_path = output_dir / "raw_requests.csv"
    jsonl_path = output_dir / "raw_requests.jsonl"

    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=RAW_REQUEST_FIELDS)
        writer.writeheader()
        for result in results:
            row = {key: as_csv_row(value) for key, value in asdict(result).items()}
            writer.writerow(row)

    with jsonl_path.open("w", encoding="utf-8") as file:
        for result in results:
            file.write(json.dumps(asdict(result), sort_keys=True) + "\n")


def write_run_summaries(output_dir: Path, summaries: list[RunSummary]) -> None:
    csv_path = output_dir / "run_summaries.csv"
    json_path = output_dir / "run_summaries.json"

    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=RUN_SUMMARY_FIELDS)
        writer.writeheader()
        for summary in summaries:
            row = {key: as_csv_row(value) for key, value in asdict(summary).items()}
            writer.writerow(row)

    with json_path.open("w", encoding="utf-8") as file:
        json.dump([asdict(summary) for summary in summaries], file, indent=2, sort_keys=True)


def write_metadata(output_dir: Path, metadata: dict[str, Any]) -> None:
    with (output_dir / "experiment_metadata.json").open("w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2, sort_keys=True)


def create_output_dir(base_output_dir: Path, timestamp: str) -> Path:
    output_dir = base_output_dir / timestamp
    output_dir.mkdir(parents=True, exist_ok=False)
    return output_dir


def build_metadata(args: argparse.Namespace, experiment_id: str, run_label: str, started_at: str) -> dict[str, Any]:
    return {
        "base_url": args.base_url,
        "experiment_id": experiment_id,
        "run_label": run_label,
        "difficulty": args.difficulty,
        "jobs_per_rate": args.jobs_per_rate,
        "target_rps_levels": args.target_rps_levels,
        "started_at": started_at,
        "finished_at": None,
        "worker_count_per_vm": args.worker_count_per_vm,
        "methodology_notes": (
            "This workload generator benchmarks the live autoscaling Compute Engine "
            "Managed Instance Group behind one GCP load balancer. It intentionally "
            "does not run fixed max-VM comparison trials. Speedup, execution time, "
            "throughput, scalability, elasticity, and cost analysis should be derived "
            "from the observed throughput, latency, queue behavior, VM count samples, "
            "and GCP billing or VM runtime data."
        ),
    }


async def print_startup_probe(client: httpx.AsyncClient) -> None:
    for path in ("/health", "/stats"):
        status_code, body, error = await fetch_json(client, path)
        if error:
            print(f"{path} probe failed: status={status_code}, error={error}")
        else:
            print(f"{path} probe: status={status_code}, body={body}")


async def run_benchmark(args: argparse.Namespace) -> None:
    experiment_id = str(uuid.uuid4())
    started_at = utc_now_iso()
    timestamp = started_at.replace(":", "").replace("-", "").replace(".", "").replace("Z", "Z")
    run_label = args.run_label or f"workload-{timestamp}"
    output_dir = create_output_dir(Path(args.output_dir), timestamp)

    metadata = build_metadata(args, experiment_id, run_label, started_at)
    write_metadata(output_dir, metadata)

    print(f"experiment_id={experiment_id}")
    print(f"run_label={run_label}")
    print(f"output_dir={output_dir}")

    if args.dry_run:
        print_dry_run(args, output_dir)
        return

    db_updater = OptionalDbMetadataUpdater()
    vm_sampler: VmCountSampler | None = None

    await db_updater.start()

    if args.sample_vm_count:
        vm_sampler = VmCountSampler(args.mig_name, args.mig_zone)
        await vm_sampler.start()

    timeout = httpx.Timeout(args.http_timeout_seconds)
    limits = httpx.Limits(
        max_connections=args.max_in_flight,
        max_keepalive_connections=min(args.max_in_flight, 200),
    )

    all_results: list[RequestResult] = []
    summaries: list[RunSummary] = []

    try:
        async with httpx.AsyncClient(
            base_url=args.base_url.rstrip("/"),
            timeout=timeout,
            limits=limits,
        ) as client:
            await print_startup_probe(client)

            for index, target_rps in enumerate(args.target_rps_levels, start=1):
                print(
                    f"Starting target RPS {target_rps} "
                    f"({index}/{len(args.target_rps_levels)}), jobs={args.jobs_per_rate}"
                )
                run_start_monotonic = time.monotonic()

                print(f"Submission start for target_rps={target_rps}")
                results = await submit_jobs_at_rate(
                    client=client,
                    experiment_id=experiment_id,
                    target_rps=target_rps,
                    run_label=run_label,
                    jobs_per_rate=args.jobs_per_rate,
                    difficulty=args.difficulty,
                    worker_count_per_vm=args.worker_count_per_vm,
                    max_in_flight=args.max_in_flight,
                    vm_sampler=vm_sampler,
                    db_updater=db_updater,
                )
                await db_updater.flush()
                print(f"Submission end for target_rps={target_rps}; jobs submitted={len(results)}")

                await poll_until_terminal(
                    client=client,
                    results=results,
                    poll_interval=args.poll_interval,
                    job_timeout_seconds=args.job_timeout_seconds,
                    vm_sampler=vm_sampler,
                )

                run_finish_monotonic = time.monotonic()
                vm_observations = (
                    vm_sampler.observations_between(run_start_monotonic, run_finish_monotonic)
                    if vm_sampler is not None
                    else []
                )
                summary = calculate_summary(
                    experiment_id=experiment_id,
                    run_label=run_label,
                    target_rps=target_rps,
                    worker_count_per_vm=args.worker_count_per_vm,
                    results=results,
                    vm_observations=vm_observations,
                )

                all_results.extend(results)
                summaries.append(summary)
                write_raw_requests(output_dir, all_results)
                write_run_summaries(output_dir, summaries)

                print(
                    f"Run complete target_rps={target_rps}: "
                    f"completed={summary.jobs_completed}, failed={summary.jobs_failed}, "
                    f"timed_out={summary.jobs_timed_out}, "
                    f"throughput={summary.completed_throughput_rps}"
                )

                status_code, stats_body, error = await fetch_json(client, "/stats")
                print(f"/stats after target_rps={target_rps}: status={status_code}, body={stats_body}, error={error}")

                await wait_for_stats_idle(client=client, poll_interval=args.poll_interval)

                if index < len(args.target_rps_levels):
                    print(f"Cooling down before next target RPS after target_rps={target_rps}.")
                else:
                    print(f"Final cooldown after target_rps={target_rps}.")
                await cooldown(args.cooldown_seconds)
    finally:
        await db_updater.stop()
        if vm_sampler is not None:
            await vm_sampler.stop()

        metadata["finished_at"] = utc_now_iso()
        write_metadata(output_dir, metadata)
        print(f"Output written to {output_dir}")


def print_dry_run(args: argparse.Namespace, output_dir: Path) -> None:
    print("DRY RUN: no HTTP requests will be sent.")
    print(f"base_url={args.base_url}")
    print(f"difficulty={args.difficulty}")
    print(f"worker_count_per_vm={args.worker_count_per_vm}")
    print(f"output_dir={output_dir}")

    for target_rps in args.target_rps_levels:
        expected_duration = args.jobs_per_rate / float(target_rps)
        print(
            f"target_rps={target_rps}, jobs={args.jobs_per_rate}, "
            f"expected_submission_duration_seconds={expected_duration:.3f}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Async workload generator for the Cloud Proof-of-Work Job Processing Service."
    )
    parser.add_argument("--base-url", default="http://8.233.134.116")
    parser.add_argument("--difficulty", type=int, default=6)
    parser.add_argument("--jobs-per-rate", type=int, default=120)
    parser.add_argument("--worker-count-per-vm", type=int, default=2)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--job-timeout-seconds", type=float, default=1800)
    parser.add_argument("--cooldown-seconds", type=float, default=60)
    parser.add_argument("--run-label", default=None)
    parser.add_argument("--output-dir", default="results/workload")
    parser.add_argument("--target-rps", default=None)
    parser.add_argument("--max-in-flight", type=int, default=2000)
    parser.add_argument("--http-timeout-seconds", type=float, default=30)
    parser.add_argument("--sample-vm-count", action="store_true")
    parser.add_argument("--mig-name", default=None)
    parser.add_argument("--mig-zone", default=None)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    args.target_rps_levels = parse_target_rps(args.target_rps)

    if args.jobs_per_rate <= 0:
        parser.error("--jobs-per-rate must be positive")
    if args.difficulty < 0 or args.difficulty > 64:
        parser.error("--difficulty must be between 0 and 64")
    if args.max_in_flight <= 0:
        parser.error("--max-in-flight must be positive")
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")
    if args.job_timeout_seconds <= 0:
        parser.error("--job-timeout-seconds must be positive")
    if args.http_timeout_seconds <= 0:
        parser.error("--http-timeout-seconds must be positive")
    if args.sample_vm_count and (not args.mig_name or not args.mig_zone):
        print("Warning: --sample-vm-count needs --mig-name and --mig-zone. VM counts will be null.")

    return args


def main() -> None:
    args = parse_args()
    asyncio.run(run_benchmark(args))


if __name__ == "__main__":
    main()
