from __future__ import annotations

"""
Duration-based workload generator for the Cloud Proof-of-Work service.

Unlike workload_generator.py, this script keeps each target RPS level active for a
fixed submission duration. For example, --duration-per-rate-seconds 5 submits:
1 RPS -> 5 jobs, 10 RPS -> 50 jobs, 1000 RPS -> 5000 jobs.
"""

import argparse
import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

import workload_generator as wg


def jobs_for_duration(target_rps: int, duration_seconds: float) -> int:
    jobs = target_rps * duration_seconds
    rounded_jobs = round(jobs)
    if abs(jobs - rounded_jobs) > 0.000001:
        return max(1, int(jobs + 0.999999))
    return max(1, int(rounded_jobs))


def build_duration_metadata(
    args: argparse.Namespace,
    experiment_id: str,
    run_label: str,
    started_at: str,
    jobs_by_rps: dict[int, int],
) -> dict[str, Any]:
    return {
        "base_url": args.base_url,
        "experiment_id": experiment_id,
        "run_label": run_label,
        "difficulty": args.difficulty,
        "duration_per_rate_seconds": args.duration_per_rate_seconds,
        "jobs_per_target_rps": {str(key): value for key, value in jobs_by_rps.items()},
        "planned_submission_duration_seconds": {
            str(key): value / float(key) for key, value in jobs_by_rps.items()
        },
        "target_rps_levels": args.target_rps_levels,
        "started_at": started_at,
        "finished_at": None,
        "worker_count_per_vm": args.worker_count_per_vm,
        "methodology_notes": (
            "This duration-based workload generator benchmarks the live autoscaling "
            "Compute Engine Managed Instance Group behind one GCP load balancer. "
            "Each target RPS is submitted for the configured duration by calculating "
            "jobs = target_rps * duration. It intentionally does not run fixed "
            "1 VM vs 2 VM vs 3 VM trials."
        ),
    }


def print_dry_run(
    args: argparse.Namespace,
    output_dir: Path,
    jobs_by_rps: dict[int, int],
) -> None:
    print("DRY RUN: no HTTP requests will be sent.")
    print(f"base_url={args.base_url}")
    print(f"difficulty={args.difficulty}")
    print(f"duration_per_rate_seconds={args.duration_per_rate_seconds}")
    print(f"worker_count_per_vm={args.worker_count_per_vm}")
    print(f"output_dir={output_dir}")

    for target_rps in args.target_rps_levels:
        jobs = jobs_by_rps[target_rps]
        planned_duration = jobs / float(target_rps)
        print(
            f"target_rps={target_rps}, jobs={jobs}, "
            f"planned_submission_duration_seconds={planned_duration:.3f}"
        )


async def run_duration_benchmark(args: argparse.Namespace) -> None:
    experiment_id = str(uuid.uuid4())
    started_at = wg.utc_now_iso()
    timestamp = started_at.replace(":", "").replace("-", "").replace(".", "").replace("Z", "Z")
    run_label = args.run_label or f"duration-workload-{timestamp}"
    output_dir = wg.create_output_dir(Path(args.output_dir), timestamp)
    jobs_by_rps = {
        target_rps: jobs_for_duration(target_rps, args.duration_per_rate_seconds)
        for target_rps in args.target_rps_levels
    }

    metadata = build_duration_metadata(args, experiment_id, run_label, started_at, jobs_by_rps)
    wg.write_metadata(output_dir, metadata)

    print(f"experiment_id={experiment_id}")
    print(f"run_label={run_label}")
    print(f"output_dir={output_dir}")

    if args.dry_run:
        print_dry_run(args, output_dir, jobs_by_rps)
        return

    db_updater = wg.OptionalDbMetadataUpdater()
    vm_sampler: wg.VmCountSampler | None = None

    await db_updater.start()

    if args.sample_vm_count:
        vm_sampler = wg.VmCountSampler(args.mig_name, args.mig_zone)
        await vm_sampler.start()

    timeout = httpx.Timeout(args.http_timeout_seconds)
    limits = httpx.Limits(
        max_connections=args.max_in_flight,
        max_keepalive_connections=min(args.max_in_flight, 200),
    )

    all_results: list[wg.RequestResult] = []
    summaries: list[wg.RunSummary] = []

    try:
        async with httpx.AsyncClient(
            base_url=args.base_url.rstrip("/"),
            timeout=timeout,
            limits=limits,
        ) as client:
            await wg.print_startup_probe(client)

            for index, target_rps in enumerate(args.target_rps_levels, start=1):
                jobs_for_rate = jobs_by_rps[target_rps]
                planned_duration = jobs_for_rate / float(target_rps)
                print(
                    f"Starting target RPS {target_rps} "
                    f"({index}/{len(args.target_rps_levels)}), "
                    f"jobs={jobs_for_rate}, planned_submission_duration={planned_duration:.3f}s"
                )
                run_start_monotonic = time.monotonic()

                print(f"Submission start for target_rps={target_rps}")
                results = await wg.submit_jobs_at_rate(
                    client=client,
                    experiment_id=experiment_id,
                    target_rps=target_rps,
                    run_label=run_label,
                    jobs_per_rate=jobs_for_rate,
                    difficulty=args.difficulty,
                    worker_count_per_vm=args.worker_count_per_vm,
                    max_in_flight=args.max_in_flight,
                    vm_sampler=vm_sampler,
                    db_updater=db_updater,
                )
                await db_updater.flush()
                print(f"Submission end for target_rps={target_rps}; jobs submitted={len(results)}")

                await wg.poll_until_terminal(
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
                summary = wg.calculate_summary(
                    experiment_id=experiment_id,
                    run_label=run_label,
                    target_rps=target_rps,
                    worker_count_per_vm=args.worker_count_per_vm,
                    results=results,
                    vm_observations=vm_observations,
                )

                all_results.extend(results)
                summaries.append(summary)
                wg.write_raw_requests(output_dir, all_results)
                wg.write_run_summaries(output_dir, summaries)

                print(
                    f"Run complete target_rps={target_rps}: "
                    f"completed={summary.jobs_completed}, failed={summary.jobs_failed}, "
                    f"timed_out={summary.jobs_timed_out}, "
                    f"throughput={summary.completed_throughput_rps}"
                )

                status_code, stats_body, error = await wg.fetch_json(client, "/stats")
                print(
                    f"/stats after target_rps={target_rps}: "
                    f"status={status_code}, body={stats_body}, error={error}"
                )

                await wg.wait_for_stats_idle(client=client, poll_interval=args.poll_interval)

                if index < len(args.target_rps_levels):
                    print(f"Cooling down before next target RPS after target_rps={target_rps}.")
                else:
                    print(f"Final cooldown after target_rps={target_rps}.")
                await wg.cooldown(args.cooldown_seconds)
    finally:
        await db_updater.stop()
        if vm_sampler is not None:
            await vm_sampler.stop()

        metadata["finished_at"] = wg.utc_now_iso()
        wg.write_metadata(output_dir, metadata)
        print(f"Output written to {output_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Duration-based async workload generator for the Cloud Proof-of-Work service."
    )
    parser.add_argument("--base-url", default="http://8.233.134.116")
    parser.add_argument("--difficulty", type=int, default=6)
    parser.add_argument("--duration-per-rate-seconds", type=float, default=5.0)
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
    args.target_rps_levels = wg.parse_target_rps(args.target_rps)

    if args.duration_per_rate_seconds <= 0:
        parser.error("--duration-per-rate-seconds must be positive")
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
    asyncio.run(run_duration_benchmark(args))


if __name__ == "__main__":
    main()

