from __future__ import annotations

import logging
import os
import socket
import threading
import uuid
from concurrent.futures import Future, ProcessPoolExecutor

from app.database import ClaimedJob, Database, utc_now
from app.pow import ProofOfWorkResult, compute_pow

logger = logging.getLogger(__name__)


class WorkerManager:
    def __init__(
        self,
        database: Database,
        *,
        worker_count: int,
        poll_interval_seconds: float,
    ) -> None:
        self.database = database
        self.worker_count = worker_count
        self.poll_interval_seconds = poll_interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._executor: ProcessPoolExecutor | None = None
        self._futures: dict[Future[ProofOfWorkResult], ClaimedJob] = {}

    def start(self) -> None:
        if self.worker_count <= 0:
            logger.info("PoW workers disabled because WORKER_COUNT=%s", self.worker_count)
            return

        self._executor = ProcessPoolExecutor(max_workers=self.worker_count)
        self._thread = threading.Thread(
            target=self._run,
            name="pow-worker-manager",
            daemon=False,
        )
        self._thread.start()
        logger.info("Started PoW worker manager with %s processes", self.worker_count)

    def stop(self) -> None:
        self._stop_event.set()

        if self._thread is not None:
            self._thread.join()

        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None

        logger.info("Stopped PoW worker manager")

    def _run(self) -> None:
        if self._executor is None:
            return

        while not self._stop_event.is_set() or self._futures:
            self._collect_finished_jobs()

            while (
                not self._stop_event.is_set()
                and len(self._futures) < self.worker_count
            ):
                claimed_job = self.database.claim_next_job(self._new_worker_id())
                if claimed_job is None:
                    break

                logger.info(
                    "Claimed PoW job %s with worker_id=%s",
                    claimed_job.id,
                    claimed_job.worker_id,
                )
                future = self._executor.submit(
                    compute_pow,
                    claimed_job.input_data,
                    claimed_job.difficulty,
                )
                self._futures[future] = claimed_job

            wait_seconds = min(self.poll_interval_seconds, 0.25) if self._futures else self.poll_interval_seconds
            self._stop_event.wait(wait_seconds)

        self._collect_finished_jobs()

    def _collect_finished_jobs(self) -> None:
        for future, job in list(self._futures.items()):
            if not future.done():
                continue

            self._futures.pop(future)
            finished_at = utc_now()

            try:
                result = future.result()
                end_to_end_time_ms = int((finished_at - job.created_at).total_seconds() * 1000)
                self.database.complete_job(
                    job,
                    result,
                    finished_at=finished_at,
                    end_to_end_time_ms=end_to_end_time_ms,
                )
                logger.info(
                    "Completed PoW job %s in %sms",
                    job.id,
                    result.execution_time_ms,
                )
            except Exception as exc:
                logger.exception("PoW job %s failed", job.id)
                self.database.fail_job(job, str(exc), finished_at=finished_at)

    @staticmethod
    def _new_worker_id() -> str:
        return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

