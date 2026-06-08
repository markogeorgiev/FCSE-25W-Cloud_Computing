from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from app.pow import ProofOfWorkResult
from app.stats import optional_float


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True)
class ClaimedJob:
    id: UUID
    input_data: str
    difficulty: int
    created_at: datetime
    started_at: datetime
    worker_id: str


class Database:
    def __init__(self, database_url: str, *, min_size: int = 1, max_size: int = 10) -> None:
        self.pool = ConnectionPool(
            conninfo=database_url,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row},
            open=False,
        )

    def open(self) -> None:
        self.pool.open(wait=True)

    def close(self) -> None:
        self.pool.close()

    def health_check(self) -> bool:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
        return bool(row and row["ok"] == 1)

    def create_job(self, input_data: str, difficulty: int) -> UUID:
        job_id = uuid4()
        now = utc_now()

        with self.pool.connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO pow_jobs (
                            id,
                            input_data,
                            difficulty,
                            status,
                            created_at
                        )
                        VALUES (%s, %s, %s, 'queued', %s)
                        """,
                        (job_id, input_data, difficulty, now),
                    )

        return job_id

    def get_job(self, job_id: UUID) -> dict[str, Any] | None:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        input_data,
                        difficulty,
                        status,
                        created_at,
                        started_at,
                        finished_at,
                        worker_id,
                        nonce,
                        result_hash,
                        execution_time_ms,
                        end_to_end_time_ms,
                        error_message
                    FROM pow_jobs
                    WHERE id = %s
                    """,
                    (job_id,),
                )
                return cur.fetchone()

    def claim_next_job(self, worker_id: str) -> ClaimedJob | None:
        started_at = utc_now()

        with self.pool.connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, input_data, difficulty, created_at
                        FROM pow_jobs
                        WHERE status = 'queued'
                        ORDER BY created_at
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """
                    )
                    row = cur.fetchone()

                    if row is None:
                        return None

                    cur.execute(
                        """
                        UPDATE pow_jobs
                        SET status = 'running',
                            started_at = %s,
                            worker_id = %s,
                            error_message = NULL
                        WHERE id = %s
                        """,
                        (started_at, worker_id, row["id"]),
                    )

        return ClaimedJob(
            id=row["id"],
            input_data=row["input_data"],
            difficulty=row["difficulty"],
            created_at=row["created_at"],
            started_at=started_at,
            worker_id=worker_id,
        )

    def complete_job(
        self,
        job: ClaimedJob,
        result: ProofOfWorkResult,
        *,
        finished_at: datetime,
        end_to_end_time_ms: int,
    ) -> None:
        with self.pool.connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE pow_jobs
                        SET status = 'completed',
                            finished_at = %s,
                            nonce = %s,
                            result_hash = %s,
                            execution_time_ms = %s,
                            end_to_end_time_ms = %s,
                            error_message = NULL
                        WHERE id = %s
                          AND worker_id = %s
                          AND status = 'running'
                        """,
                        (
                            finished_at,
                            result.nonce,
                            result.result_hash,
                            result.execution_time_ms,
                            end_to_end_time_ms,
                            job.id,
                            job.worker_id,
                        ),
                    )

    def fail_job(self, job: ClaimedJob, error_message: str, *, finished_at: datetime) -> None:
        with self.pool.connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE pow_jobs
                        SET status = 'failed',
                            finished_at = %s,
                            error_message = %s
                        WHERE id = %s
                          AND worker_id = %s
                          AND status = 'running'
                        """,
                        (finished_at, error_message, job.id, job.worker_id),
                    )

    def get_stats(self) -> dict[str, Any]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE status = 'queued')::INTEGER AS queued_count,
                        COUNT(*) FILTER (WHERE status = 'running')::INTEGER AS running_count,
                        COUNT(*) FILTER (WHERE status = 'completed')::INTEGER AS completed_count,
                        COUNT(*) FILTER (WHERE status = 'failed')::INTEGER AS failed_count,
                        AVG(execution_time_ms) FILTER (
                            WHERE status = 'completed'
                              AND execution_time_ms IS NOT NULL
                        ) AS avg_execution_time_ms,
                        AVG(end_to_end_time_ms) FILTER (
                            WHERE status = 'completed'
                              AND end_to_end_time_ms IS NOT NULL
                        ) AS avg_end_to_end_time_ms
                    FROM pow_jobs
                    """
                )
                row = cur.fetchone() or {}

        return {
            "queued_count": int(row.get("queued_count") or 0),
            "running_count": int(row.get("running_count") or 0),
            "completed_count": int(row.get("completed_count") or 0),
            "failed_count": int(row.get("failed_count") or 0),
            "avg_execution_time_ms": optional_float(row.get("avg_execution_time_ms")),
            "avg_end_to_end_time_ms": optional_float(row.get("avg_end_to_end_time_ms")),
        }
