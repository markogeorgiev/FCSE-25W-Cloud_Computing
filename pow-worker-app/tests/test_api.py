from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class FakeDatabase:
    def __init__(self) -> None:
        self.jobs: dict[UUID, dict[str, object]] = {}

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def create_job(self, input_data: str, difficulty: int) -> UUID:
        job_id = uuid4()
        self.jobs[job_id] = {
            "id": job_id,
            "input_data": input_data,
            "difficulty": difficulty,
            "status": "queued",
            "created_at": utc_now(),
            "started_at": None,
            "finished_at": None,
            "worker_id": None,
            "nonce": None,
            "result_hash": None,
            "execution_time_ms": None,
            "end_to_end_time_ms": None,
            "error_message": None,
        }
        return job_id

    def get_job(self, job_id: UUID) -> dict[str, object] | None:
        return self.jobs.get(job_id)

    def health_check(self) -> bool:
        return True

    def get_stats(self) -> dict[str, object]:
        return {
            "queued_count": len(self.jobs),
            "running_count": 0,
            "completed_count": 0,
            "failed_count": 0,
            "avg_execution_time_ms": None,
            "avg_end_to_end_time_ms": None,
        }


def make_settings() -> Settings:
    return Settings(
        database_url="postgresql://example.invalid/pow",
        worker_count=0,
        poll_interval_seconds=0.01,
        default_difficulty=6,
        api_host="127.0.0.1",
        api_port=8000,
    )


def test_create_pow_job_inserts_queued_job() -> None:
    fake_db = FakeDatabase()
    app = create_app(make_settings(), fake_db, start_workers=False)

    with TestClient(app) as client:
        response = client.post(
            "/pow",
            json={"input_data": "some payload", "difficulty": 2},
        )

    assert response.status_code == 202
    body = response.json()
    job_id = UUID(body["job_id"])

    assert body == {"job_id": str(job_id), "status": "queued"}
    assert fake_db.jobs[job_id]["input_data"] == "some payload"
    assert fake_db.jobs[job_id]["difficulty"] == 2
    assert fake_db.jobs[job_id]["status"] == "queued"


def test_get_pow_job_returns_current_state() -> None:
    fake_db = FakeDatabase()
    job_id = fake_db.create_job("payload", 1)
    app = create_app(make_settings(), fake_db, start_workers=False)

    with TestClient(app) as client:
        response = client.get(f"/pow/{job_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(job_id)
    assert body["status"] == "queued"


def test_health_reports_database_connectivity() -> None:
    fake_db = FakeDatabase()
    app = create_app(make_settings(), fake_db, start_workers=False)

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database_connected": True}

