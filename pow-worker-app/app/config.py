from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


@dataclass(frozen=True)
class Settings:
    database_url: str
    worker_count: int
    poll_interval_seconds: float
    default_difficulty: int
    api_host: str
    api_port: int


def get_settings() -> Settings:
    load_dotenv()

    return Settings(
        database_url=os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5432/pow_jobs",
        ),
        worker_count=_get_int("WORKER_COUNT", 1),
        poll_interval_seconds=_get_float("POLL_INTERVAL_SECONDS", 1.0),
        default_difficulty=_get_int("DEFAULT_DIFFICULTY", 6),
        api_host=os.getenv("API_HOST", "0.0.0.0"),
        api_port=_get_int("API_PORT", 8000),
    )

