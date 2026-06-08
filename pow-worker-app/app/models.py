from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


JobStatus = Literal["queued", "running", "completed", "failed"]


class PowJobCreate(BaseModel):
    input_data: str = Field(..., min_length=1)
    difficulty: int | None = Field(default=None, ge=0, le=64)


class PowJobCreated(BaseModel):
    job_id: UUID
    status: Literal["queued"]


class PowJobState(BaseModel):
    id: UUID
    input_data: str
    difficulty: int
    status: JobStatus
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    worker_id: str | None = None
    nonce: int | None = None
    result_hash: str | None = None
    execution_time_ms: int | None = None
    end_to_end_time_ms: int | None = None
    error_message: str | None = None


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    database_connected: bool


class StatsResponse(BaseModel):
    queued_count: int
    running_count: int
    completed_count: int
    failed_count: int
    avg_execution_time_ms: float | None = None
    avg_end_to_end_time_ms: float | None = None

