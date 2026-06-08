from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Iterator
from uuid import UUID

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, status

from app.config import Settings, get_settings
from app.database import Database
from app.models import (
    HealthResponse,
    PowJobCreate,
    PowJobCreated,
    PowJobState,
    StatsResponse,
)
from app.worker import WorkerManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    database: Database | None = None,
    *,
    start_workers: bool = True,
) -> FastAPI:
    settings = settings or get_settings()
    db = database or Database(
        settings.database_url,
        max_size=max(settings.worker_count + 4, 5),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> Iterator[None]:
        app.state.database = db
        app.state.settings = settings
        app.state.worker_manager = None

        logger.info("Opening PostgreSQL connection pool")
        db.open()

        worker_manager: WorkerManager | None = None
        if start_workers:
            worker_manager = WorkerManager(
                db,
                worker_count=settings.worker_count,
                poll_interval_seconds=settings.poll_interval_seconds,
            )
            worker_manager.start()
            app.state.worker_manager = worker_manager

        try:
            yield
        finally:
            if worker_manager is not None:
                worker_manager.stop()
            db.close()
            logger.info("Closed PostgreSQL connection pool")

    app = FastAPI(
        title="Cloud Proof-of-Work Job Processing Service",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.database = db
    app.state.settings = settings

    @app.post("/pow", response_model=PowJobCreated, status_code=status.HTTP_202_ACCEPTED)
    def create_pow_job(
        payload: PowJobCreate,
        database: Database = Depends(get_database),
        settings: Settings = Depends(get_current_settings),
    ) -> PowJobCreated:
        difficulty = payload.difficulty
        if difficulty is None:
            difficulty = settings.default_difficulty

        job_id = database.create_job(payload.input_data, difficulty)
        logger.info("Created PoW job %s with difficulty=%s", job_id, difficulty)
        return PowJobCreated(job_id=job_id, status="queued")

    @app.get("/pow/{job_id}", response_model=PowJobState)
    def get_pow_job(
        job_id: UUID,
        database: Database = Depends(get_database),
    ) -> PowJobState:
        job = database.get_job(job_id)
        if job is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="PoW job not found",
            )
        return PowJobState(**job)

    @app.get("/health", response_model=HealthResponse)
    def health(database: Database = Depends(get_database)) -> HealthResponse:
        try:
            database_connected = database.health_check()
        except Exception:
            logger.exception("Database health check failed")
            database_connected = False

        return HealthResponse(
            status="ok" if database_connected else "degraded",
            database_connected=database_connected,
        )

    @app.get("/stats", response_model=StatsResponse)
    def stats(database: Database = Depends(get_database)) -> StatsResponse:
        return StatsResponse(**database.get_stats())

    return app


def get_database(request: Request) -> Database:
    return request.app.state.database


def get_current_settings(request: Request) -> Settings:
    return request.app.state.settings


app = create_app()


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
