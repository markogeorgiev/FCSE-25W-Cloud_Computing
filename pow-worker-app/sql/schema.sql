CREATE TABLE IF NOT EXISTS pow_jobs (
    id UUID PRIMARY KEY,
    input_data TEXT NOT NULL,
    difficulty INTEGER NOT NULL CHECK (difficulty >= 0 AND difficulty <= 64),
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    created_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    worker_id TEXT,
    nonce BIGINT,
    result_hash TEXT,
    execution_time_ms BIGINT,
    end_to_end_time_ms BIGINT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_pow_jobs_status
    ON pow_jobs(status);

CREATE INDEX IF NOT EXISTS idx_pow_jobs_created_at
    ON pow_jobs(created_at);

CREATE INDEX IF NOT EXISTS idx_pow_jobs_status_created_at
    ON pow_jobs(status, created_at);

CREATE TABLE IF NOT EXISTS benchmark_runs (
    id UUID PRIMARY KEY,
    target_rps INTEGER NOT NULL,
    duration_seconds INTEGER NOT NULL,
    worker_count INTEGER NOT NULL,
    total_requests INTEGER NOT NULL,
    accepted_requests INTEGER NOT NULL,
    completed_requests INTEGER NOT NULL,
    failed_requests INTEGER NOT NULL,
    avg_execution_time_ms DOUBLE PRECISION,
    p95_execution_time_ms DOUBLE PRECISION,
    avg_end_to_end_time_ms DOUBLE PRECISION,
    p95_end_to_end_time_ms DOUBLE PRECISION,
    throughput_rps DOUBLE PRECISION,
    speedup DOUBLE PRECISION,
    created_at TIMESTAMP NOT NULL
);

ALTER TABLE pow_jobs
ADD COLUMN IF NOT EXISTS experiment_id UUID,
ADD COLUMN IF NOT EXISTS target_rps INTEGER,
ADD COLUMN IF NOT EXISTS run_label TEXT,
ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS post_response_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_pow_jobs_experiment_id
ON pow_jobs (experiment_id);

CREATE INDEX IF NOT EXISTS idx_pow_jobs_experiment_status
ON pow_jobs (experiment_id, status);

CREATE INDEX IF NOT EXISTS idx_pow_jobs_experiment_created_at
ON pow_jobs (experiment_id, created_at);

CREATE INDEX IF NOT EXISTS idx_pow_jobs_target_rps
ON pow_jobs (target_rps);


SELECT * FROM pow_jobs;
