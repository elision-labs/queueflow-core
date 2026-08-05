-- QueueFlow core schema (Rust). Plain PostgreSQL 13+; no extensions required.
--
-- The jobs table is both the queue and the record of truth: a job is "on the
-- queue" when status IN ('pending','retrying') AND scheduled_at <= now().
-- Workers claim rows with FOR UPDATE SKIP LOCKED; leasing is owning the row
-- via a random lease_token until locked_until.

CREATE SCHEMA IF NOT EXISTS queueflow;

-- Jobs: the durable record of every unit of work, and the claim queue.
-- Update-heavy (claim, heartbeat, finish all touch the same row): fillfactor
-- leaves page headroom so updates stay HOT and skip index writes, and the
-- vacuum threshold is far below the default 20%.
CREATE TABLE IF NOT EXISTS queueflow.jobs (
    id               TEXT PRIMARY KEY,
    queue_name       TEXT NOT NULL,
    task_name        TEXT NOT NULL,
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    config           JSONB NOT NULL DEFAULT '{}'::jsonb,
    status           TEXT NOT NULL DEFAULT 'pending',
    priority         INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    locked_until     TIMESTAMPTZ,
    lease_token      UUID,
    delivery_count   INTEGER NOT NULL DEFAULT 0,
    error_message    TEXT,
    retry_count      INTEGER NOT NULL DEFAULT 0,
    next_retry_at    TIMESTAMPTZ,
    workflow_id      TEXT,
    workflow_step_id TEXT,
    result           JSONB,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    tenant_id        TEXT
) WITH (
    fillfactor = 85,
    autovacuum_vacuum_scale_factor = 0.05
);

-- The hot dequeue path: never touches terminal rows, so retained history
-- does not slow claims.
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON queueflow.jobs
    (queue_name, priority DESC, scheduled_at, created_at)
    WHERE status IN ('pending', 'retrying');

-- The janitor's expired-lease sweep.
CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry ON queueflow.jobs (locked_until)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_jobs_status     ON queueflow.jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_queue      ON queueflow.jobs (queue_name);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON queueflow.jobs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_workflow   ON queueflow.jobs (workflow_id) WHERE workflow_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_tenant     ON queueflow.jobs (tenant_id) WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_metadata   ON queueflow.jobs USING GIN (metadata);

-- Wake long-pollers on new work. Fires for out-of-band SQL inserts too, and
-- intentionally also for future-dated jobs (waiters claim nothing and
-- recompute their next-due sleep — cheap, and keeps the predicate trivial).
-- Postgres deduplicates identical (channel, payload) pairs per transaction,
-- so a same-queue batch insert collapses to one wakeup on commit.
CREATE OR REPLACE FUNCTION queueflow.notify_work() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('queueflow_work', NEW.queue_name);
    RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_notify ON queueflow.jobs;
CREATE TRIGGER jobs_notify AFTER INSERT ON queueflow.jobs
    FOR EACH ROW WHEN (NEW.status = 'pending')
    EXECUTE FUNCTION queueflow.notify_work();

-- Workflows: one row per workflow instance.
CREATE TABLE IF NOT EXISTS queueflow.workflows (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'created',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    context      JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
    tenant_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_workflows_status     ON queueflow.workflows (status);
CREATE INDEX IF NOT EXISTS idx_workflows_created_at ON queueflow.workflows (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_workflows_tenant     ON queueflow.workflows (tenant_id) WHERE tenant_id IS NOT NULL;

-- Workflow steps: the DAG nodes. `idx` preserves declaration order; the
-- (workflow_id, name) primary key guarantees a step is enqueued at most once.
CREATE TABLE IF NOT EXISTS queueflow.workflow_steps (
    workflow_id   TEXT NOT NULL REFERENCES queueflow.workflows (id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    idx           INTEGER NOT NULL,
    task_name     TEXT NOT NULL,
    payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    depends_on    JSONB NOT NULL DEFAULT '[]'::jsonb,
    config        JSONB,
    on_success    TEXT NOT NULL DEFAULT 'continue',
    on_failure    TEXT NOT NULL DEFAULT 'halt',
    status        TEXT NOT NULL DEFAULT 'pending',
    job_id        TEXT,
    error_message TEXT,
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (workflow_id, name)
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow ON queueflow.workflow_steps (workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_status   ON queueflow.workflow_steps (status);

-- Dead-letter queue: jobs that exhausted retries, failed permanently, or had
-- no registered handler. Inspect / replay from here.
CREATE TABLE IF NOT EXISTS queueflow.dead_letters (
    id            BIGSERIAL PRIMARY KEY,
    job_id        TEXT NOT NULL,
    queue_name    TEXT,
    task_name     TEXT,
    reason        TEXT NOT NULL,
    error_message TEXT,
    tenant_id     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_job ON queueflow.dead_letters (job_id);
