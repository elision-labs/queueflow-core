-- QueueFlow core schema (Rust). PostgreSQL + PGMQ.
--
-- The engine talks to this schema exclusively through the Postgres adapter;
-- PGMQ provides the durable, at-least-once message queue (visibility timeouts
-- and native delayed delivery power the durable retries).

CREATE SCHEMA IF NOT EXISTS queueflow;
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;

-- Jobs: the durable record of every unit of work.
CREATE TABLE IF NOT EXISTS queueflow.jobs (
    id               TEXT PRIMARY KEY,
    queue_name       TEXT NOT NULL,
    task_name        TEXT NOT NULL,
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    config           JSONB NOT NULL DEFAULT '{}'::jsonb,
    status           TEXT NOT NULL DEFAULT 'pending',
    priority         INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    error_message    TEXT,
    retry_count      INTEGER NOT NULL DEFAULT 0,
    next_retry_at    TIMESTAMPTZ,
    workflow_id      TEXT,
    workflow_step_id TEXT,
    result           JSONB,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    tenant_id        TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status     ON queueflow.jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_queue      ON queueflow.jobs (queue_name);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON queueflow.jobs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_workflow   ON queueflow.jobs (workflow_id) WHERE workflow_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_tenant     ON queueflow.jobs (tenant_id) WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_metadata   ON queueflow.jobs USING GIN (metadata);

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
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_job ON queueflow.dead_letters (job_id);
