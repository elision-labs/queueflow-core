-- Recurring enqueues: cron schedules, evaluated in UTC by the engine. The
-- schedule's next_run_at is the durable "alarm"; every server's janitor loop
-- pumps due schedules, and the per-firing idempotency key
-- (cron:{id}:{due_ts}) collapses concurrent pumps to one job.

CREATE TABLE IF NOT EXISTS queueflow.cron_schedules (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    cron_expr        TEXT NOT NULL,
    task_name        TEXT NOT NULL,
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    config           JSONB,
    queue_name       TEXT,
    tenant_id        TEXT,
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    next_run_at      TIMESTAMPTZ NOT NULL,
    last_enqueued_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The pump's hot path: due, enabled schedules only.
CREATE INDEX IF NOT EXISTS idx_cron_due ON queueflow.cron_schedules (next_run_at) WHERE enabled;

CREATE INDEX IF NOT EXISTS idx_cron_tenant ON queueflow.cron_schedules (tenant_id)
    WHERE tenant_id IS NOT NULL;

-- Names are unique per tenant (NULL tenants share one namespace).
CREATE UNIQUE INDEX IF NOT EXISTS idx_cron_name_tenant
    ON queueflow.cron_schedules ((COALESCE(tenant_id, '')), name);
