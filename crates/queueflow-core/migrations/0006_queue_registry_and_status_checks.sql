-- Scale: the queue registry, closed status constraints, and sharper indexes.
--
-- Index builds here are plain (transactional) CREATE / DROP INDEX, which
-- lock the table against writes for the duration of the build. CONCURRENTLY
-- is deliberately not used: the migration runner holds an advisory lock
-- while it works, every other server waiting on that lock sits inside an
-- open transaction, and CONCURRENTLY waits for all open transactions, so
-- two servers starting together with auto-migrate would deadlock. On a
-- table with millions of rows, run `queueflow migrate` from one place
-- before rolling the fleet, or build the indexes by hand with CONCURRENTLY
-- first (the CREATE INDEX IF NOT EXISTS statements below then no-op).

-- Queue registry. One row per (tenant, queue) the engine has enqueued to,
-- so `/queues` can list a queue that is currently empty, and the home of the
-- per-tenant pause flag. `tenant_id` is '' for jobs without a tenant (primary
-- keys cannot hold NULL). The claim query skips jobs whose (tenant, queue) is
-- paused; everything else about a paused queue is unchanged, so pausing is
-- also how a queue is drained: pause, then wait for `running` to reach zero.
CREATE TABLE IF NOT EXISTS queueflow.queues (
    tenant_id  TEXT NOT NULL DEFAULT '',
    name       TEXT NOT NULL,
    paused     BOOLEAN NOT NULL DEFAULT false,
    paused_at  TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, name)
);

-- Status constraints. The engine's enums are closed; the schema now says so
-- too, which protects the partial indexes (their predicates enumerate the
-- same values) from out-of-band writes. Added NOT VALID (a brief lock, no
-- scan) and validated separately (a scan that does not block writes).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_status_check') THEN
        ALTER TABLE queueflow.jobs ADD CONSTRAINT jobs_status_check
            CHECK (status IN ('pending', 'running', 'completed', 'failed', 'retrying', 'cancelled'))
            NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'workflows_status_check') THEN
        ALTER TABLE queueflow.workflows ADD CONSTRAINT workflows_status_check
            CHECK (status IN ('created', 'running', 'completed', 'failed', 'partially_failed', 'cancelled'))
            NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'workflow_steps_status_check') THEN
        ALTER TABLE queueflow.workflow_steps ADD CONSTRAINT workflow_steps_status_check
            CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'skipped'))
            NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'workflow_steps_on_failure_check') THEN
        ALTER TABLE queueflow.workflow_steps ADD CONSTRAINT workflow_steps_on_failure_check
            CHECK (on_failure IN ('halt', 'skip', 'continue'))
            NOT VALID;
    END IF;
END $$;

ALTER TABLE queueflow.jobs VALIDATE CONSTRAINT jobs_status_check;
ALTER TABLE queueflow.workflows VALIDATE CONSTRAINT workflows_status_check;
ALTER TABLE queueflow.workflow_steps VALIDATE CONSTRAINT workflow_steps_status_check;
ALTER TABLE queueflow.workflow_steps VALIDATE CONSTRAINT workflow_steps_on_failure_check;

-- Tenant listings. The API lists per tenant in `created_at` order with a
-- `(created_at, id)` keyset cursor; these indexes answer that directly at
-- any page depth and replace the bare tenant indexes.
CREATE INDEX IF NOT EXISTS idx_jobs_tenant_created
    ON queueflow.jobs (tenant_id, created_at DESC, id DESC)
    WHERE tenant_id IS NOT NULL;
DROP INDEX IF EXISTS queueflow.idx_jobs_tenant;

CREATE INDEX IF NOT EXISTS idx_workflows_tenant_created
    ON queueflow.workflows (tenant_id, created_at DESC, id DESC)
    WHERE tenant_id IS NOT NULL;
DROP INDEX IF EXISTS queueflow.idx_workflows_tenant;

-- Retention. The purge deletes terminal rows by
-- `COALESCE(completed_at, created_at) < cutoff`; without an index on that
-- expression every sweep scanned all terminal history.
CREATE INDEX IF NOT EXISTS idx_jobs_purge
    ON queueflow.jobs ((COALESCE(completed_at, created_at)))
    WHERE status IN ('completed', 'failed', 'cancelled');
CREATE INDEX IF NOT EXISTS idx_workflows_purge
    ON queueflow.workflows ((COALESCE(completed_at, created_at)))
    WHERE status IN ('completed', 'failed', 'partially_failed', 'cancelled');

-- Redundant indexes. `idx_jobs_status` and `idx_jobs_queue` covered whole
-- columns on a table that is mostly terminal history. Every hot query is
-- served by a partial index already: claims and next-due probes by
-- `idx_jobs_claim` / `idx_jobs_next_due`, the lease sweep by
-- `idx_jobs_lease_expiry`, the per-queue backlog by their union. Listings
-- order by `created_at` and filter the few rows they visit. Dropping them
-- removes two index writes from every insert and every status transition.
DROP INDEX IF EXISTS queueflow.idx_jobs_status;
DROP INDEX IF EXISTS queueflow.idx_jobs_queue;
