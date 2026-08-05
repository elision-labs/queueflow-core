-- Idempotent job creation: a client-supplied key that is unique per tenant.
-- COALESCE folds NULL tenants into one namespace so out-of-band jobs are also
-- deduplicated (Postgres unique indexes would otherwise treat NULLs as
-- distinct).

ALTER TABLE queueflow.jobs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
    ON queueflow.jobs ((COALESCE(tenant_id, '')), idempotency_key)
    WHERE idempotency_key IS NOT NULL;
