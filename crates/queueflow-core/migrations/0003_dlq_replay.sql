-- Dead-letter replay bookkeeping. A dead letter can be replayed at most once:
-- the claim is `UPDATE ... SET replayed_at = now() WHERE replayed_at IS NULL`,
-- committed in the same transaction as the replacement job's insert.

ALTER TABLE queueflow.dead_letters ADD COLUMN IF NOT EXISTS replayed_at TIMESTAMPTZ;
ALTER TABLE queueflow.dead_letters ADD COLUMN IF NOT EXISTS replay_job_id TEXT;

-- The admin surface lists newest-first and scopes by tenant.
CREATE INDEX IF NOT EXISTS idx_dead_letters_created_at ON queueflow.dead_letters (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dead_letters_tenant ON queueflow.dead_letters (tenant_id)
    WHERE tenant_id IS NOT NULL;
