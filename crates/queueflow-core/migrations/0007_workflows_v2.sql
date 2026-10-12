-- Workflows v2: step timestamps, sub-workflows, fan-out, conditions,
-- per-step queues, workflow timeouts and retries, and a workflow event
-- channel for the SSE stream.

ALTER TABLE queueflow.workflows
    ADD COLUMN IF NOT EXISTS error_message      TEXT,
    ADD COLUMN IF NOT EXISTS timeout_secs       BIGINT,
    ADD COLUMN IF NOT EXISTS deadline_at        TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS retry_count        INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS parent_workflow_id TEXT,
    ADD COLUMN IF NOT EXISTS parent_step_name   TEXT;

ALTER TABLE queueflow.workflow_steps
    ADD COLUMN IF NOT EXISTS started_at        TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS completed_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS queue_name        TEXT,
    ADD COLUMN IF NOT EXISTS condition         JSONB,
    ADD COLUMN IF NOT EXISTS fan_out           JSONB,
    ADD COLUMN IF NOT EXISTS child_workflow    JSONB,
    ADD COLUMN IF NOT EXISTS child_workflow_id TEXT;

-- The janitor's timeout sweep: few rows ever match.
CREATE INDEX IF NOT EXISTS idx_workflows_deadline ON queueflow.workflows (deadline_at)
    WHERE deadline_at IS NOT NULL AND status IN ('created', 'running');

-- Parent lookups (cancel propagation, the stalled-child sweep).
CREATE INDEX IF NOT EXISTS idx_workflows_parent ON queueflow.workflows (parent_workflow_id)
    WHERE parent_workflow_id IS NOT NULL;

-- Fan-out children are addressed by (workflow, step); the existing
-- idx_jobs_workflow leads with workflow_id, which serves this too, so no new
-- job index.

-- Workflow events: wake per-workflow watchers (the SSE stream) when a step
-- or the workflow itself changes state. Payload: the workflow id.
CREATE OR REPLACE FUNCTION queueflow.notify_workflow_event() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('queueflow_workflow_events', NEW.workflow_id);
    RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS workflow_steps_notify ON queueflow.workflow_steps;
CREATE TRIGGER workflow_steps_notify AFTER UPDATE ON queueflow.workflow_steps
    FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status
                       OR OLD.job_id IS DISTINCT FROM NEW.job_id
                       OR OLD.child_workflow_id IS DISTINCT FROM NEW.child_workflow_id)
    EXECUTE FUNCTION queueflow.notify_workflow_event();

CREATE OR REPLACE FUNCTION queueflow.notify_workflow_status() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('queueflow_workflow_events', NEW.id);
    RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS workflows_status_notify ON queueflow.workflows;
CREATE TRIGGER workflows_status_notify AFTER UPDATE OF status ON queueflow.workflows
    FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION queueflow.notify_workflow_status();
