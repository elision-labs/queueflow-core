-- Performance and latency support.
--
-- 1. idx_jobs_next_due: idle workers ask "when is the next delayed job due?"
--    on every empty claim. The claim index leads with priority, so that MIN
--    walked every pending/retrying entry for the queue; this index makes it
--    a first-row probe however deep the backlog of future retries grows.
CREATE INDEX IF NOT EXISTS idx_jobs_next_due ON queueflow.jobs
    (queue_name, scheduled_at)
    WHERE status IN ('pending', 'retrying');

-- 2. Job status events: wake per-job watchers (the SSE stream) on status
--    transitions instead of having each of them poll the row. The payload is
--    the job id; watchers re-read the job, so a lost notification only costs
--    one bounded poll interval. Fires for out-of-band SQL updates too.
CREATE OR REPLACE FUNCTION queueflow.notify_job_event() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('queueflow_job_events', NEW.id);
    RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_status_notify ON queueflow.jobs;
CREATE TRIGGER jobs_status_notify AFTER UPDATE OF status ON queueflow.jobs
    FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION queueflow.notify_job_event();
