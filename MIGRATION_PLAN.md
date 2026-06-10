# Migration plan: PGMQ to plain-SQL claim queue

## Decision

Drop the PGMQ extension, keep Postgres as the only production backend. The ten
`pgmq.*` calls in `adapters/postgres/message_queue.rs` are replaced by a
`FOR UPDATE SKIP LOCKED` claim query against `queueflow.jobs`, which already
holds the durable record of every job.

Why:

- Two sources of truth today: every job is a row plus a shadow PGMQ message,
  and the engine spends real effort keeping them consistent (ack ordering,
  orphaned-message cleanup, terminal-state guard).
- The one unguarded crash window (a crash between `create_job` and
  `pgmq.send` strands a pending row forever, with no reconciler) disappears
  when enqueue is a single insert.
- PGMQ blocks shipped or promised features: the API accepts `priority` but the
  adapter ignores it (PGMQ is FIFO); lease ownership rides on guessable
  sequential `msg_id`s; `run_at` scheduling needs the claim query.
- The extension is unavailable on plain RDS, Cloud SQL, and Azure Postgres.
  "Point it at any Postgres" is the product's pitch.
- What PGMQ provides is small and replaceable: visibility timeout becomes
  `locked_until` plus a janitor sweep; delayed delivery becomes
  `scheduled_at`; long-poll becomes LISTEN/NOTIFY, which stops burning one DB
  connection per idle waiter (`read_with_poll` holds a connection for the
  whole poll window).

This is not broker-agnosticism. No Redis/SQS adapters: delivery semantics do
not transfer, and the test matrix would explode. The `ports` traits remain the
seam, with the in-memory adapter for tests and Postgres for production.

## Target semantics

One table, `queueflow.jobs`, is both the queue and the record of truth. A job
is "on the queue" when `status IN ('pending','retrying') AND scheduled_at <=
now()`. Leasing is claiming the row; acking is reaching a terminal status. The
`QueueMessage` / `msg_id` layer disappears entirely.

Delivery stays at-least-once with the same guarantees as today, plus two
upgrades:

- A crashed worker consumes retry budget. Today a PGMQ redelivery re-runs the
  handler without incrementing `retry_count`, so a poison job (e.g. one that
  OOMs the worker) crash-loops forever.
- Lease ownership is enforced by a random token instead of trusting a
  client-supplied sequential `msg_id`, closing lease forgery and
  expired-lease completion races.

## Phase 1: schema (additive, `0003_claim_queue.sql`)

```sql
ALTER TABLE queueflow.jobs
    ADD COLUMN scheduled_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN locked_until   TIMESTAMPTZ,
    ADD COLUMN lease_token    UUID,
    ADD COLUMN delivery_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE queueflow.dead_letters ADD COLUMN tenant_id TEXT;

-- The hot dequeue path: never touches terminal rows, so retention of
-- history does not slow claims.
CREATE INDEX idx_jobs_claim ON queueflow.jobs
    (queue_name, priority DESC, scheduled_at, created_at)
    WHERE status IN ('pending', 'retrying');

CREATE INDEX idx_jobs_lease_expiry ON queueflow.jobs (locked_until)
    WHERE status = 'running';

-- Wake long-pollers on new work (covers out-of-band SQL inserts too).
CREATE FUNCTION queueflow.notify_work() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('queueflow_work', NEW.queue_name);
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_notify AFTER INSERT ON queueflow.jobs
    FOR EACH ROW WHEN (NEW.status = 'pending')
    EXECUTE FUNCTION queueflow.notify_work();

-- The jobs table becomes update-heavy (claim, heartbeat, finish all touch the
-- same row). Leave page headroom so updates stay HOT and skip index writes,
-- and vacuum more aggressively than the default 20% threshold.
ALTER TABLE queueflow.jobs SET (
    fillfactor = 85,
    autovacuum_vacuum_scale_factor = 0.05
);
```

Two notes on the trigger:

- It is row-level, so a 1000-job batch insert fires 1000 notifies. Postgres
  deduplicates identical (channel, payload) pairs within a transaction, so
  same-queue batches collapse to one wakeup on commit; no further work needed
  unless batches span many queues, in which case switch to a statement-level
  trigger with a `REFERENCING NEW TABLE` transition table emitting distinct
  queue names.
- It intentionally fires even for future-dated `run_at` jobs; waiters claim
  nothing and recompute their next-due sleep. Cheap, and keeps the trigger
  predicate trivial.

Baseline question: `0001_init.sql` runs `CREATE EXTENSION pgmq`. If nothing is
deployed yet, rewrite `0001` to drop that line and fold these columns in,
rather than shipping a migration chain where fresh installs on RDS fail at
step one. Only keep the chain linear if real deployments exist.

## Phase 2: port changes

Delete the `MessageQueue` trait, `QueueMessage`, `ReadMessage`, and
`adapters/postgres/message_queue.rs`. `JobStore` absorbs four methods:

```rust
/// Claim up to `count` due jobs. Atomically marks them running, stamps
/// locked_until, generates a lease token, increments delivery_count.
async fn claim_jobs(&self, queue: &str, count: usize, lease_secs: u32)
    -> Result<Vec<LeasedJob>, StorageError>;   // LeasedJob = { job, lease_token }

/// Extend a held lease. False if the token no longer owns the job.
async fn extend_lease(&self, job_id: &str, token: &str, secs: u32)
    -> Result<bool, StorageError>;

/// Terminal-state transition guarded by lease ownership. Replaces the
/// update_status + ack pair for the worker paths.
async fn finish_if_leased(&self, job_id: &str, token: &str, status: JobStatus,
    error: Option<&str>, result: Option<&Json>) -> Result<bool, StorageError>;

/// Park until work may exist on `queue` or `max_wait` passes.
/// Postgres: one shared LISTEN connection fans out to in-process waiters.
/// Memory: tokio::sync::Notify. Either way, zero polling connections.
async fn await_work(&self, queue: &str, max_wait: Duration) -> Result<(), StorageError>;
```

The claim query (inside `claim_jobs`):

```sql
WITH c AS (
    SELECT id FROM queueflow.jobs
    WHERE queue_name = $1 AND status IN ('pending','retrying') AND scheduled_at <= now()
    ORDER BY priority DESC, scheduled_at, created_at
    FOR UPDATE SKIP LOCKED LIMIT $2
)
UPDATE queueflow.jobs j
SET status = 'running', started_at = COALESCE(started_at, now()),
    locked_until = now() + make_interval(secs => $3),
    lease_token = gen_random_uuid(), delivery_count = delivery_count + 1
FROM c WHERE j.id = c.id
RETURNING j.*;
```

`mark_retrying` sets `status = 'retrying', scheduled_at = next_retry_at` and
clears the lease. That single row update replaces today's
mark-retrying / send-delayed / ack three-step, and the "failed to enqueue
retry" window in `handle_failure` ceases to exist.

## Phase 3: engine changes (`engine/mod.rs`, `scheduler.rs`)

- `enqueue` / `enqueue_batch` / `enqueue_step`: drop the `queue.send` call;
  the insert is the publish. The stranded-row crash window is gone.
  `EnqueueOptions` gains `run_at: Option<DateTime<Utc>>` mapped to
  `scheduled_at` (this is the scheduled-jobs feature, one line now).
- `worker_loop`: `claim_jobs(queue, 1, vt)`, and when empty,
  `await_work(queue, until_next_due.min(poll_window))`, where `claim_jobs` on
  an empty result also returns the earliest future `scheduled_at` so delayed
  retries wake on time rather than on a poll tick.
- `process_message` becomes `process_job(LeasedJob)`. The completion path
  keeps its ordering (mark completed, then advance the workflow), but the
  self-heal mechanism changes: today a failed advance relies on PGMQ
  redelivery hitting the terminal-state guard. That mechanism leaves with
  PGMQ, so the janitor (phase 4) takes over that role. `resume_workflow`,
  `handle_failure`, the scheduler, retry policy, and the DAG code are
  otherwise untouched.
- Remote worker methods (`lease_jobs`, `heartbeat_lease`, `complete_leased`,
  `fail_leased`) swap `(queue, msg_id: i64)` for `(job_id, lease_token)` and
  use `finish_if_leased`, which closes the lease-forgery and
  expired-lease-race issues in one move.

## Phase 4: the janitor (new, ~150 LOC)

One background loop per server, interval configurable (default ~5s for the
first two sweeps, hourly for retention):

1. Expired leases: claim `running` rows with `locked_until < now()`
   (SKIP LOCKED) and route them through
   `handle_failure(error: "lease expired", retryable: true)`. Crashes now
   follow the normal retry/backoff/DLQ policy.
2. Workflow self-heal: re-drive `resume_workflow` for steps whose linked job
   is terminal but whose step status is not
   (`SELECT s.* FROM workflow_steps s JOIN jobs j ON s.job_id = j.id
   WHERE j.status IN ('completed','failed') AND s.status NOT IN (...)`).
   This replaces redelivery-based healing and is idempotent by the same
   argument as today's terminal-state guard.
3. Retention: delete (or copy to an archive table, config flag) terminal
   jobs, workflows, and dead letters older than the retention window. The
   partial claim index means history never slows the hot path, but listing
   and storage still need this.

Multi-server coordination: sweeps 1 and 2 are SKIP LOCKED / idempotent, so
concurrent janitors are safe but wasteful. Guard each cycle with
`pg_try_advisory_lock` (one key per sweep); whichever server wins runs it,
the rest skip the tick. Retention in particular should never run twice
concurrently against an archive table.

## Phase 5: API surface

- Lease response items gain `lease_token`; `CompleteJobRequest`,
  `FailJobRequest`, and `HeartbeatRequest` replace `queue` + `lease_id` with
  `lease_token`.
- Heartbeat returns `{ "status": "running" | "cancelled" | ... }` instead of
  204, so remote workers learn about cancellation mid-run (free here, since
  the row is already loaded to validate the token).
- `POST /jobs` gains optional `run_at`.
- Breaking changes are fine pre-1.0: bump the OpenAPI spec, regenerate the
  SDKs, update the hand-written Node SDK and example.

## Phase 6: cutover and removal

Because the job row was always the source of truth, cutover never reads PGMQ.
One-time backfill, then drop:

```sql
UPDATE queueflow.jobs SET scheduled_at = COALESCE(next_retry_at, created_at)
    WHERE status IN ('pending', 'retrying');
-- per existing queue: SELECT pgmq.drop_queue(q);
DROP EXTENSION IF EXISTS pgmq;
```

Any in-flight `running` rows at cutover have no `locked_until` and get swept
by the janitor into a normal retry. Zero-downtime is not worth engineering for
at this stage; a brief stop-the-workers window during deploy is fine.

## Testing

The in-memory adapter is rewritten to the same claim/lease semantics with
`TestClock`, so the existing engine and workflow suites carry over with
mechanical changes (`process_once` claims instead of reads). Add targeted
tests for the new invariants:

- A stale lease token cannot complete or heartbeat a reclaimed job.
- Lease expiry consumes retry budget and dead-letters after `max_retries`.
- The janitor heals a completed-job / unfinished-step stall.
- `run_at` jobs are invisible until due.
- Priority ordering within a queue.

The Postgres integration suite re-targets the claim query and the NOTIFY
wakeup.

## Order of work and rough size

1. Schema + store claim/lease methods + memory adapter (~600 LOC touched)
2. Engine/scheduler rewiring, delete PGMQ adapter and `MessageQueue`
   (~400 LOC net removed)
3. Janitor (~150 LOC new)
4. LISTEN/NOTIFY wakeups (~100 LOC new)
5. API/DTO/OpenAPI/SDK regeneration
6. Cutover migration

Steps 1-2 are the critical path and land together (the engine cannot half-use
PGMQ). The auth work (real API keys, producer vs worker scopes) is a separate
parallel workstream.

## Coordination with in-flight work

The working tree currently carries a large uncommitted changeset (worker
protocol, pagination, idempotency, SSE events, SDK tooling) touching the same
files this migration rewrites: `ports.rs`, `engine/mod.rs`, `scheduler.rs`,
`handlers.rs`, `dto.rs`, both adapters.

- Commit (or land) the in-flight changeset before starting phase 1. Do not
  begin the claim-queue work on top of an uncommitted tree; there is no clean
  rebase path if both efforts edit `engine/mod.rs` concurrently.
- Treat `ports.rs` as frozen for other work once phase 2 starts; every trait
  signature change there forces edits in both adapters, the engine, and the
  tests.
- The worker-protocol DTOs (`LeaseJobsRequest`, `CompleteJobRequest`,
  `FailJobRequest`, `HeartbeatRequest`) change again in phase 5. Hold off on
  SDK regeneration until then, or it runs twice.

## Known deferral

The completion path is still two writes (job terminal, then workflow advance)
healed by the janitor rather than one transaction. A fully transactional
complete-and-advance is possible now that everything lives in one database,
but it would mean threading a transaction handle through the scheduler's port
calls, a significantly bigger refactor for a window the janitor already
covers. Ship without it; revisit only if the self-heal sweep fires in
practice.

## Related follow-ups (not part of this migration)

- Real API keys: `api_keys(key_hash, tenant_id, scope)` with hashed keys and
  `producer` vs `worker` scopes. The worker endpoints are deliberately
  cross-tenant; that only holds once worker credentials are distinguishable
  from tenant credentials.
- DLQ admin API (list/replay), tenant-scoped via the new
  `dead_letters.tenant_id` column.
- Batch enqueue parity: support per-job queue, metadata, and idempotency keys
  instead of silently dropping them.
- Per-step queue for workflow steps (scheduler currently hardcodes the
  default queue).
- Cancellation token for in-process `TaskHandler::handle`.
