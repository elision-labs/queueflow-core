//! Ports (hexagonal architecture): the abstract interfaces the engine depends
//! on. Concrete adapters live in [`crate::adapters`].
//!
//! This is the heart of the engine's testability: the
//! engine, workflow scheduler, and HTTP API are written against these traits,
//! so the *entire* system runs against fast, deterministic in-memory adapters
//! in unit tests — no PostgreSQL required — while production wires the
//! Postgres adapter.
//!
//! There is deliberately no separate message-queue port: the job store *is*
//! the queue. A job is "queued" when `status IN (pending, retrying)` and
//! `scheduled_at` has passed; workers take work by claiming rows
//! ([`JobStore::claim_jobs`]) and own it through a lease token until they
//! finish ([`JobStore::finish_if_leased`]) or the lease expires.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::*;

/// Abstract clock so time-dependent behaviour (retry backoff, timestamps) is
/// deterministic in tests via [`crate::adapters::clock::TestClock`].
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Headroom added on top of a job's configured timeout when a claim is asked
/// to cover it (`cover_timeout` on [`JobStore::claim_jobs`]), leaving room for
/// post-handler bookkeeping (terminal write, workflow advance).
pub const LEASE_GRACE_SECS: u64 = 30;

/// Errors a [`JobStore`] can produce.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("job not found: {0}")]
    JobNotFound(String),
    #[error("workflow not found: {0}")]
    WorkflowNotFound(String),
    #[error("workflow step not found: {workflow}/{step}")]
    StepNotFound { workflow: String, step: String },
    #[error("dead letter not found: {0}")]
    DeadLetterNotFound(i64),
    #[error("cron schedule not found: {0}")]
    CronNotFound(String),
    #[error("database error: {0}")]
    Database(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Keyset-pagination cursor: the sort key of the last row of the previous
/// page. Listing continues strictly after it in the filter's sort order, so
/// deep pages cost the same as page one (OFFSET scans and discards
/// everything it skips). `id` is the row id as text (dead-letter ids are the
/// integer rendered in decimal).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageCursor {
    pub created_at: DateTime<Utc>,
    pub id: String,
}

/// Filter + pagination for listing jobs or workflows.
#[derive(Clone, Debug)]
pub struct ListFilter {
    pub tenant_id: Option<String>,
    pub status: Option<String>,
    pub queue: Option<String>,
    pub limit: i64,
    pub offset: i64,
    /// Order by `created_at` descending when true (the default), ascending when
    /// false.
    pub order_desc: bool,
    /// Compute the exact total match count. Off by default: an exact count is a
    /// full `COUNT(*)` over the filtered set on Postgres, which large tables
    /// pay for on every page. `has_more` is always computed cheaply.
    pub include_total: bool,
    /// Resume listing strictly after this row (keyset pagination). When set,
    /// `offset` is ignored. `total`, when requested, still counts the whole
    /// filtered set, not the remainder.
    pub after: Option<PageCursor>,
}

impl Default for ListFilter {
    fn default() -> Self {
        Self {
            tenant_id: None,
            status: None,
            queue: None,
            limit: 50,
            offset: 0,
            order_desc: true,
            include_total: false,
            after: None,
        }
    }
}

/// One page of list results. `has_more` is derived by fetching one row past
/// `limit`, so it is exact and costs no extra count query. `total` is only
/// present when [`ListFilter::include_total`] was set.
#[derive(Clone, Debug, Default)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub has_more: bool,
    pub total: Option<i64>,
}

/// Result of a [`JobStore::claim_jobs`] attempt.
#[derive(Clone, Debug, Default)]
pub struct Claimed {
    /// The jobs this caller now owns (marked running, leased).
    pub jobs: Vec<LeasedJob>,
    /// When nothing was claimable: the earliest future `scheduled_at` on the
    /// queue, if any. Lets an idle waiter wake exactly when delayed work
    /// (a backoff retry or a `run_at` job) becomes due, instead of on a poll
    /// tick. `None` when jobs were claimed or the queue is empty.
    pub next_due: Option<DateTime<Utc>>,
    /// The queue's work epoch as observed *before* the claim ran. Passing it
    /// to [`JobStore::await_work`] closes the missed-wakeup window: a
    /// notification that lands between the empty claim and the park advances
    /// the epoch, and `await_work` then returns immediately instead of
    /// sleeping out the poll cap.
    pub epoch: u64,
}

/// Confirmation of a lease-guarded terminal write, carrying the finished
/// job's workflow linkage so callers advancing a workflow do not need to
/// re-read the row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FinishedJob {
    pub workflow_id: Option<String>,
    pub workflow_step_id: Option<String>,
}

/// Durable storage for jobs and workflows — and, via the claim/lease methods,
/// the work queue itself. PostgreSQL is the production source of truth; the
/// in-memory adapter mirrors its observable behaviour.
#[async_trait]
pub trait JobStore: Send + Sync {
    /// Persist a new job. The insert *is* the publish: a `pending` job whose
    /// `scheduled_at` has passed is immediately claimable. Returns `false`
    /// when the job's idempotency key already exists for its tenant (the
    /// existing row is left untouched and nothing is inserted); `true` when
    /// the row was created.
    async fn create_job(&self, job: &Job) -> Result<bool, StorageError>;
    async fn batch_create_jobs(&self, jobs: &[Job]) -> Result<Vec<String>, StorageError>;
    async fn get_job(&self, id: &str) -> Result<Job, StorageError>;
    async fn list_jobs(&self, filter: &ListFilter) -> Result<Page<Job>, StorageError>;

    /// Look up the job previously created with this idempotency key, if any.
    async fn find_job_by_idempotency_key(
        &self,
        tenant_id: Option<&str>,
        key: &str,
    ) -> Result<Option<Job>, StorageError>;

    // ---- Claim / lease (the queue role) ------------------------------------

    /// Atomically claim up to `count` due jobs from `queue`: each claimed row
    /// is marked `running`, stamped with `locked_until = now + lease_secs`,
    /// given a fresh random lease token, and has its `delivery_count`
    /// incremented. Concurrent claimers never receive the same job
    /// (`FOR UPDATE SKIP LOCKED` on Postgres). Higher `priority` wins, then
    /// earlier `scheduled_at`, then earlier `created_at`.
    ///
    /// With `cover_timeout`, each claim's lease is stretched to at least the
    /// job's configured `timeout_secs` plus [`LEASE_GRACE_SECS`], so a local
    /// worker that never heartbeats cannot be reaped mid-run and needs no
    /// separate lease-extension round trip. Heartbeating callers (the remote
    /// worker protocol) pass `false` to keep short leases and fast crash
    /// recovery.
    async fn claim_jobs(
        &self,
        queue: &str,
        count: usize,
        lease_secs: u32,
        cover_timeout: bool,
    ) -> Result<Claimed, StorageError>;

    /// Extend a held lease to `lease_secs` from now. Returns the job's current
    /// status: `Some(Running)` means the token still owned the job and the
    /// lease was extended; any other status means the lease was *not* extended
    /// (the job finished, or was cancelled mid-run — which is how a
    /// heartbeating worker learns to stop). `None` means the token no longer
    /// owns a live lease on a running job (expired and reclaimed).
    async fn extend_lease(
        &self,
        job_id: &str,
        token: &str,
        lease_secs: u32,
    ) -> Result<Option<JobStatus>, StorageError>;

    /// Terminal-state transition guarded by lease ownership: succeeds only if
    /// the job is still `running` under `token`. Stamps `completed_at`, clears
    /// the lease, records `error` / `result`. Returns `None` when the guard
    /// failed (lease lost, job already terminal, or cancelled mid-run) — the
    /// caller decides whether that is an idempotent replay or a conflict.
    /// On success, returns the job's workflow linkage so a workflow advance
    /// needs no re-read.
    async fn finish_if_leased(
        &self,
        job_id: &str,
        token: &str,
        status: JobStatus,
        error: Option<&str>,
        result: Option<&Json>,
    ) -> Result<Option<FinishedJob>, StorageError>;

    /// Park until work may exist on `queue` or `max_wait` passes. May wake
    /// spuriously; callers re-claim and loop. `since_epoch` is the epoch a
    /// preceding [`JobStore::claim_jobs`] observed: if the queue was notified
    /// after that snapshot, this returns immediately instead of parking, so
    /// no wakeup is ever lost between claim and park. Postgres: one shared
    /// LISTEN connection fans out to all in-process waiters (degrading to a
    /// bounded sleep if LISTEN is unavailable). Memory: `tokio::sync::Notify`.
    /// Either way an idle worker holds zero database connections.
    async fn await_work(
        &self,
        queue: &str,
        since_epoch: u64,
        max_wait: Duration,
    ) -> Result<(), StorageError>;

    /// Park until `job_id` may have changed status, or `max_wait` passes. May
    /// wake spuriously; callers re-read the job and loop. Postgres: driven by
    /// the `jobs_status_notify` trigger over the shared LISTEN connection, so
    /// idle watchers cost no queries. The default implementation is a plain
    /// bounded sleep (pure polling), which keeps custom stores working.
    async fn await_job_change(&self, job_id: &str, max_wait: Duration) -> Result<(), StorageError> {
        let _ = job_id;
        tokio::time::sleep(max_wait).await;
        Ok(())
    }

    /// Atomically cancel a job that is not yet terminal. Returns `true` if the
    /// job was cancelled, `false` if it was already terminal (or missing) — the
    /// check and the write are a single operation so a completing worker can
    /// never be raced into overwriting a terminal status.
    async fn cancel_job_if_active(&self, id: &str, reason: &str) -> Result<bool, StorageError>;

    /// Record that a job is scheduled for retry: status becomes `retrying`,
    /// `scheduled_at` moves to `next_retry_at` (the durable delay lives in the
    /// row itself), and the lease is released in the same write. Guarded by
    /// lease ownership like [`JobStore::finish_if_leased`] — returns `false`
    /// without writing when the token lost the lease or the job was cancelled
    /// mid-run (so a retry can never resurrect a cancelled job).
    async fn mark_retrying(
        &self,
        id: &str,
        token: &str,
        retry_count: u32,
        next_retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<bool, StorageError>;

    /// Move a permanently-failed (or undeliverable) job to the dead-letter
    /// table for later inspection / replay.
    async fn move_to_dlq(&self, id: &str, reason: &str, error: &str) -> Result<(), StorageError>;

    async fn count_dead_letters(&self) -> Result<i64, StorageError>;

    /// List dead letters, honouring the filter's tenant/queue scoping and
    /// paging (`status` does not apply and is ignored).
    async fn list_dead_letters(
        &self,
        filter: &ListFilter,
    ) -> Result<Page<DeadLetter>, StorageError>;

    async fn get_dead_letter(&self, id: i64) -> Result<DeadLetter, StorageError>;

    /// Atomically claim an unreplayed dead letter and insert `replacement` as
    /// a fresh pending job: the claim (`replayed_at IS NULL`) and the insert
    /// commit together, so a dead letter can never spawn two replays. Returns
    /// `false` — persisting nothing — when the entry was already replayed.
    async fn replay_dead_letter(&self, id: i64, replacement: &Job) -> Result<bool, StorageError>;

    // ---- Cron schedules -----------------------------------------------------

    /// Persist a new schedule. Returns `false` when the (tenant, name) pair
    /// already exists (nothing is written).
    async fn create_cron(&self, cron: &CronSchedule) -> Result<bool, StorageError>;
    async fn get_cron(&self, id: &str) -> Result<CronSchedule, StorageError>;
    /// List schedules (tenant scoping and paging honoured; `status`/`queue`
    /// are ignored).
    async fn list_crons(&self, filter: &ListFilter) -> Result<Page<CronSchedule>, StorageError>;
    /// Returns `false` when the schedule did not exist.
    async fn delete_cron(&self, id: &str) -> Result<bool, StorageError>;
    /// Enable or disable a schedule. `next_run_at` is the caller's freshly
    /// computed next occurrence, so a re-enabled schedule fires at its next
    /// future slot instead of catching up on everything it missed. Returns
    /// `false` when the schedule did not exist.
    async fn set_cron_enabled(
        &self,
        id: &str,
        enabled: bool,
        next_run_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;
    /// Enabled schedules whose `next_run_at` has passed. May return the same
    /// schedule to concurrent callers; the per-firing idempotency key makes
    /// the duplicate enqueue a no-op.
    async fn due_crons(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<CronSchedule>, StorageError>;
    /// Record a firing: stamp `last_enqueued_at = fired_at` and move
    /// `next_run_at` forward.
    async fn advance_cron(
        &self,
        id: &str,
        fired_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    async fn ping(&self) -> Result<(), StorageError>;

    // ---- Janitor sweeps -----------------------------------------------------

    /// Claim `running` jobs whose lease expired (`locked_until < now`),
    /// re-leasing them to the caller for `lease_secs` under a fresh token so
    /// the janitor can route them through the normal failure policy. Safe to
    /// run concurrently on multiple servers (`SKIP LOCKED` on Postgres).
    async fn claim_expired_leases(
        &self,
        limit: usize,
        lease_secs: u32,
    ) -> Result<Vec<LeasedJob>, StorageError>;

    /// Jobs that reached a terminal state but whose linked workflow step is
    /// not terminal — a crash hit between the job's terminal write and the
    /// step-status write. The janitor re-drives workflow advancement for them.
    async fn stalled_step_jobs(&self, limit: usize) -> Result<Vec<Job>, StorageError>;

    /// Non-terminal workflows with zero non-terminal linked jobs — a crash hit
    /// after the step-status write but before dependents were enqueued or the
    /// final status aggregated. The janitor re-runs `advance` on them
    /// (idempotent: scheduling is protected by the `link_step_job` claim).
    async fn stalled_workflow_ids(&self, limit: usize) -> Result<Vec<String>, StorageError>;

    /// Delete terminal jobs, terminal workflows (with their steps), and dead
    /// letters older than `older_than`. Returns the number of jobs deleted.
    /// Postgres guards the sweep with an advisory lock so concurrent janitors
    /// do not race the same bulk delete (the losers return 0).
    async fn purge_terminal(&self, older_than: DateTime<Utc>) -> Result<u64, StorageError>;

    // ---- Workflow persistence (same store; one source of truth) ----

    async fn create_workflow(&self, wf: &Workflow) -> Result<(), StorageError>;
    async fn get_workflow(&self, id: &str) -> Result<Workflow, StorageError>;
    async fn list_workflows(&self, filter: &ListFilter) -> Result<Page<Workflow>, StorageError>;

    /// The workflow's status alone — a one-column read for the hot
    /// advancement path, which needs terminal checks far more often than the
    /// full step definitions. The default delegates to
    /// [`JobStore::get_workflow`].
    async fn get_workflow_status(&self, id: &str) -> Result<WorkflowStatus, StorageError> {
        Ok(self.get_workflow(id).await?.status)
    }

    /// The workflow's accumulated context alone — read by the scheduler once
    /// per advancement round instead of once per scheduled step. The default
    /// delegates to [`JobStore::get_workflow`].
    async fn get_workflow_context(&self, id: &str) -> Result<Map, StorageError> {
        Ok(self.get_workflow(id).await?.context)
    }

    /// `(step_name, status, linked_job_id)` for every step of a workflow.
    async fn workflow_step_statuses(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StepRecord>, StorageError>;

    /// Claim a step by attaching a created job to it. Returns `true` if this
    /// call won the claim, `false` if the step already had a linked job (a
    /// concurrent `advance` got there first). The claim is atomic; it is the
    /// guard that makes step scheduling race-free across workers.
    async fn link_step_job(
        &self,
        workflow_id: &str,
        step_name: &str,
        job_id: &str,
    ) -> Result<bool, StorageError>;

    /// Atomically claim a workflow step *and* create its job (`job` carries
    /// `workflow_id` / `workflow_step_id`): the step->job link and the insert
    /// commit together. Returns `false` — persisting nothing — when the step
    /// already had a linked job. Without this atomicity, a losing duplicate
    /// from two concurrent `advance` calls would be briefly claimable
    /// (the insert is the publish) and a fan-in step could execute twice.
    async fn create_step_job(&self, job: &Job) -> Result<bool, StorageError>;

    async fn set_step_status(
        &self,
        workflow_id: &str,
        step_name: &str,
        status: StepStatus,
        error: Option<&str>,
    ) -> Result<(), StorageError>;

    /// Transition a workflow's status. A workflow already in a terminal state
    /// is never overwritten; returns `true` only when the status actually
    /// changed (used to keep stats exact under concurrent aggregation).
    async fn set_workflow_status(
        &self,
        workflow_id: &str,
        status: WorkflowStatus,
    ) -> Result<bool, StorageError>;

    /// Merge a single key/value into a workflow's accumulated context.
    async fn merge_workflow_context(
        &self,
        workflow_id: &str,
        key: &str,
        value: &Json,
    ) -> Result<(), StorageError>;
}

/// The persisted state of one workflow step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepRecord {
    pub name: String,
    pub status: StepStatus,
    pub job_id: Option<String>,
}
