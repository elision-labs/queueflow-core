//! The job-processing engine: enqueue, worker loop, durable retries, the
//! janitor, and workflow delegation.

pub mod janitor;
pub mod retry;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use futures::FutureExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::domain::*;
use crate::error::EngineError;
use crate::ports::*;
use crate::stats::EngineStats;
use crate::task::{handler_fn, TaskHandler};
use crate::workflow::scheduler::WorkflowScheduler;

pub use janitor::JanitorConfig;

/// Options for [`Engine::enqueue`].
#[derive(Clone, Debug, Default)]
pub struct EnqueueOptions {
    pub config: Option<JobConfig>,
    pub queue: Option<String>,
    pub tenant_id: Option<String>,
    pub metadata: Map,
    /// Makes the enqueue idempotent per tenant: a second enqueue with the same
    /// key returns the original job's id instead of creating a duplicate.
    pub idempotency_key: Option<String>,
    /// Don't run before this instant. The job is persisted immediately but
    /// invisible to claims until then (durable scheduling, survives restarts).
    pub run_at: Option<DateTime<Utc>>,
}

/// Headroom added on top of a job's timeout when extending its lease, covering
/// post-handler bookkeeping (terminal write, workflow advance).
const LEASE_GRACE_SECS: u64 = 30;

/// Best-effort text from a caught panic payload (for logs and job errors).
pub(crate) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Upper bound on a single idle wait. NOTIFY wakes waiters instantly; this is
/// the missed-notification safety net, so it can be generous without making
/// idle workers chatty.
const WORKER_POLL_SECS: u64 = 5;

/// The core engine, generic over the storage port.
///
/// Construct via [`Engine::builder`]. The engine is wrapped in an `Arc` so
/// worker tasks can be spawned cheaply.
pub struct Engine<JS> {
    store: Arc<JS>,
    clock: Arc<dyn Clock>,
    handlers: HashMap<String, Arc<dyn TaskHandler>>,
    scheduler: WorkflowScheduler<JS>,
    default_queue: String,
    worker_count: usize,
    lease_secs: u32,
    janitor: JanitorConfig,
    stats: Arc<EngineStats>,
    shutdown: CancellationToken,
    running: AtomicBool,
}

/// Fluent constructor for [`Engine`].
pub struct EngineBuilder<JS> {
    store: Arc<JS>,
    clock: Arc<dyn Clock>,
    handlers: HashMap<String, Arc<dyn TaskHandler>>,
    default_queue: String,
    worker_count: usize,
    lease_secs: u32,
    janitor: JanitorConfig,
}

impl<JS> EngineBuilder<JS>
where
    JS: JobStore + 'static,
{
    /// Default queue name (also used for workflow steps).
    pub fn default_queue(mut self, q: impl Into<String>) -> Self {
        self.default_queue = q.into();
        self
    }

    /// Number of concurrent worker tasks per queue.
    pub fn worker_count(mut self, n: usize) -> Self {
        self.worker_count = n.max(1);
        self
    }

    /// Lease duration (seconds) applied when claiming a job. Extended
    /// automatically to cover the job's configured timeout.
    pub fn lease_secs(mut self, secs: u32) -> Self {
        self.lease_secs = secs.max(1);
        self
    }

    /// Tune the janitor (sweep cadence, batch size, retention).
    pub fn janitor(mut self, config: JanitorConfig) -> Self {
        self.janitor = config;
        self
    }

    /// Register a handler implementation for a task name.
    pub fn register(mut self, name: impl Into<String>, handler: Arc<dyn TaskHandler>) -> Self {
        self.handlers.insert(name.into(), handler);
        self
    }

    /// Register an async closure as a handler.
    pub fn register_fn<F, Fut>(self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(Map) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Map, crate::error::HandlerError>> + Send + 'static,
    {
        self.register(name, handler_fn(f))
    }

    pub fn build(self) -> Arc<Engine<JS>> {
        let stats = Arc::new(EngineStats::default());
        let scheduler = WorkflowScheduler::new(
            self.store.clone(),
            self.clock.clone(),
            self.default_queue.clone(),
            stats.clone(),
        );
        Arc::new(Engine {
            store: self.store,
            clock: self.clock,
            handlers: self.handlers,
            scheduler,
            default_queue: self.default_queue,
            worker_count: self.worker_count,
            lease_secs: self.lease_secs,
            janitor: self.janitor,
            stats,
            shutdown: CancellationToken::new(),
            running: AtomicBool::new(false),
        })
    }
}

impl<JS> Engine<JS>
where
    JS: JobStore + 'static,
{
    pub fn builder(store: Arc<JS>, clock: Arc<dyn Clock>) -> EngineBuilder<JS> {
        EngineBuilder {
            store,
            clock,
            handlers: HashMap::new(),
            default_queue: "default".to_string(),
            worker_count: 8,
            lease_secs: 30,
            janitor: JanitorConfig::default(),
        }
    }

    pub fn stats(&self) -> Arc<EngineStats> {
        self.stats.clone()
    }

    /// A child token that fires when the engine shuts down.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Names of all registered task handlers.
    pub fn registered_tasks(&self) -> Vec<String> {
        let mut v: Vec<String> = self.handlers.keys().cloned().collect();
        v.sort();
        v
    }

    // ---- Enqueue ----------------------------------------------------------

    /// Enqueue a single job. The insert is the publish: once the row is
    /// committed the job is claimable (or scheduled, if `run_at` is set), so
    /// there is no crash window between "recorded" and "queued".
    pub async fn enqueue(
        &self,
        task_name: &str,
        payload: Map,
        opts: EnqueueOptions,
    ) -> Result<String, EngineError> {
        if let Some(cfg) = &opts.config {
            cfg.validate().map_err(EngineError::Validation)?;
        }
        let queue = opts.queue.unwrap_or_else(|| self.default_queue.clone());
        let now = self.clock.now();
        let job = Job {
            id: Uuid::new_v4().to_string(),
            queue_name: queue,
            task_name: task_name.to_string(),
            payload,
            config: opts.config.unwrap_or_default(),
            status: JobStatus::Pending,
            created_at: now,
            scheduled_at: opts.run_at.unwrap_or(now),
            started_at: None,
            completed_at: None,
            delivery_count: 0,
            error_message: None,
            retry_count: 0,
            next_retry_at: None,
            workflow_id: None,
            workflow_step_id: None,
            result: None,
            metadata: opts.metadata,
            tenant_id: opts.tenant_id,
            idempotency_key: opts.idempotency_key,
        };

        let inserted = self.store.create_job(&job).await?;
        if !inserted {
            // Idempotent replay: hand back the original job.
            let key = job.idempotency_key.as_deref().unwrap_or_default();
            let existing = self
                .store
                .find_job_by_idempotency_key(job.tenant_id.as_deref(), key)
                .await?
                .ok_or_else(|| {
                    EngineError::Conflict("job already exists for this idempotency key".into())
                })?;
            return Ok(existing.id);
        }
        EngineStats::incr(&self.stats.jobs_created);
        Ok(job.id)
    }

    /// Enqueue many jobs efficiently (one multi-row insert).
    pub async fn enqueue_batch(
        &self,
        requests: Vec<(String, Map, Option<JobConfig>)>,
        tenant_id: Option<String>,
    ) -> Result<Vec<String>, EngineError> {
        for (i, (_, _, config)) in requests.iter().enumerate() {
            if let Some(cfg) = config {
                cfg.validate()
                    .map_err(|e| EngineError::Validation(format!("jobs[{i}]: {e}")))?;
            }
        }
        let now = self.clock.now();
        let jobs: Vec<Job> = requests
            .into_iter()
            .map(|(task_name, payload, config)| Job {
                id: Uuid::new_v4().to_string(),
                queue_name: self.default_queue.clone(),
                task_name,
                payload,
                config: config.unwrap_or_default(),
                status: JobStatus::Pending,
                created_at: now,
                scheduled_at: now,
                started_at: None,
                completed_at: None,
                delivery_count: 0,
                error_message: None,
                retry_count: 0,
                next_retry_at: None,
                workflow_id: None,
                workflow_step_id: None,
                result: None,
                metadata: Map::new(),
                tenant_id: tenant_id.clone(),
                idempotency_key: None,
            })
            .collect();

        let ids = self.store.batch_create_jobs(&jobs).await?;
        for _ in &jobs {
            EngineStats::incr(&self.stats.jobs_created);
        }
        Ok(ids)
    }

    pub async fn get_job(&self, id: &str) -> Result<Job, EngineError> {
        Ok(self.store.get_job(id).await?)
    }

    pub async fn list_jobs(&self, filter: &ListFilter) -> Result<Page<Job>, EngineError> {
        Ok(self.store.list_jobs(filter).await?)
    }

    /// Cancel a job. Pending/retrying work simply becomes unclaimable; running
    /// work keeps its handler alive but every lease-guarded write (heartbeat,
    /// complete, fail, retry) is refused from then on. Cancelling a job that
    /// already reached a terminal state is a 409 Conflict — history is never
    /// rewritten.
    pub async fn cancel_job(&self, id: &str) -> Result<(), EngineError> {
        let cancelled = self
            .store
            .cancel_job_if_active(id, "cancelled by user")
            .await?;
        if !cancelled {
            // Either missing (404 via get_job) or already terminal (409).
            let job = self.store.get_job(id).await?;
            return Err(EngineError::Conflict(format!(
                "job is already {}",
                job.status.as_str()
            )));
        }
        Ok(())
    }

    // ---- Workflows (delegated to the scheduler) ---------------------------

    pub async fn create_workflow(
        &self,
        req: CreateWorkflowRequest,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError> {
        self.scheduler.create(req, tenant_id).await
    }

    pub async fn get_workflow(&self, id: &str) -> Result<Workflow, EngineError> {
        Ok(self.store.get_workflow(id).await?)
    }

    pub async fn list_workflows(&self, filter: &ListFilter) -> Result<Page<Workflow>, EngineError> {
        Ok(self.store.list_workflows(filter).await?)
    }

    pub async fn cancel_workflow(&self, id: &str) -> Result<(), EngineError> {
        let wf = self.store.get_workflow(id).await?; // 404 if missing
        if wf.status.is_terminal() {
            return Err(EngineError::Conflict(format!(
                "workflow is already {}",
                wf.status.as_str()
            )));
        }
        for rec in self.store.workflow_step_statuses(id).await? {
            if !rec.status.is_terminal() {
                self.store
                    .set_step_status(
                        id,
                        &rec.name,
                        StepStatus::Cancelled,
                        Some("cancelled by user"),
                    )
                    .await?;
                // Cancel the step's live job too, or a worker would still
                // execute work for a dead workflow: a pending job stays
                // claimable, and a running one could land its completion.
                // The status flip blocks both (claims filter on status;
                // outcome writes are lease-guarded against non-running rows).
                if let Some(job_id) = &rec.job_id {
                    self.store
                        .cancel_job_if_active(job_id, "workflow cancelled")
                        .await?;
                }
            }
        }
        // The guarded transition is a no-op if a concurrent advance already
        // finished the workflow; that race resolves in the workflow's favour.
        self.store
            .set_workflow_status(id, WorkflowStatus::Cancelled)
            .await?;
        Ok(())
    }

    pub async fn workflow_diagram(&self, id: &str) -> Result<String, EngineError> {
        self.scheduler.diagram(id).await
    }

    // ---- Cron schedules ---------------------------------------------------

    /// Create a recurring enqueue. The expression (standard 5-field crontab,
    /// UTC; 6/7 fields with leading seconds also accepted) and any config
    /// override are validated here; the first firing is the next occurrence
    /// after now. Names are unique per tenant (a duplicate is a Conflict).
    pub async fn create_cron(
        &self,
        req: CreateCronRequest,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError> {
        if req.name.trim().is_empty() {
            return Err(EngineError::Validation("name is required".into()));
        }
        if req.task_name.trim().is_empty() {
            return Err(EngineError::Validation("task_name is required".into()));
        }
        if let Some(cfg) = &req.config {
            cfg.validate().map_err(EngineError::Validation)?;
        }
        let now = self.clock.now();
        let next =
            crate::cron::next_occurrence(&req.cron_expr, now).map_err(EngineError::Validation)?;
        let cron = CronSchedule {
            id: Uuid::new_v4().to_string(),
            name: req.name,
            cron_expr: req.cron_expr,
            task_name: req.task_name,
            payload: req.payload,
            config: req.config,
            queue_name: req.queue,
            tenant_id,
            enabled: true,
            next_run_at: next,
            last_enqueued_at: None,
            created_at: now,
        };
        if !self.store.create_cron(&cron).await? {
            return Err(EngineError::Conflict(format!(
                "cron schedule '{}' already exists",
                cron.name
            )));
        }
        Ok(cron.id)
    }

    pub async fn get_cron(&self, id: &str) -> Result<CronSchedule, EngineError> {
        Ok(self.store.get_cron(id).await?)
    }

    pub async fn list_crons(&self, filter: &ListFilter) -> Result<Page<CronSchedule>, EngineError> {
        Ok(self.store.list_crons(filter).await?)
    }

    pub async fn delete_cron(&self, id: &str) -> Result<(), EngineError> {
        if !self.store.delete_cron(id).await? {
            return Err(StorageError::CronNotFound(id.to_string()).into());
        }
        Ok(())
    }

    /// Pause or resume a schedule. Resuming recomputes `next_run_at` from
    /// now, so the schedule fires at its next future slot rather than
    /// catching up on everything it missed while paused.
    pub async fn set_cron_enabled(&self, id: &str, enabled: bool) -> Result<(), EngineError> {
        let cron = self.store.get_cron(id).await?;
        let next = crate::cron::next_occurrence(&cron.cron_expr, self.clock.now())
            .map_err(EngineError::Validation)?;
        if !self.store.set_cron_enabled(id, enabled, next).await? {
            return Err(StorageError::CronNotFound(id.to_string()).into());
        }
        Ok(())
    }

    /// Enqueue a job for every due cron schedule; returns how many fired.
    /// Runs from the janitor loop on every server: the idempotency key
    /// (`cron:{id}:{due_ts}`) is bound to the stored due instant, so
    /// concurrent pumps enqueue exactly once, and occurrences missed during
    /// downtime collapse into at most one catch-up firing before the
    /// schedule jumps to its next future slot.
    pub async fn cron_tick(&self) -> Result<usize, EngineError> {
        let now = self.clock.now();
        let due = self.store.due_crons(now, self.janitor.batch).await?;
        let mut fired = 0;
        for cron in due {
            let due_at = cron.next_run_at;
            let mut metadata = Map::new();
            metadata.insert("cron_id".into(), Json::String(cron.id.clone()));
            metadata.insert("cron_name".into(), Json::String(cron.name.clone()));
            metadata.insert("cron_due_at".into(), Json::String(due_at.to_rfc3339()));
            let enqueue = self
                .enqueue(
                    &cron.task_name,
                    cron.payload.clone(),
                    EnqueueOptions {
                        config: cron.config.clone(),
                        queue: cron.queue_name.clone(),
                        tenant_id: cron.tenant_id.clone(),
                        metadata,
                        idempotency_key: Some(format!("cron:{}:{}", cron.id, due_at.timestamp())),
                        run_at: None,
                    },
                )
                .await;
            if let Err(e) = enqueue {
                // Leave next_run_at untouched: the next tick retries this
                // same firing (same idempotency key).
                tracing::warn!(cron_id = %cron.id, error = %e, "cron firing failed; will retry");
                continue;
            }
            fired += 1;
            match crate::cron::next_occurrence(&cron.cron_expr, now) {
                Ok(next) => self.store.advance_cron(&cron.id, now, next).await?,
                Err(e) => {
                    // E.g. a year-bounded expression ran out of occurrences.
                    tracing::error!(cron_id = %cron.id, error = %e, "cron has no future occurrence; disabling");
                    let _ = self.store.set_cron_enabled(&cron.id, false, due_at).await;
                }
            }
        }
        Ok(fired)
    }

    // ---- Dead letters -----------------------------------------------------

    pub async fn list_dead_letters(
        &self,
        filter: &ListFilter,
    ) -> Result<Page<DeadLetter>, EngineError> {
        Ok(self.store.list_dead_letters(filter).await?)
    }

    pub async fn get_dead_letter(&self, id: i64) -> Result<DeadLetter, EngineError> {
        Ok(self.store.get_dead_letter(id).await?)
    }

    /// Replay a dead-lettered job as a fresh, detached job: same task,
    /// payload, queue, config, and tenant; a new id; zeroed retry and
    /// delivery counters. Workflow linkage is deliberately not resurrected —
    /// the original step already settled its workflow — but the new job's
    /// metadata records the provenance (`replayed_from_job`,
    /// `replayed_from_dead_letter`). A dead letter replays at most once;
    /// replaying it again is a Conflict. Requires the original job row (the
    /// retention sweep may have purged it).
    pub async fn replay_dead_letter(&self, id: i64) -> Result<String, EngineError> {
        let dl = self.store.get_dead_letter(id).await?;
        if let Some(existing) = &dl.replay_job_id {
            return Err(EngineError::Conflict(format!(
                "dead letter {id} was already replayed as job {existing}"
            )));
        }
        let original = self.store.get_job(&dl.job_id).await?;

        let now = self.clock.now();
        let mut metadata = original.metadata.clone();
        metadata.insert(
            "replayed_from_job".into(),
            Json::String(original.id.clone()),
        );
        metadata.insert("replayed_from_dead_letter".into(), Json::from(id));
        let job = Job {
            id: Uuid::new_v4().to_string(),
            queue_name: original.queue_name.clone(),
            task_name: original.task_name.clone(),
            payload: original.payload.clone(),
            config: original.config.clone(),
            status: JobStatus::Pending,
            created_at: now,
            scheduled_at: now,
            started_at: None,
            completed_at: None,
            delivery_count: 0,
            error_message: None,
            retry_count: 0,
            next_retry_at: None,
            workflow_id: None,
            workflow_step_id: None,
            result: None,
            metadata,
            tenant_id: original.tenant_id.clone(),
            idempotency_key: None,
        };

        if !self.store.replay_dead_letter(id, &job).await? {
            return Err(EngineError::Conflict(format!(
                "dead letter {id} was already replayed"
            )));
        }
        EngineStats::incr(&self.stats.jobs_created);
        Ok(job.id)
    }

    pub async fn ping(&self) -> Result<(), EngineError> {
        Ok(self.store.ping().await?)
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Mark the engine as accepting work without spawning local workers. Used
    /// by API-only deployments (workers run in separate processes) so the
    /// readiness probe does not report a permanently missing worker pool.
    pub fn mark_running(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    // ---- Worker loop ------------------------------------------------------

    /// Spawn `worker_count` concurrent workers draining `queue`. The returned
    /// [`JoinSet`] resolves when all workers stop (on shutdown). Call
    /// [`Engine::shutdown`] (or cancel via the token) to stop them.
    ///
    /// Workers are supervised: a panic that escapes the per-job containment
    /// (an adapter bug, say) restarts the worker instead of silently
    /// shrinking the pool. The pool only winds down at shutdown.
    pub fn run_workers(self: &Arc<Self>, queue: impl Into<String>) -> JoinSet<()> {
        let queue = queue.into();
        self.mark_running();
        let mut set = JoinSet::new();
        for worker_id in 0..self.worker_count {
            let engine = Arc::clone(self);
            let q = queue.clone();
            set.spawn(async move {
                loop {
                    let run = std::panic::AssertUnwindSafe(
                        engine.clone().worker_loop(worker_id, q.clone()),
                    )
                    .catch_unwind()
                    .await;
                    match run {
                        Ok(()) => break, // clean exit (shutdown)
                        Err(panic) => {
                            tracing::error!(
                                worker_id,
                                panic = %panic_message(&*panic),
                                "worker panicked; restarting"
                            );
                            if engine.shutdown.is_cancelled() {
                                break;
                            }
                        }
                    }
                }
            });
        }
        set
    }

    async fn worker_loop(self: Arc<Self>, worker_id: usize, queue: String) {
        tracing::info!(worker_id, queue = %queue, "worker started");
        loop {
            if self.shutdown.is_cancelled() {
                break;
            }
            match self.store.claim_jobs(&queue, 1, self.lease_secs).await {
                Ok(mut claimed) => {
                    if let Some(lease) = claimed.jobs.pop() {
                        self.process_job(lease).await;
                    } else {
                        // Idle: park until NOTIFY wakes us, the next delayed
                        // job comes due, or the safety-net window passes.
                        let wait = self.idle_wait(claimed.next_due);
                        tokio::select! {
                            biased;
                            _ = self.shutdown.cancelled() => break,
                            _ = self.store.await_work(&queue, wait) => {}
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(worker_id, error = %e, "claim error");
                    tokio::select! {
                        _ = self.shutdown.cancelled() => break,
                        _ = tokio::time::sleep(StdDuration::from_secs(1)) => {}
                    }
                }
            }
        }
        tracing::info!(worker_id, queue = %queue, "worker stopped");
    }

    /// How long an idle worker should park: until the next delayed job is due,
    /// capped by the missed-NOTIFY safety window.
    fn idle_wait(&self, next_due: Option<DateTime<Utc>>) -> StdDuration {
        let cap = StdDuration::from_secs(WORKER_POLL_SECS);
        match next_due {
            Some(due) => {
                let until = (due - self.clock.now()).num_milliseconds().max(0) as u64;
                StdDuration::from_millis(until).min(cap)
            }
            None => cap,
        }
    }

    /// Claim and process at most one job. Returns whether a job was handled.
    /// Primarily used to drive the engine deterministically in tests.
    pub async fn process_once(&self, queue: &str) -> Result<bool, EngineError> {
        let mut claimed = self.store.claim_jobs(queue, 1, self.lease_secs).await?;
        match claimed.jobs.pop() {
            Some(lease) => {
                self.process_job(lease).await;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Execute one claimed job end to end: run its handler with a timeout,
    /// then complete / retry / dead-letter and advance any workflow.
    ///
    /// Delivery is **at-least-once**: a worker can crash after the handler
    /// finished but before the terminal write, and the janitor will route the
    /// expired lease through the retry policy — so handlers should be
    /// idempotent. Every outcome write is lease-guarded, so a job that was
    /// cancelled mid-run or reclaimed after lease expiry is never overwritten
    /// by this worker.
    pub async fn process_job(&self, lease: LeasedJob) {
        let LeasedJob { job, lease_token } = lease;

        let Some(handler) = self.handlers.get(&job.task_name).cloned() else {
            let err = format!("no handler registered for task '{}'", job.task_name);
            self.fail_terminally(&job, &lease_token, "handler_not_found", &err)
                .await;
            return;
        };

        // Extend the lease to cover the job's full timeout (plus grace for
        // bookkeeping). Without this, any job running longer than the claim
        // lease would be reaped by the janitor and retried concurrently.
        let needed = job.config.timeout_secs.saturating_add(LEASE_GRACE_SECS);
        if needed > self.lease_secs as u64 {
            let secs = needed.min(u32::MAX as u64) as u32;
            match self.store.extend_lease(&job.id, &lease_token, secs).await {
                Ok(Some(JobStatus::Running)) => {}
                Ok(_) => {
                    // Cancelled or reclaimed between claim and here: not ours.
                    tracing::info!(job_id = %job.id, "job lost its lease before starting; skipping");
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job.id, error = %e,
                        "failed to extend lease; a long-running job may be reaped while still running"
                    );
                }
            }
        }

        let timeout = StdDuration::from_secs(job.config.timeout_secs.max(1));
        // catch_unwind: a panicking handler must not take down the worker.
        // At-least-once delivery treats a panic like any other crash, so it
        // consumes retry budget and eventually dead-letters.
        let handler_fut =
            std::panic::AssertUnwindSafe(handler.handle(job.payload.clone())).catch_unwind();
        let outcome = tokio::time::timeout(timeout, handler_fut).await;

        match outcome {
            Ok(Err(panic)) => {
                let msg = format!("handler panicked: {}", panic_message(&*panic));
                self.handle_failure(&job, &lease_token, &msg, true).await;
            }
            Ok(Ok(Ok(result))) => {
                let result_value = serde_json::to_value(&result).unwrap_or(Json::Null);
                match self
                    .store
                    .finish_if_leased(
                        &job.id,
                        &lease_token,
                        JobStatus::Completed,
                        None,
                        Some(&result_value),
                    )
                    .await
                {
                    Ok(true) => {
                        EngineStats::incr(&self.stats.jobs_completed);
                        // Advance the workflow after the terminal write; if
                        // this fails (or we crash), the janitor's stalled-step
                        // sweep re-drives it.
                        if let (Some(wf), Some(step)) = (&job.workflow_id, &job.workflow_step_id) {
                            if let Err(e) =
                                self.scheduler.on_step_completed(wf, step, &result).await
                            {
                                tracing::error!(error = %e, job_id = %job.id, "workflow advance failed; janitor will self-heal");
                            }
                        }
                    }
                    Ok(false) => {
                        tracing::info!(job_id = %job.id, "lease lost before completion (cancelled or reclaimed); result dropped");
                    }
                    Err(e) => {
                        tracing::error!(job_id = %job.id, error = %e, "failed to record completion; lease will expire and the janitor will retry");
                    }
                }
            }
            Ok(Ok(Err(handler_err))) => {
                self.handle_failure(
                    &job,
                    &lease_token,
                    &handler_err.message,
                    handler_err.retryable,
                )
                .await;
            }
            Err(_elapsed) => {
                self.handle_failure(&job, &lease_token, "job exceeded its timeout", true)
                    .await;
            }
        }
    }

    /// Apply failure policy to a leased job: schedule a durable retry, or
    /// dead-letter and advance any workflow. Shared by the local worker loop,
    /// the remote-worker `fail` endpoint, and the janitor's expired-lease
    /// sweep. Returns whether the outcome was recorded (false = lease lost).
    async fn handle_failure(&self, job: &Job, token: &str, error: &str, retryable: bool) -> bool {
        let cfg = &job.config;
        if retryable && job.retry_count < cfg.max_retries {
            let attempt = job.retry_count;
            let now = self.clock.now();
            let next = cfg.retry_backoff.next_retry_at(cfg, attempt, now);

            // One lease-guarded row update is the entire durable retry; there
            // is no longer a mark-retrying / re-enqueue pair that can tear.
            match self
                .store
                .mark_retrying(&job.id, token, attempt + 1, next, error)
                .await
            {
                Ok(true) => {
                    EngineStats::incr(&self.stats.jobs_retried);
                    tracing::info!(job_id = %job.id, attempt = attempt + 1, next_retry_at = %next, "job scheduled for retry");
                    true
                }
                Ok(false) => {
                    tracing::info!(job_id = %job.id, "lease lost before retry could be recorded (cancelled or reclaimed)");
                    false
                }
                Err(e) => {
                    tracing::error!(job_id = %job.id, error = %e, "failed to record retry; lease will expire and the janitor will retry");
                    false
                }
            }
        } else {
            let reason = if retryable {
                "max_attempts_exceeded"
            } else {
                "non_retryable"
            };
            self.fail_terminally(job, token, reason, error).await
        }
    }

    /// Lease-guarded permanent failure: terminal write first, then the DLQ
    /// record and workflow advancement (both re-drivable; the DLQ row is
    /// advisory). Returns whether the failure was recorded.
    async fn fail_terminally(&self, job: &Job, token: &str, reason: &str, error: &str) -> bool {
        match self
            .store
            .finish_if_leased(&job.id, token, JobStatus::Failed, Some(error), None)
            .await
        {
            Ok(true) => {
                EngineStats::incr(&self.stats.jobs_failed);
                if let Err(e) = self.store.move_to_dlq(&job.id, reason, error).await {
                    tracing::error!(job_id = %job.id, error = %e, "failed to record dead letter");
                } else {
                    EngineStats::incr(&self.stats.jobs_dead_lettered);
                }
                if let (Some(wf), Some(step)) = (&job.workflow_id, &job.workflow_step_id) {
                    if let Err(e) = self.scheduler.on_step_failed(wf, step, error).await {
                        tracing::error!(error = %e, job_id = %job.id, "workflow failure handling failed; janitor will self-heal");
                    }
                }
                true
            }
            Ok(false) => {
                tracing::info!(job_id = %job.id, "lease lost before failure could be recorded (cancelled or reclaimed)");
                false
            }
            Err(e) => {
                tracing::error!(job_id = %job.id, error = %e, "failed to record failure; lease will expire and the janitor will retry");
                false
            }
        }
    }

    /// Re-drive workflow advancement for an already-terminal job (idempotent).
    /// Used by the janitor to recover an advance that failed after the
    /// terminal write.
    pub(crate) async fn resume_workflow(&self, job: &Job) {
        let (Some(wf), Some(step)) = (&job.workflow_id, &job.workflow_step_id) else {
            return;
        };
        let outcome = match job.status {
            JobStatus::Completed => {
                let result: Map = job
                    .result
                    .as_ref()
                    .and_then(|r| r.as_object())
                    .map(|o| o.clone().into_iter().collect())
                    .unwrap_or_default();
                self.scheduler.on_step_completed(wf, step, &result).await
            }
            JobStatus::Failed => {
                let err = job.error_message.clone().unwrap_or_default();
                self.scheduler.on_step_failed(wf, step, &err).await
            }
            // A step job cancelled directly (not via cancel_workflow) follows
            // the step's failure policy so the workflow can settle.
            JobStatus::Cancelled => {
                self.scheduler
                    .on_step_failed(wf, step, "job was cancelled")
                    .await
            }
            _ => Ok(()),
        };
        if let Err(e) = outcome {
            tracing::error!(error = %e, job_id = %job.id, "workflow resume failed");
        }
    }

    // ---- Remote worker protocol --------------------------------------------
    //
    // These four methods are the polyglot worker surface: lease, heartbeat,
    // complete, fail. They reuse exactly the machinery of the local worker
    // loop (lease guards, retry policy, DLQ, workflow advancement), so a
    // handler written in any language gets the same at-least-once semantics
    // as one compiled into this binary.

    /// Lease up to `count` jobs from `queue` for `lease_secs`, waiting up to
    /// `wait_secs` for work to arrive.
    pub async fn lease_jobs(
        &self,
        queue: &str,
        count: usize,
        lease_secs: u32,
        wait_secs: u32,
    ) -> Result<Vec<LeasedJob>, EngineError> {
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(wait_secs as u64);
        loop {
            let claimed = self.store.claim_jobs(queue, count, lease_secs).await?;
            if !claimed.jobs.is_empty() {
                return Ok(claimed.jobs);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(vec![]);
            }
            let wait = self.idle_wait(claimed.next_due).min(remaining);
            self.store.await_work(queue, wait).await?;
        }
    }

    /// Extend a lease so a still-running remote job is not reaped. Returns the
    /// job's current status: `Running` means extended; anything else means the
    /// job finished or was cancelled mid-run and the worker should stop.
    pub async fn heartbeat_lease(
        &self,
        job_id: &str,
        lease_token: &str,
        extend_secs: u32,
    ) -> Result<JobStatus, EngineError> {
        match self
            .store
            .extend_lease(job_id, lease_token, extend_secs)
            .await?
        {
            Some(status) => Ok(status),
            None => Err(EngineError::Conflict(
                "lease is no longer held (it expired and the job was reclaimed)".into(),
            )),
        }
    }

    /// Complete a leased job with a result. Replaying a finished job is an
    /// idempotent success; reporting against a lost lease is a conflict.
    pub async fn complete_leased(
        &self,
        job_id: &str,
        lease_token: &str,
        result: Map,
    ) -> Result<(), EngineError> {
        let result_value = serde_json::to_value(&result).unwrap_or(Json::Null);
        let finished = self
            .store
            .finish_if_leased(
                job_id,
                lease_token,
                JobStatus::Completed,
                None,
                Some(&result_value),
            )
            .await?;
        if finished {
            EngineStats::incr(&self.stats.jobs_completed);
            let job = self.store.get_job(job_id).await?;
            if let (Some(wf), Some(step)) = (&job.workflow_id, &job.workflow_step_id) {
                if let Err(e) = self.scheduler.on_step_completed(wf, step, &result).await {
                    // The completion is recorded; the janitor re-drives the
                    // advance. Failing the request would only provoke a
                    // pointless client retry.
                    tracing::error!(error = %e, job_id = %job_id, "workflow advance failed; janitor will self-heal");
                }
            }
            return Ok(());
        }
        // Guard failed: missing (404), idempotent replay of a finished job
        // (ok), or a genuinely lost lease (409).
        let job = self.store.get_job(job_id).await?;
        if job.status.is_terminal() {
            return Ok(());
        }
        Err(EngineError::Conflict(
            "lease is no longer held; the job was reclaimed".into(),
        ))
    }

    /// Fail a leased job, applying the normal retry / dead-letter policy.
    pub async fn fail_leased(
        &self,
        job_id: &str,
        lease_token: &str,
        error: &str,
        retryable: bool,
    ) -> Result<(), EngineError> {
        let job = self.store.get_job(job_id).await?;
        if job.status.is_terminal() {
            return Ok(()); // idempotent replay
        }
        if self
            .handle_failure(&job, lease_token, error, retryable)
            .await
        {
            return Ok(());
        }
        // Nothing was recorded; distinguish a racing finish from a lost lease.
        let job = self.store.get_job(job_id).await?;
        if job.status.is_terminal() {
            return Ok(());
        }
        Err(EngineError::Conflict(
            "lease is no longer held; the job was reclaimed".into(),
        ))
    }

    /// Signal all workers (and the janitor) to stop. They drain in-flight
    /// work, then exit.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.shutdown.cancel();
    }
}
