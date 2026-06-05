//! The job-processing engine: enqueue, worker loop, durable retries, and
//! workflow delegation.

pub mod retry;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::domain::*;
use crate::error::EngineError;
use crate::ports::*;
use crate::stats::EngineStats;
use crate::task::{handler_fn, TaskHandler};
use crate::workflow::scheduler::WorkflowScheduler;

/// Options for [`Engine::enqueue`].
#[derive(Clone, Debug, Default)]
pub struct EnqueueOptions {
    pub config: Option<JobConfig>,
    pub queue: Option<String>,
    pub tenant_id: Option<String>,
    pub metadata: Map,
}

/// The core engine, generic over the storage and queue ports.
///
/// Construct via [`Engine::builder`]. The engine is wrapped in an `Arc` so
/// worker tasks can be spawned cheaply.
pub struct Engine<JS, MQ> {
    store: Arc<JS>,
    queue: Arc<MQ>,
    clock: Arc<dyn Clock>,
    handlers: HashMap<String, Arc<dyn TaskHandler>>,
    scheduler: WorkflowScheduler<JS, MQ>,
    default_queue: String,
    worker_count: usize,
    read_vt_secs: u32,
    stats: Arc<EngineStats>,
    shutdown: CancellationToken,
    running: AtomicBool,
}

/// Fluent constructor for [`Engine`].
pub struct EngineBuilder<JS, MQ> {
    store: Arc<JS>,
    queue: Arc<MQ>,
    clock: Arc<dyn Clock>,
    handlers: HashMap<String, Arc<dyn TaskHandler>>,
    default_queue: String,
    worker_count: usize,
    read_vt_secs: u32,
}

impl<JS, MQ> EngineBuilder<JS, MQ>
where
    JS: JobStore + 'static,
    MQ: MessageQueue + 'static,
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

    /// Visibility timeout (seconds) applied when leasing a message.
    pub fn visibility_timeout_secs(mut self, secs: u32) -> Self {
        self.read_vt_secs = secs.max(1);
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

    pub fn build(self) -> Arc<Engine<JS, MQ>> {
        let stats = Arc::new(EngineStats::default());
        let scheduler = WorkflowScheduler::new(
            self.store.clone(),
            self.queue.clone(),
            self.clock.clone(),
            self.default_queue.clone(),
            stats.clone(),
        );
        Arc::new(Engine {
            store: self.store,
            queue: self.queue,
            clock: self.clock,
            handlers: self.handlers,
            scheduler,
            default_queue: self.default_queue,
            worker_count: self.worker_count,
            read_vt_secs: self.read_vt_secs,
            stats,
            shutdown: CancellationToken::new(),
            running: AtomicBool::new(false),
        })
    }
}

impl<JS, MQ> Engine<JS, MQ>
where
    JS: JobStore + 'static,
    MQ: MessageQueue + 'static,
{
    pub fn builder(store: Arc<JS>, queue: Arc<MQ>, clock: Arc<dyn Clock>) -> EngineBuilder<JS, MQ> {
        EngineBuilder {
            store,
            queue,
            clock,
            handlers: HashMap::new(),
            default_queue: "default".to_string(),
            worker_count: 8,
            read_vt_secs: 30,
        }
    }

    pub fn stats(&self) -> Arc<EngineStats> {
        self.stats.clone()
    }

    pub fn default_queue(&self) -> &str {
        &self.default_queue
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

    /// Enqueue a single job.
    pub async fn enqueue(
        &self,
        task_name: &str,
        payload: Map,
        opts: EnqueueOptions,
    ) -> Result<String, EngineError> {
        let queue = opts.queue.unwrap_or_else(|| self.default_queue.clone());
        let job = Job {
            id: Uuid::new_v4().to_string(),
            queue_name: queue.clone(),
            task_name: task_name.to_string(),
            payload,
            config: opts.config.unwrap_or_default(),
            status: JobStatus::Pending,
            created_at: self.clock.now(),
            started_at: None,
            completed_at: None,
            error_message: None,
            retry_count: 0,
            next_retry_at: None,
            workflow_id: None,
            workflow_step_id: None,
            result: None,
            metadata: opts.metadata,
            tenant_id: opts.tenant_id,
        };

        // Persist the row first (durable record), then publish to the queue, so
        // a crash in between leaves a queryable job, never a phantom message.
        self.store.create_job(&job).await?;
        let msg = QueueMessage::for_job(&job);
        self.queue.send(&queue, &msg, job.config.priority).await?;
        EngineStats::incr(&self.stats.jobs_created);
        Ok(job.id)
    }

    /// Enqueue many jobs efficiently.
    pub async fn enqueue_batch(
        &self,
        requests: Vec<(String, Map, Option<JobConfig>)>,
        tenant_id: Option<String>,
    ) -> Result<Vec<String>, EngineError> {
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
                started_at: None,
                completed_at: None,
                error_message: None,
                retry_count: 0,
                next_retry_at: None,
                workflow_id: None,
                workflow_step_id: None,
                result: None,
                metadata: Map::new(),
                tenant_id: tenant_id.clone(),
            })
            .collect();

        let ids = self.store.batch_create_jobs(&jobs).await?;
        for job in &jobs {
            let msg = QueueMessage::for_job(job);
            self.queue
                .send(&job.queue_name, &msg, job.config.priority)
                .await?;
            EngineStats::incr(&self.stats.jobs_created);
        }
        Ok(ids)
    }

    pub async fn get_job(&self, id: &str) -> Result<Job, EngineError> {
        Ok(self.store.get_job(id).await?)
    }

    pub async fn list_jobs(&self, filter: &ListFilter) -> Result<(Vec<Job>, i64), EngineError> {
        Ok(self.store.list_jobs(filter).await?)
    }

    /// Cancel a job. Running work is signalled via status; pending/retrying
    /// work simply won't run once it is marked cancelled.
    pub async fn cancel_job(&self, id: &str) -> Result<(), EngineError> {
        let _ = self.store.get_job(id).await?; // 404 if missing
        self.store
            .update_status(id, JobStatus::Cancelled, Some("cancelled by user"), None)
            .await?;
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

    pub async fn list_workflows(
        &self,
        filter: &ListFilter,
    ) -> Result<(Vec<Workflow>, i64), EngineError> {
        Ok(self.store.list_workflows(filter).await?)
    }

    pub async fn cancel_workflow(&self, id: &str) -> Result<(), EngineError> {
        let _ = self.store.get_workflow(id).await?; // 404 if missing
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
            }
        }
        self.store
            .set_workflow_status(id, WorkflowStatus::Cancelled)
            .await?;
        Ok(())
    }

    pub async fn workflow_diagram(&self, id: &str) -> Result<String, EngineError> {
        self.scheduler.diagram(id).await
    }

    pub async fn ping(&self) -> Result<(), EngineError> {
        Ok(self.store.ping().await?)
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    // ---- Worker loop ------------------------------------------------------

    /// Spawn `worker_count` concurrent workers draining `queue`. The returned
    /// [`JoinSet`] resolves when all workers stop (on shutdown). Call
    /// [`Engine::shutdown`] (or cancel via the token) to stop them.
    pub fn run_workers(self: &Arc<Self>, queue: impl Into<String>) -> JoinSet<()> {
        let queue = queue.into();
        self.running.store(true, Ordering::SeqCst);
        let mut set = JoinSet::new();
        for worker_id in 0..self.worker_count {
            let engine = Arc::clone(self);
            let q = queue.clone();
            set.spawn(async move { engine.worker_loop(worker_id, q).await });
        }
        set
    }

    async fn worker_loop(self: Arc<Self>, worker_id: usize, queue: String) {
        tracing::info!(worker_id, queue = %queue, "worker started");
        loop {
            if self.shutdown.is_cancelled() {
                break;
            }
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                read = self.queue.read(&queue, self.read_vt_secs, 1) => {
                    match read {
                        Ok(mut msgs) => {
                            if let Some(rm) = msgs.pop() {
                                self.process_message(&queue, rm).await;
                            } else {
                                // No work: back off briefly, but stay cancellable.
                                tokio::select! {
                                    _ = self.shutdown.cancelled() => break,
                                    _ = tokio::time::sleep(StdDuration::from_millis(200)) => {}
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(worker_id, error = %e, "queue read error");
                            tokio::select! {
                                _ = self.shutdown.cancelled() => break,
                                _ = tokio::time::sleep(StdDuration::from_secs(1)) => {}
                            }
                        }
                    }
                }
            }
        }
        tracing::info!(worker_id, queue = %queue, "worker stopped");
    }

    /// Lease and process at most one message. Returns whether a message was
    /// handled. Primarily used to drive the engine deterministically in tests.
    pub async fn process_once(&self, queue: &str) -> Result<bool, EngineError> {
        let mut msgs = self.queue.read(queue, self.read_vt_secs, 1).await?;
        match msgs.pop() {
            Some(rm) => {
                self.process_message(queue, rm).await;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Acknowledge (remove) a processed message, logging — never silently
    /// swallowing — failures. A failed ack means the message will be redelivered
    /// after its visibility timeout; the terminal-state guard in
    /// [`Engine::process_message`] then makes that redelivery idempotent.
    async fn ack(&self, queue: &str, msg_id: i64) {
        if let Err(e) = self.queue.delete(queue, msg_id).await {
            tracing::warn!(queue, msg_id, error = %e, "failed to ack message; it may be redelivered");
        }
    }

    /// Execute one leased message end to end: load the job, run its handler with
    /// a timeout, then complete / retry / dead-letter and advance any workflow.
    ///
    /// Delivery is **at-least-once**: a message may be redelivered (an ack
    /// failed, or a worker crashed after finishing). Handlers should therefore
    /// be idempotent. The engine adds two safety nets: a terminal-state guard so
    /// a finished job's handler is never re-run, and "advance the workflow
    /// before acking" so a failed advance is retried rather than lost.
    pub async fn process_message(&self, queue: &str, rm: ReadMessage) {
        let job_id = rm.message.job_id.clone();

        let job = match self.store.get_job(&job_id).await {
            Ok(j) => j,
            Err(_) => {
                // The durable row is gone; drop the orphaned message.
                self.ack(queue, rm.msg_id).await;
                return;
            }
        };

        // Idempotency guard: never re-run a terminal job (completed / failed /
        // cancelled). Instead re-drive any workflow advancement — which is
        // idempotent — so a step whose post-completion advance failed before
        // ack can self-heal, then ack.
        if job.status.is_terminal() {
            self.resume_workflow(&job).await;
            self.ack(queue, rm.msg_id).await;
            return;
        }

        let Some(handler) = self.handlers.get(&job.task_name).cloned() else {
            let err = format!("no handler registered for task '{}'", job.task_name);
            let _ = self
                .store
                .move_to_dlq(&job_id, "handler_not_found", &err)
                .await;
            let _ = self
                .store
                .update_status(&job_id, JobStatus::Failed, Some(&err), None)
                .await;
            EngineStats::incr(&self.stats.jobs_failed);
            EngineStats::incr(&self.stats.jobs_dead_lettered);
            self.advance_or_keep(queue, &rm, &job, &err, JobStatus::Failed)
                .await;
            return;
        };

        let _ = self
            .store
            .update_status(&job_id, JobStatus::Running, None, None)
            .await;

        let timeout = StdDuration::from_secs(job.config.timeout_secs.max(1));
        let outcome = tokio::time::timeout(timeout, handler.handle(job.payload.clone())).await;

        match outcome {
            Ok(Ok(result)) => {
                let result_value = serde_json::to_value(&result).unwrap_or(Json::Null);
                let _ = self
                    .store
                    .update_status(&job_id, JobStatus::Completed, None, Some(&result_value))
                    .await;
                EngineStats::incr(&self.stats.jobs_completed);

                // Advance the workflow BEFORE acking so a failed advance leaves
                // the message to be redelivered and self-healed by the guard.
                if let (Some(wf), Some(step)) = (&job.workflow_id, &job.workflow_step_id) {
                    if let Err(e) = self.scheduler.on_step_completed(wf, step, &result).await {
                        tracing::error!(error = %e, job_id = %job_id, "workflow advance failed; message will be redelivered");
                        return; // do NOT ack — redelivery self-heals
                    }
                }
                self.ack(queue, rm.msg_id).await;
            }
            Ok(Err(handler_err)) => {
                self.handle_failure(
                    queue,
                    &rm,
                    &job,
                    &handler_err.message,
                    handler_err.retryable,
                )
                .await;
            }
            Err(_elapsed) => {
                self.handle_failure(queue, &rm, &job, "job exceeded its timeout", true)
                    .await;
            }
        }
    }

    async fn handle_failure(
        &self,
        queue: &str,
        rm: &ReadMessage,
        job: &Job,
        error: &str,
        retryable: bool,
    ) {
        let cfg = &job.config;
        if retryable && job.retry_count < cfg.max_retries {
            let attempt = job.retry_count;
            let now = self.clock.now();
            let next = cfg.retry_backoff.next_retry_at(cfg, attempt, now);
            let delay = (next - now).num_seconds().max(0) as u64;

            let _ = self
                .store
                .mark_retrying(&job.id, attempt + 1, next, error)
                .await;
            // Durable retry: the delay lives in the queue, surviving restarts.
            // Only ack the original once the delayed copy is safely enqueued, so
            // a send failure can't drop the job entirely.
            let msg = QueueMessage::for_job(job);
            match self
                .queue
                .send_delayed(queue, &msg, cfg.priority, delay)
                .await
            {
                Ok(_) => {
                    self.ack(queue, rm.msg_id).await;
                    EngineStats::incr(&self.stats.jobs_retried);
                    tracing::info!(job_id = %job.id, attempt = attempt + 1, delay_secs = delay, "job scheduled for retry");
                }
                Err(e) => {
                    tracing::error!(job_id = %job.id, error = %e, "failed to enqueue retry; original message will be redelivered");
                }
            }
        } else {
            let reason = if retryable {
                "max_attempts_exceeded"
            } else {
                "non_retryable"
            };
            let _ = self.store.move_to_dlq(&job.id, reason, error).await;
            let _ = self
                .store
                .update_status(&job.id, JobStatus::Failed, Some(error), None)
                .await;
            EngineStats::incr(&self.stats.jobs_failed);
            EngineStats::incr(&self.stats.jobs_dead_lettered);
            self.advance_or_keep(queue, rm, job, error, JobStatus::Failed)
                .await;
        }
    }

    /// Advance a workflow after a terminal failure, then ack — but if the
    /// advance fails, leave the message so it is redelivered and self-healed by
    /// the terminal-state guard. Non-workflow jobs are simply acked.
    async fn advance_or_keep(
        &self,
        queue: &str,
        rm: &ReadMessage,
        job: &Job,
        error: &str,
        _terminal: JobStatus,
    ) {
        if let (Some(wf), Some(step)) = (&job.workflow_id, &job.workflow_step_id) {
            if let Err(e) = self.scheduler.on_step_failed(wf, step, error).await {
                tracing::error!(error = %e, job_id = %job.id, "workflow failure handling failed; message will be redelivered");
                return; // do NOT ack — redelivery self-heals
            }
        }
        self.ack(queue, rm.msg_id).await;
    }

    /// Re-drive workflow advancement for an already-terminal job (idempotent).
    /// Used by the redelivery guard to recover an advance that failed before ack.
    async fn resume_workflow(&self, job: &Job) {
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
            _ => Ok(()),
        };
        if let Err(e) = outcome {
            tracing::error!(error = %e, job_id = %job.id, "workflow resume failed");
        }
    }

    /// Signal all workers to stop. They drain in-flight work, then exit.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.shutdown.cancel();
    }
}
