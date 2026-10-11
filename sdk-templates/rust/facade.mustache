/*
Ergonomic facade for the QueueFlow Rust SDK.

Hand-written ergonomics layered on the GENERATED core (apis + models).
Injected at generation time as a supporting file, so it ships with the
generated crate but is never produced by the raw codegen. The wire types and
the per-tag API modules come from the generated files and are never edited.

The facade is the recommended entry point: a `QueueFlow` client with one
method per endpoint, `wait_for_job` / `wait_for_workflow` pollers, a workflow
builder with local DAG validation, typed errors, and a worker runtime
(`run_worker`) that leases jobs, heartbeats them, and reports outcomes.
*/

use std::collections::{HashMap, HashSet};
use std::error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::apis::configuration::Configuration;
use crate::apis::{
    cron_api, dlq_api, health_api, jobs_api, system_api, worker_api, workflows_api,
    Error as ApiError,
};
use crate::models;

/// A JSON object, as used for payloads, results, context and metadata.
pub type Json = HashMap<String, serde_json::Value>;

/// Per-request HTTP timeout applied when `QueueFlowOptions::timeout` is not
/// set.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Added to a lease call's `wait_secs` to form its deadline: a long poll must
/// outlive the regular per-request timeout.
const LEASE_TIMEOUT_SLACK: Duration = Duration::from_secs(35);

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors surfaced by the facade.
#[derive(Debug)]
pub enum FacadeError {
    /// A non-2xx response. `body` is the raw error body (usually
    /// `{"error": "..."}`), possibly empty.
    Http {
        /// The facade method that failed, e.g. `"get_job"`.
        op: &'static str,
        status: u16,
        body: String,
    },
    /// The request never produced a response, or the response did not
    /// decode (connection refused, TLS, JSON).
    Transport {
        op: &'static str,
        source: Box<dyn error::Error + Send + Sync>,
    },
    /// A workflow definition failed local validation before any round-trip.
    InvalidWorkflow(String),
    /// `QueueFlow::with_options` was given unusable options.
    InvalidOptions(String),
    /// A `wait_for_*` poller, or a worker-protocol call, ran out of time.
    Timeout(String),
}

impl FacadeError {
    /// The HTTP status for an `Http` error, `None` otherwise.
    pub fn status(&self) -> Option<u16> {
        match self {
            FacadeError::Http { status, .. } => Some(*status),
            _ => None,
        }
    }
}

impl fmt::Display for FacadeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FacadeError::Http { op, status, body } => {
                write!(f, "queueflow: {}: HTTP {}: {}", op, status, body.trim())
            }
            FacadeError::Transport { op, source } => write!(f, "queueflow: {}: {}", op, source),
            FacadeError::InvalidWorkflow(msg)
            | FacadeError::InvalidOptions(msg)
            | FacadeError::Timeout(msg) => write!(f, "{}", msg),
        }
    }
}

impl error::Error for FacadeError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            FacadeError::Transport { source, .. } => {
                let e: &(dyn error::Error + 'static) = source.as_ref();
                Some(e)
            }
            _ => None,
        }
    }
}

/// Maps a generated-client error to a `FacadeError` for operation `op`.
fn api_err<T>(op: &'static str) -> impl Fn(ApiError<T>) -> FacadeError {
    move |e| match e {
        ApiError::ResponseError(content) => FacadeError::Http {
            op,
            status: content.status.as_u16(),
            body: content.content,
        },
        ApiError::Reqwest(e) => FacadeError::Transport {
            op,
            source: Box::new(e),
        },
        ApiError::Serde(e) => FacadeError::Transport {
            op,
            source: Box::new(e),
        },
        ApiError::Io(e) => FacadeError::Transport {
            op,
            source: Box::new(e),
        },
    }
}

/// Bounds a worker-protocol call, whose HTTP client carries no global timeout.
async fn bounded<T, F>(op: &'static str, limit: Duration, fut: F) -> Result<T, FacadeError>
where
    F: Future<Output = Result<T, FacadeError>>,
{
    match tokio::time::timeout(limit, fut).await {
        Ok(result) => result,
        Err(_) => Err(FacadeError::Timeout(format!(
            "queueflow: {} did not complete within {:?}",
            op, limit
        ))),
    }
}

fn job_terminal(status: models::JobStatus) -> bool {
    matches!(
        status,
        models::JobStatus::Completed | models::JobStatus::Failed | models::JobStatus::Cancelled
    )
}

fn workflow_terminal(status: models::WorkflowStatus) -> bool {
    matches!(
        status,
        models::WorkflowStatus::Completed
            | models::WorkflowStatus::Failed
            | models::WorkflowStatus::PartiallyFailed
            | models::WorkflowStatus::Cancelled
    )
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Options for [`QueueFlow::with_options`].
#[derive(Clone, Debug)]
pub struct QueueFlowOptions {
    /// Server base URL, e.g. `http://localhost:8000`.
    pub base_url: String,
    /// Tenant bearer token (jobs, workflows, cron, DLQ, system routes).
    pub token: String,
    /// Credential for the worker-protocol routes (`lease_jobs`,
    /// `heartbeat_job`, `complete_job`, `fail_job`, `run_worker`). Servers
    /// started with `--worker-token` refuse tenant tokens there. Defaults to
    /// `token`, which only works on a server running without a worker token.
    pub worker_token: Option<String>,
    /// Per-request HTTP timeout (default [`DEFAULT_TIMEOUT`]). Lease calls
    /// are exempt: they get `wait_secs` plus 35 seconds instead.
    pub timeout: Duration,
}

impl QueueFlowOptions {
    /// Options with the defaults: no separate worker token, 30s timeout.
    pub fn new(base_url: &str, token: &str) -> Self {
        QueueFlowOptions {
            base_url: base_url.to_owned(),
            token: token.to_owned(),
            worker_token: None,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Sets the worker-protocol credential.
    pub fn worker_token(mut self, token: &str) -> Self {
        self.worker_token = Some(token.to_owned());
        self
    }

    /// Sets the per-request HTTP timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// QueueFlow is an ergonomic wrapper over the generated API modules.
///
/// ```no_run
/// # async fn demo() -> Result<(), queueflow_sdk::FacadeError> {
/// use queueflow_sdk::{QueueFlow, WaitOptions};
///
/// let qf = QueueFlow::new("http://localhost:8000", "dev");
/// let id = qf.create_job("echo", None).await?;
/// let done = qf.wait_for_job(&id, WaitOptions::default()).await?;
/// println!("{}", done.status);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct QueueFlow {
    /// The generated client configuration, authenticated with the tenant
    /// token. Pass `&qf.config` to any `apis::*` function the helpers do not
    /// cover.
    pub config: Configuration,
    /// Like `config`, but authenticated with the worker token and with no
    /// client-level timeout (worker calls are bounded per call).
    pub worker_config: Configuration,
    timeout: Duration,
}

impl QueueFlow {
    /// Builds a client pointed at `base_url`, authenticating with `token`.
    /// The worker token defaults to `token`; use [`QueueFlow::with_options`]
    /// to set a separate one (required by servers started with
    /// `--worker-token`).
    pub fn new(base_url: &str, token: &str) -> Self {
        Self::build(QueueFlowOptions::new(base_url, token))
            .expect("queueflow: failed to build the HTTP client")
    }

    /// Builds a client from `opts`, validating that `base_url` and `token`
    /// are set.
    pub fn with_options(opts: QueueFlowOptions) -> Result<Self, FacadeError> {
        if opts.base_url.is_empty() {
            return Err(FacadeError::InvalidOptions(
                "queueflow: base_url is required".to_owned(),
            ));
        }
        if opts.token.is_empty() {
            return Err(FacadeError::InvalidOptions(
                "queueflow: token is required".to_owned(),
            ));
        }
        Self::build(opts)
    }

    fn build(opts: QueueFlowOptions) -> Result<Self, FacadeError> {
        let timeout = if opts.timeout.is_zero() {
            DEFAULT_TIMEOUT
        } else {
            opts.timeout
        };
        let base_path = opts.base_url.trim_end_matches('/').to_owned();
        let client_err = |e: reqwest::Error| FacadeError::InvalidOptions(e.to_string());

        let mut config = Configuration::new();
        config.base_path = base_path.clone();
        config.bearer_access_token = Some(opts.token.clone());
        config.client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(client_err)?;

        let mut worker_config = Configuration::new();
        worker_config.base_path = base_path;
        worker_config.bearer_access_token = Some(opts.worker_token.unwrap_or(opts.token));
        worker_config.client = reqwest::Client::builder().build().map_err(client_err)?;

        Ok(QueueFlow {
            config,
            worker_config,
            timeout,
        })
    }

    /// The per-request HTTP timeout in effect.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

// ---------------------------------------------------------------------------
// Shared option types
// ---------------------------------------------------------------------------

/// Filters for the list endpoints. `None` fields are not sent, so the server
/// defaults apply.
#[derive(Clone, Debug, Default)]
pub struct ListOptions {
    pub status: Option<String>,
    pub queue: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// `"created_at ASC"` or `"created_at DESC"`.
    pub order_by: Option<String>,
    pub include_total: Option<bool>,
    /// Opaque keyset cursor from a previous page's `next_cursor`. When set,
    /// `offset` is ignored (cheaper than deep OFFSET paging).
    pub cursor: Option<String>,
    /// RFC 3339 instant; only rows created at or after it (inclusive).
    pub created_after: Option<String>,
    /// RFC 3339 instant; only rows created strictly before it (exclusive).
    pub created_before: Option<String>,
}

/// Expands to a generated list call with every `ListOptions` field passed in
/// the generated functions' positional order.
macro_rules! list_call {
    ($f:path, $config:expr, $o:expr) => {
        $f(
            $config,
            $o.status.as_deref(),
            $o.queue.as_deref(),
            $o.limit,
            $o.offset,
            $o.order_by.as_deref(),
            $o.include_total,
            $o.cursor.as_deref(),
            $o.created_after.clone(),
            $o.created_before.clone(),
        )
    };
}

/// Tunes `wait_for_job` and `wait_for_workflow`. The default is a 60 second
/// timeout and a 500 millisecond poll interval.
#[derive(Clone, Copy, Debug)]
pub struct WaitOptions {
    pub timeout: Duration,
    pub interval: Duration,
}

impl Default for WaitOptions {
    fn default() -> Self {
        WaitOptions {
            timeout: Duration::from_secs(60),
            interval: Duration::from_millis(500),
        }
    }
}

impl WaitOptions {
    /// `WaitOptions` with the given timeout and the default interval.
    pub fn with_timeout(timeout: Duration) -> Self {
        WaitOptions {
            timeout,
            ..WaitOptions::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

/// Everything `CreateJobRequest` accepts. `None` fields are not sent, so the
/// server defaults apply.
#[derive(Clone, Debug, Default)]
pub struct CreateJobOptions {
    pub payload: Option<Json>,
    /// Override the destination queue.
    pub queue: Option<String>,
    /// Higher is claimed first within a queue.
    pub priority: Option<i32>,
    /// Max retry attempts before dead-lettering.
    pub max_retries: Option<i32>,
    /// Per-attempt timeout, in seconds.
    pub timeout_secs: Option<i64>,
    /// How retry delays grow between attempts (default exponential).
    pub retry_backoff: Option<models::BackoffStrategy>,
    /// Base retry delay, in seconds.
    pub retry_delay_secs: Option<i64>,
    /// Upper bound on any computed retry delay, in seconds.
    pub retry_max_delay_secs: Option<i64>,
    /// Retry-delay jitter in `0.0..=1.0` (`0.1` = +/-10%).
    pub jitter_factor: Option<f64>,
    /// Makes the create idempotent per tenant (sent as `Idempotency-Key`):
    /// the same key returns the original job id.
    pub idempotency_key: Option<String>,
    /// RFC 3339 instant before which the job must not run. It is created
    /// immediately but stays invisible to workers until then.
    pub run_at: Option<String>,
}

impl CreateJobOptions {
    fn request(self, task: &str) -> (models::CreateJobRequest, Option<String>) {
        let mut req = models::CreateJobRequest::new(task.to_owned());
        req.payload = self.payload;
        let mut cfg = models::JobConfigRequest::new();
        let mut set = false;
        if let Some(v) = self.queue {
            cfg.queue = Some(Some(v));
            set = true;
        }
        if let Some(v) = self.priority {
            cfg.priority = Some(Some(v));
            set = true;
        }
        if let Some(v) = self.max_retries {
            cfg.max_retries = Some(Some(v));
            set = true;
        }
        if let Some(v) = self.timeout_secs {
            cfg.timeout = Some(Some(v));
            set = true;
        }
        if let Some(v) = self.retry_backoff {
            cfg.retry_backoff = Some(Some(v));
            set = true;
        }
        if let Some(v) = self.retry_delay_secs {
            cfg.retry_delay_secs = Some(Some(v));
            set = true;
        }
        if let Some(v) = self.retry_max_delay_secs {
            cfg.retry_max_delay_secs = Some(Some(v));
            set = true;
        }
        if let Some(v) = self.jitter_factor {
            cfg.jitter_factor = Some(Some(v));
            set = true;
        }
        if set {
            req.config = Some(Some(Box::new(cfg)));
        }
        if let Some(v) = self.run_at {
            req.run_at = Some(Some(v));
        }
        (req, self.idempotency_key)
    }
}

impl QueueFlow {
    /// Enqueues a job with the server's default config and returns the new
    /// job id. See [`QueueFlow::create_job_with_options`] for queue,
    /// priority, retries, idempotency and `run_at`.
    pub async fn create_job(&self, task: &str, payload: Option<Json>) -> Result<String, FacadeError> {
        self.create_job_with_options(
            task,
            CreateJobOptions {
                payload,
                ..CreateJobOptions::default()
            },
        )
        .await
    }

    /// Enqueues a job and returns the new job id (the original job's id when
    /// `idempotency_key` matches an earlier create).
    pub async fn create_job_with_options(
        &self,
        task: &str,
        opts: CreateJobOptions,
    ) -> Result<String, FacadeError> {
        let (req, key) = opts.request(task);
        let created = jobs_api::create_job(&self.config, req, key.as_deref())
            .await
            .map_err(api_err("create_job"))?;
        Ok(created.job_id)
    }

    /// Fetches a job record.
    pub async fn get_job(&self, id: &str) -> Result<models::Job, FacadeError> {
        jobs_api::get_job(&self.config, id)
            .await
            .map_err(api_err("get_job"))
    }

    /// Lists jobs. The response carries `next_cursor` when more pages exist.
    pub async fn list_jobs(&self, opts: &ListOptions) -> Result<models::ListJobsResponse, FacadeError> {
        list_call!(jobs_api::list_jobs, &self.config, opts)
            .await
            .map_err(api_err("list_jobs"))
    }

    /// Cancels a pending, scheduled or running job.
    pub async fn cancel_job(&self, id: &str) -> Result<(), FacadeError> {
        jobs_api::cancel_job(&self.config, id)
            .await
            .map_err(api_err("cancel_job"))
    }

    /// Polls until the job reaches a terminal state (completed, failed or
    /// cancelled), or `FacadeError::Timeout` past `opts.timeout`.
    pub async fn wait_for_job(&self, id: &str, opts: WaitOptions) -> Result<models::Job, FacadeError> {
        let deadline = Instant::now() + opts.timeout;
        loop {
            let job = self.get_job(id).await?;
            if job_terminal(job.status) {
                return Ok(job);
            }
            if Instant::now() + opts.interval > deadline {
                return Err(FacadeError::Timeout(format!(
                    "job {} did not finish within {:?}",
                    id, opts.timeout
                )));
            }
            tokio::time::sleep(opts.interval).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------

impl QueueFlow {
    /// Validates the builder's DAG locally, creates the workflow and returns
    /// the new workflow id.
    pub async fn create_workflow(&self, builder: &WorkflowBuilder) -> Result<String, FacadeError> {
        let body = builder.build()?;
        let created = workflows_api::create_workflow(&self.config, body)
            .await
            .map_err(api_err("create_workflow"))?;
        Ok(created.workflow_id)
    }

    /// Fetches a workflow record (its step definitions, not their runtime
    /// status; see [`QueueFlow::workflow_steps`]).
    pub async fn get_workflow(&self, id: &str) -> Result<models::Workflow, FacadeError> {
        workflows_api::get_workflow(&self.config, id)
            .await
            .map_err(api_err("get_workflow"))
    }

    /// Lists workflows.
    pub async fn list_workflows(
        &self,
        opts: &ListOptions,
    ) -> Result<models::ListWorkflowsResponse, FacadeError> {
        list_call!(workflows_api::list_workflows, &self.config, opts)
            .await
            .map_err(api_err("list_workflows"))
    }

    /// Cancels a workflow and its not-yet-finished steps.
    pub async fn cancel_workflow(&self, id: &str) -> Result<(), FacadeError> {
        workflows_api::cancel_workflow(&self.config, id)
            .await
            .map_err(api_err("cancel_workflow"))
    }

    /// The Mermaid (`graph TD`) diagram of the DAG.
    pub async fn workflow_diagram(
        &self,
        id: &str,
    ) -> Result<models::WorkflowDiagramResponse, FacadeError> {
        workflows_api::get_workflow_diagram(&self.config, id)
            .await
            .map_err(api_err("workflow_diagram"))
    }

    /// Runtime status of every step in declaration order, with the id of the
    /// job executing each step once scheduled.
    pub async fn workflow_steps(&self, id: &str) -> Result<Vec<models::WorkflowStepState>, FacadeError> {
        workflows_api::get_workflow_step_states(&self.config, id)
            .await
            .map(|r| r.steps)
            .map_err(api_err("workflow_steps"))
    }

    /// Polls until the workflow reaches a terminal state (completed, failed,
    /// partially_failed or cancelled), or `FacadeError::Timeout`.
    pub async fn wait_for_workflow(
        &self,
        id: &str,
        opts: WaitOptions,
    ) -> Result<models::Workflow, FacadeError> {
        let deadline = Instant::now() + opts.timeout;
        loop {
            let wf = self.get_workflow(id).await?;
            if workflow_terminal(wf.status) {
                return Ok(wf);
            }
            if Instant::now() + opts.interval > deadline {
                return Err(FacadeError::Timeout(format!(
                    "workflow {} did not finish within {:?}",
                    id, opts.timeout
                )));
            }
            tokio::time::sleep(opts.interval).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Cron
// ---------------------------------------------------------------------------

/// The optional `CreateCronRequest` fields.
#[derive(Clone, Debug, Default)]
pub struct CreateCronOptions {
    /// Sent with every firing.
    pub payload: Option<Json>,
    /// Queue for the enqueued jobs (server default when `None`).
    pub queue: Option<String>,
    /// Execution config override for the enqueued jobs.
    pub config: Option<models::JobConfig>,
}

impl QueueFlow {
    /// Registers a recurring enqueue (5-field crontab, evaluated in UTC) and
    /// returns the schedule id.
    pub async fn create_cron(&self, name: &str, schedule: &str, task: &str) -> Result<String, FacadeError> {
        self.create_cron_with_options(name, schedule, task, CreateCronOptions::default())
            .await
    }

    /// Registers a recurring enqueue with a payload, queue and config
    /// override, and returns the schedule id.
    pub async fn create_cron_with_options(
        &self,
        name: &str,
        schedule: &str,
        task: &str,
        opts: CreateCronOptions,
    ) -> Result<String, FacadeError> {
        // Named fields, not the positional constructor: name/cron_expr are
        // both strings and were once swapped silently.
        let req = models::CreateCronRequest {
            config: opts.config.map(|c| Some(Box::new(c))),
            cron_expr: schedule.to_owned(),
            name: name.to_owned(),
            payload: opts.payload,
            queue: opts.queue.map(Some),
            task_name: task.to_owned(),
        };
        let resp = cron_api::create_cron(&self.config, req)
            .await
            .map_err(api_err("create_cron"))?;
        Ok(resp.cron_id)
    }

    /// Lists cron schedules.
    pub async fn list_crons(&self, opts: &ListOptions) -> Result<models::ListCronsResponse, FacadeError> {
        list_call!(cron_api::list_crons, &self.config, opts)
            .await
            .map_err(api_err("list_crons"))
    }

    /// Fetches a cron schedule.
    pub async fn get_cron(&self, id: &str) -> Result<models::CronSchedule, FacadeError> {
        cron_api::get_cron(&self.config, id)
            .await
            .map_err(api_err("get_cron"))
    }

    /// Deletes a schedule; already-enqueued jobs are unaffected.
    pub async fn delete_cron(&self, id: &str) -> Result<(), FacadeError> {
        cron_api::delete_cron(&self.config, id)
            .await
            .map_err(api_err("delete_cron"))
    }

    /// Stops firings until [`QueueFlow::resume_cron`].
    pub async fn pause_cron(&self, id: &str) -> Result<(), FacadeError> {
        cron_api::pause_cron(&self.config, id)
            .await
            .map_err(api_err("pause_cron"))
    }

    /// Resumes firings at the next future occurrence (missed runs are
    /// skipped).
    pub async fn resume_cron(&self, id: &str) -> Result<(), FacadeError> {
        cron_api::resume_cron(&self.config, id)
            .await
            .map_err(api_err("resume_cron"))
    }
}

// ---------------------------------------------------------------------------
// Dead letters
// ---------------------------------------------------------------------------

impl QueueFlow {
    /// Lists terminally-failed jobs.
    pub async fn list_dead_letters(
        &self,
        opts: &ListOptions,
    ) -> Result<models::ListDeadLettersResponse, FacadeError> {
        list_call!(dlq_api::list_dead_letters, &self.config, opts)
            .await
            .map_err(api_err("list_dead_letters"))
    }

    /// Fetches one dead-letter entry.
    pub async fn get_dead_letter(&self, id: i64) -> Result<models::DeadLetter, FacadeError> {
        dlq_api::get_dead_letter(&self.config, id)
            .await
            .map_err(api_err("get_dead_letter"))
    }

    /// Re-runs a dead-lettered job as a fresh job and returns the new job
    /// id. Each entry replays at most once (a second replay is a 409).
    pub async fn replay_dead_letter(&self, id: i64) -> Result<String, FacadeError> {
        dlq_api::replay_dead_letter(&self.config, id)
            .await
            .map(|r| r.job_id)
            .map_err(api_err("replay_dead_letter"))
    }
}

// ---------------------------------------------------------------------------
// System and health
// ---------------------------------------------------------------------------

impl QueueFlow {
    /// The engine's counters.
    pub async fn stats(&self) -> Result<models::StatsSnapshot, FacadeError> {
        system_api::get_stats(&self.config)
            .await
            .map_err(api_err("stats"))
    }

    /// Live backlog per queue (pending, scheduled, running and the oldest
    /// pending age), one entry per queue with non-terminal jobs.
    pub async fn queues(&self) -> Result<Vec<models::QueueStats>, FacadeError> {
        system_api::list_queues(&self.config)
            .await
            .map(|r| r.queues)
            .map_err(api_err("queues"))
    }

    /// Names of the task handlers registered on the server.
    pub async fn tasks(&self) -> Result<Vec<String>, FacadeError> {
        system_api::list_tasks(&self.config)
            .await
            .map(|r| r.tasks)
            .map_err(api_err("tasks"))
    }

    /// Liveness probe (`GET /health`).
    pub async fn health(&self) -> Result<models::HealthStatus, FacadeError> {
        health_api::get_health(&self.config)
            .await
            .map_err(api_err("health"))
    }

    /// Readiness probe (`GET /ready`).
    pub async fn ready(&self) -> Result<models::ReadyStatus, FacadeError> {
        health_api::get_ready(&self.config)
            .await
            .map_err(api_err("ready"))
    }
}

// ---------------------------------------------------------------------------
// Worker protocol (worker token)
// ---------------------------------------------------------------------------

/// Tunes [`QueueFlow::lease_jobs`]. `None` takes the server defaults
/// (`max_jobs` 1, `lease_secs` 30, `wait_secs` 0).
#[derive(Clone, Copy, Debug, Default)]
pub struct LeaseOptions {
    /// Jobs to lease in one call (1..=100).
    pub max_jobs: Option<i32>,
    /// Lease duration (1..=3600); heartbeat to extend.
    pub lease_secs: Option<i32>,
    /// Long-poll wait when the queue is empty (0..=30).
    pub wait_secs: Option<i32>,
}

impl QueueFlow {
    /// Leases up to `opts.max_jobs` jobs from `queue`, long-polling up to
    /// `opts.wait_secs` when it is empty. The call is bounded by `wait_secs`
    /// plus 35 seconds rather than the regular timeout.
    pub async fn lease_jobs(&self, queue: &str, opts: LeaseOptions) -> Result<Vec<models::LeasedJob>, FacadeError> {
        let wait = Duration::from_secs(opts.wait_secs.unwrap_or(0).max(0) as u64);
        let req = models::LeaseJobsRequest {
            lease_secs: opts.lease_secs.map(Some),
            max_jobs: opts.max_jobs.map(Some),
            wait_secs: opts.wait_secs.map(Some),
        };
        bounded("lease_jobs", wait + LEASE_TIMEOUT_SLACK, async {
            worker_api::lease_jobs(&self.worker_config, queue, req)
                .await
                .map(|r| r.jobs)
                .map_err(api_err("lease_jobs"))
        })
        .await
    }

    /// Extends a lease by `extend_secs` and returns the job's current status:
    /// `Running` means extended; anything else means the server owns the
    /// outcome and the handler should stop. A 409 means the lease was
    /// already lost.
    pub async fn heartbeat_job(&self, lease: &models::LeasedJob, extend_secs: i32) -> Result<models::JobStatus, FacadeError> {
        let req = models::HeartbeatRequest::new(extend_secs, lease.lease_token.clone());
        bounded("heartbeat_job", self.timeout, async {
            worker_api::heartbeat_job(&self.worker_config, &lease.job.id, req)
                .await
                .map(|r| r.status)
                .map_err(api_err("heartbeat_job"))
        })
        .await
    }

    /// Reports success with `result`. Replaying against a finished job is a
    /// server-side no-op.
    pub async fn complete_job(&self, lease: &models::LeasedJob, result: Json) -> Result<(), FacadeError> {
        let mut req = models::CompleteJobRequest::new(lease.lease_token.clone());
        req.result = Some(result);
        bounded("complete_job", self.timeout, async {
            worker_api::complete_job(&self.worker_config, &lease.job.id, req)
                .await
                .map_err(api_err("complete_job"))
        })
        .await
    }

    /// Reports failure; the server applies the job's retry policy, or
    /// dead-letters it immediately when `retryable` is false.
    pub async fn fail_job(&self, lease: &models::LeasedJob, message: &str, retryable: bool) -> Result<(), FacadeError> {
        let mut req = models::FailJobRequest::new(message.to_owned(), lease.lease_token.clone());
        req.retryable = Some(retryable);
        bounded("fail_job", self.timeout, async {
            worker_api::fail_job(&self.worker_config, &lease.job.id, req)
                .await
                .map_err(api_err("fail_job"))
        })
        .await
    }
}

// ---------------------------------------------------------------------------
// Worker runtime
// ---------------------------------------------------------------------------

/// A clonable, one-shot cancellation signal. Used to stop [`QueueFlow::run_worker`]
/// (via [`WorkerOptions::stop`]) and, per job, to tell a handler its lease
/// was lost or the drain timeout passed ([`JobContext::cancel`]).
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<CancelInner>);

#[derive(Debug, Default)]
struct CancelInner {
    flag: AtomicBool,
    notify: Notify,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fires the signal. Idempotent.
    pub fn cancel(&self) {
        self.0.flag.store(true, Ordering::SeqCst);
        self.0.notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.flag.load(Ordering::SeqCst)
    }

    /// Resolves once `cancel` has been called (immediately if it already
    /// was).
    pub async fn cancelled(&self) {
        // `Notified` receives `notify_waiters` wakeups from the moment it is
        // created, so checking the flag after creating it closes the race.
        let notified = self.0.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

/// Why a handler failed. A plain `Retryable` error leaves the job to the
/// server's retry policy; `NonRetryable` dead-letters it immediately.
///
/// Any `std::error::Error` converts into `Retryable` with `?`. This type
/// deliberately does not implement `std::error::Error` itself, which is what
/// makes that blanket conversion possible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandlerError {
    Retryable(String),
    NonRetryable(String),
}

impl HandlerError {
    pub fn retryable(message: impl Into<String>) -> Self {
        HandlerError::Retryable(message.into())
    }

    /// A permanent failure (e.g. invalid input): the job skips its remaining
    /// retries and dead-letters immediately.
    pub fn non_retryable(message: impl Into<String>) -> Self {
        HandlerError::NonRetryable(message.into())
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, HandlerError::Retryable(_))
    }

    pub fn message(&self) -> &str {
        match self {
            HandlerError::Retryable(m) | HandlerError::NonRetryable(m) => m,
        }
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl<E: error::Error> From<E> for HandlerError {
    fn from(e: E) -> Self {
        HandlerError::Retryable(e.to_string())
    }
}

/// What a handler returns: the job's result object, or why it failed.
pub type HandlerResult = Result<Json, HandlerError>;

/// Handed to a handler for one leased job.
#[derive(Clone, Debug)]
pub struct JobContext {
    pub job: models::Job,
    /// Fires when the job's lease is lost (cancelled mid-run, or reclaimed
    /// after expiry; the server then owns the outcome and nothing is
    /// reported) or when the worker's drain timeout passes. The handler
    /// future is dropped at the same moment, so this matters for work the
    /// handler spawned elsewhere.
    pub cancel: CancelToken,
}

type BoxedHandlerFuture = Pin<Box<dyn Future<Output = HandlerResult> + Send>>;
type HandlerFn = Arc<dyn Fn(JobContext) -> BoxedHandlerFuture + Send + Sync>;

/// Task handlers by task name, for [`QueueFlow::run_worker`].
///
/// ```no_run
/// use queueflow_sdk::{HandlerError, Handlers, Json};
///
/// let handlers = Handlers::new()
///     .register("send-email", |ctx| async move {
///         let to = ctx.job.payload.and_then(|p| p.get("to").cloned());
///         if to.is_none() {
///             return Err(HandlerError::non_retryable("payload has no `to`"));
///         }
///         Ok(Json::new())
///     });
/// ```
#[derive(Clone, Default)]
pub struct Handlers {
    map: HashMap<String, HandlerFn>,
}

impl Handlers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an async handler for `task`. Delivery is at-least-once, so
    /// handlers must be idempotent.
    pub fn register<F, Fut>(mut self, task: &str, f: F) -> Self
    where
        F: Fn(JobContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static,
    {
        self.map
            .insert(task.to_owned(), Arc::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    /// The registered task names.
    pub fn task_names(&self) -> Vec<&str> {
        self.map.keys().map(String::as_str).collect()
    }
}

impl fmt::Debug for Handlers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handlers")
            .field("tasks", &self.task_names())
            .finish()
    }
}

/// Callback for lease failures, see [`WorkerOptions::on_error`].
pub type LeaseErrorHook = Arc<dyn Fn(&FacadeError) + Send + Sync>;

/// Tunes [`QueueFlow::run_worker`].
#[derive(Clone)]
pub struct WorkerOptions {
    /// Jobs in flight at once (default 1). Each lease call asks for as many
    /// jobs as there are free slots.
    pub concurrency: usize,
    /// Lease duration (default 30); each job is heartbeated at half this
    /// interval while its handler runs.
    pub lease_secs: i32,
    /// Server-side long-poll when the queue is empty (default 20, max 30).
    pub wait_secs: i32,
    /// How long `run_worker` waits for in-flight handlers after `stop` fires
    /// (default 30s). Past it they are cancelled and dropped; unreported
    /// leases expire and the server redelivers.
    pub drain_timeout: Duration,
    /// Cancel this token to stop the worker. Keep a clone.
    pub stop: CancelToken,
    /// Called with every lease failure instead of the default throttled
    /// `eprintln!` (first failure, then every 30th).
    pub on_error: Option<LeaseErrorHook>,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        WorkerOptions {
            concurrency: 1,
            lease_secs: 30,
            wait_secs: 20,
            drain_timeout: Duration::from_secs(30),
            stop: CancelToken::new(),
            on_error: None,
        }
    }
}

impl fmt::Debug for WorkerOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerOptions")
            .field("concurrency", &self.concurrency)
            .field("lease_secs", &self.lease_secs)
            .field("wait_secs", &self.wait_secs)
            .field("drain_timeout", &self.drain_timeout)
            .field("stopped", &self.stop.is_cancelled())
            .field("on_error", &self.on_error.is_some())
            .finish()
    }
}

/// State shared by the lease loop and the per-job tasks it spawns.
struct WorkerShared {
    qf: QueueFlow,
    handlers: Handlers,
    lease_secs: i32,
    /// Fires when the drain timeout passes: in-flight handlers are dropped.
    drain: CancelToken,
}

impl QueueFlow {
    /// The worker loop: lease up to `opts.concurrency` jobs from `queue`,
    /// dispatch each to `handlers` by task name on its own task, heartbeat
    /// while the handler runs, and report the outcome. Jobs whose task has
    /// no handler are failed non-retryably.
    ///
    /// Returns `Ok(())` once `opts.stop` is cancelled: the in-flight lease
    /// long-poll is abandoned, then in-flight handlers get
    /// `opts.drain_timeout` to finish and report. Returns the
    /// `FacadeError::Http` when the lease call is rejected with 401 or 403,
    /// since a wrong or missing worker token cannot heal by retrying. Other
    /// lease errors back off one second and retry.
    ///
    /// The future is `Send`, so it can be `tokio::spawn`ed with a cloned
    /// client:
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), queueflow_sdk::FacadeError> {
    /// use queueflow_sdk::{Handlers, Json, QueueFlow, QueueFlowOptions, WorkerOptions};
    ///
    /// let qf = QueueFlow::with_options(
    ///     QueueFlowOptions::new("http://localhost:8000", "dev").worker_token("worker-secret"),
    /// )?;
    /// let handlers = Handlers::new().register("echo", |ctx| async move {
    ///     Ok(ctx.job.payload.unwrap_or_default())
    /// });
    /// let opts = WorkerOptions::default();
    /// let stop = opts.stop.clone();
    /// let worker = tokio::spawn({
    ///     let qf = qf.clone();
    ///     async move { qf.run_worker("default", handlers, opts).await }
    /// });
    /// // ... later, on SIGTERM:
    /// stop.cancel();
    /// worker.await.expect("worker task panicked")?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run_worker(&self, queue: &str, handlers: Handlers, opts: WorkerOptions) -> Result<(), FacadeError> {
        let concurrency = opts.concurrency.max(1);
        let lease_secs = opts.lease_secs.max(1);
        let wait_secs = opts.wait_secs.clamp(0, 30);
        let stop = opts.stop.clone();
        let shared = Arc::new(WorkerShared {
            qf: self.clone(),
            handlers,
            lease_secs,
            drain: CancelToken::new(),
        });
        let mut set: JoinSet<()> = JoinSet::new();
        let mut failures: u32 = 0;
        let mut fatal: Option<FacadeError> = None;

        loop {
            if stop.is_cancelled() {
                break;
            }
            // All slots busy: wait for one to free up before leasing again.
            if set.len() >= concurrency {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    joined = set.join_next() => {
                        if let Some(joined) = joined {
                            log_join(joined);
                        }
                        continue;
                    }
                }
            }
            let free = (concurrency - set.len()) as i32;
            let lease_opts = LeaseOptions {
                max_jobs: Some(free),
                lease_secs: Some(lease_secs),
                wait_secs: Some(wait_secs),
            };
            let leased = tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                r = self.lease_jobs(queue, lease_opts) => r,
            };
            match leased {
                Ok(jobs) => {
                    failures = 0;
                    for lease in jobs {
                        let s = shared.clone();
                        set.spawn(async move { s.process(lease).await });
                    }
                }
                Err(e) => {
                    if matches!(e.status(), Some(401) | Some(403)) {
                        fatal = Some(e);
                        break;
                    }
                    failures += 1;
                    match &opts.on_error {
                        Some(cb) => cb(&e),
                        None if failures == 1 || failures.is_multiple_of(30) => eprintln!(
                            "[queueflow] worker lease failed ({} consecutive): {}",
                            failures, e
                        ),
                        None => {}
                    }
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }

        // Drain: in-flight handlers finish and report, up to the timeout.
        let drain = async {
            while let Some(joined) = set.join_next().await {
                log_join(joined);
            }
        };
        if tokio::time::timeout(opts.drain_timeout, drain).await.is_err() {
            eprintln!(
                "[queueflow] worker drain timeout ({:?}) passed with {} job(s) in flight; their leases will expire and the server redelivers",
                opts.drain_timeout,
                set.len()
            );
            // Firing the drain token lets each job task cancel its own
            // JobContext token and drop its handler; the bounded join gives
            // them a poll to do so before the remaining tasks are aborted.
            shared.drain.cancel();
            let settle = async {
                while let Some(joined) = set.join_next().await {
                    log_join(joined);
                }
            };
            let _ = tokio::time::timeout(Duration::from_secs(1), settle).await;
            set.shutdown().await;
        }
        match fatal {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

fn log_join(joined: Result<(), tokio::task::JoinError>) {
    if let Err(e) = joined {
        // A panicking handler kills only its own task; the job's lease
        // expires and the server redelivers per the retry policy.
        eprintln!("[queueflow] job task panicked; its lease will expire and redeliver: {}", e);
    }
}

impl WorkerShared {
    /// Runs one leased job: dispatch to its handler under a heartbeat, then
    /// report the outcome. Reporting errors are logged, not retried: the
    /// lease expires and the server redelivers (at-least-once).
    async fn process(&self, lease: models::LeasedJob) {
        let job: models::Job = (*lease.job).clone();
        let handler = match self.handlers.map.get(&job.task_name) {
            Some(h) => h.clone(),
            None => {
                let msg = format!("no handler for task {:?}", job.task_name);
                if let Err(e) = self.qf.fail_job(&lease, &msg, false).await {
                    eprintln!("[queueflow] job {}: failed to report missing handler: {}", job.id, e);
                }
                return;
            }
        };

        // The heartbeat doubles as the lease-loss channel: a non-running
        // status or a 409 cancels the job token, which drops the handler;
        // nothing is reported since the server already owns the outcome.
        let token = CancelToken::new();
        let heartbeat = tokio::spawn({
            let qf = self.qf.clone();
            let lease = lease.clone();
            let token = token.clone();
            let lease_secs = self.lease_secs;
            async move {
                let every = Duration::from_secs((lease_secs / 2).max(1) as u64);
                let mut ticker =
                    tokio::time::interval_at(tokio::time::Instant::now() + every, every);
                loop {
                    ticker.tick().await;
                    match qf.heartbeat_job(&lease, lease_secs).await {
                        Ok(models::JobStatus::Running) => {}
                        Ok(_) => {
                            token.cancel();
                            return;
                        }
                        Err(e) if e.status() == Some(409) => {
                            token.cancel();
                            return;
                        }
                        Err(e) => eprintln!(
                            "[queueflow] job {}: heartbeat failed: {}",
                            lease.job.id, e
                        ),
                    }
                }
            }
        });

        let ctx = JobContext {
            job: job.clone(),
            cancel: token.clone(),
        };
        let mut fut = handler(ctx);
        let outcome = tokio::select! {
            biased;
            _ = token.cancelled() => None,
            _ = self.drain.cancelled() => {
                token.cancel();
                None
            }
            out = &mut fut => Some(out),
        };
        heartbeat.abort();
        drop(fut);

        let Some(outcome) = outcome else {
            return; // lease lost or drain timed out: the server owns the outcome
        };

        // Handler outcome and outcome reporting are separate concerns: a
        // reporting error is logged, never re-reported as a job failure.
        let report = match outcome {
            Ok(result) => self.qf.complete_job(&lease, result).await,
            Err(err) => {
                self.qf
                    .fail_job(&lease, err.message(), err.is_retryable())
                    .await
            }
        };
        if let Err(e) = report {
            eprintln!(
                "[queueflow] job {}: failed to report outcome; the lease will expire and the server redelivers: {}",
                job.id, e
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Workflow builder
// ---------------------------------------------------------------------------

/// Per-step options for [`WorkflowBuilder::step_with_options`].
#[derive(Clone, Debug, Default)]
pub struct StepOptions {
    /// Names of the steps that must complete before this one runs.
    pub after: Vec<String>,
    /// Passed to the task handler.
    pub payload: Option<Json>,
    /// Execution config override (retries, timeout, ...).
    pub config: Option<models::JobConfig>,
    /// `Halt` (default), `Skip` or `Continue`.
    pub on_failure: Option<models::OnFailure>,
    pub on_success: Option<models::OnSuccess>,
    pub metadata: Option<Json>,
}

/// A typed builder for a workflow DAG with local validation.
#[derive(Clone, Debug)]
pub struct WorkflowBuilder {
    name: String,
    steps: Vec<models::WorkflowStep>,
    context: Option<Json>,
    metadata: Option<Json>,
}

impl WorkflowBuilder {
    /// Starts a workflow definition named `name`.
    pub fn new(name: &str) -> Self {
        WorkflowBuilder {
            name: name.to_owned(),
            steps: Vec::new(),
            context: None,
            metadata: None,
        }
    }

    /// Adds a step running `task`, gated on the named `after` steps.
    pub fn step(self, name: &str, task: &str, after: &[&str]) -> Self {
        self.step_with_options(
            name,
            task,
            StepOptions {
                after: after.iter().map(|d| d.to_string()).collect(),
                ..StepOptions::default()
            },
        )
    }

    /// Adds a step with a payload, failure policy, config override and
    /// dependencies.
    pub fn step_with_options(mut self, name: &str, task: &str, opts: StepOptions) -> Self {
        let mut step = models::WorkflowStep::new(name.to_owned(), task.to_owned());
        if !opts.after.is_empty() {
            step.depends_on = Some(opts.after);
        }
        step.payload = opts.payload;
        step.config = opts.config.map(|c| Some(Box::new(c)));
        step.on_failure = opts.on_failure;
        step.on_success = opts.on_success;
        step.metadata = opts.metadata;
        self.steps.push(step);
        self
    }

    /// Seeds the shared workflow context (merged across calls).
    pub fn context(mut self, context: Json) -> Self {
        self.context.get_or_insert_with(Json::new).extend(context);
        self
    }

    /// Attaches metadata to the workflow (merged across calls).
    pub fn metadata(mut self, metadata: Json) -> Self {
        self.metadata.get_or_insert_with(Json::new).extend(metadata);
        self
    }

    /// Validates the DAG (duplicate names, dangling deps, cycles) and returns
    /// the request body. A structurally invalid DAG returns an error before
    /// any network round-trip.
    pub fn build(&self) -> Result<models::CreateWorkflowRequest, FacadeError> {
        if self.name.is_empty() {
            return Err(FacadeError::InvalidWorkflow(
                "workflow name is required".to_owned(),
            ));
        }
        if self.steps.is_empty() {
            return Err(FacadeError::InvalidWorkflow(format!(
                "workflow {:?} has no steps",
                self.name
            )));
        }
        let mut names = HashSet::new();
        for step in &self.steps {
            if !names.insert(step.name.as_str()) {
                return Err(FacadeError::InvalidWorkflow(format!(
                    "duplicate step name {:?}",
                    step.name
                )));
            }
        }
        for step in &self.steps {
            for dep in step.depends_on.iter().flatten() {
                if !names.contains(dep.as_str()) {
                    return Err(FacadeError::InvalidWorkflow(format!(
                        "step {:?} depends on unknown step {:?}",
                        step.name, dep
                    )));
                }
            }
        }
        assert_acyclic(&self.steps)?;
        let mut req = models::CreateWorkflowRequest::new(self.name.clone(), self.steps.clone());
        req.context = self.context.clone();
        req.metadata = self.metadata.clone();
        Ok(req)
    }
}

fn assert_acyclic(steps: &[models::WorkflowStep]) -> Result<(), FacadeError> {
    fn visit<'a>(
        node: &'a str,
        deps_of: &HashMap<&'a str, &'a [String]>,
        visiting: &mut HashSet<&'a str>,
        done: &mut HashSet<&'a str>,
    ) -> Result<(), FacadeError> {
        if done.contains(node) {
            return Ok(());
        }
        if !visiting.insert(node) {
            return Err(FacadeError::InvalidWorkflow(format!(
                "dependency cycle at {:?}",
                node
            )));
        }
        if let Some(deps) = deps_of.get(node) {
            for dep in deps.iter() {
                visit(dep, deps_of, visiting, done)?;
            }
        }
        visiting.remove(node);
        done.insert(node);
        Ok(())
    }

    let deps_of: HashMap<&str, &[String]> = steps
        .iter()
        .map(|s| {
            (
                s.name.as_str(),
                s.depends_on.as_deref().unwrap_or(&[]),
            )
        })
        .collect();
    let mut visiting = HashSet::new();
    let mut done = HashSet::new();
    for step in steps {
        visit(step.name.as_str(), &deps_of, &mut visiting, &mut done)?;
    }
    Ok(())
}
