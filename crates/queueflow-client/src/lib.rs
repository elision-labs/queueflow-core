//! # queueflow-client
//!
//! Hand-written Rust client for the QueueFlow REST API. It reuses the
//! engine's own serde domain types ([`Job`], [`Workflow`], [`LeasedJob`],
//! [`CreateWorkflowRequest`]), so the wire format cannot drift from the
//! server, and stays free of any server-side dependencies (axum, sqlx).
//!
//! Two roles in one crate:
//! * **Producer / inspector** — enqueue jobs (optionally idempotent), create
//!   workflows, list, cancel, await completion.
//! * **Worker** — the [`worker`] module implements the remote worker protocol
//!   (lease, heartbeat, complete, fail), letting job handlers run in a
//!   separate process from the server.

pub mod worker;

use std::time::Duration;

use serde::{Deserialize, Serialize};

// Re-export the domain types callers need (also imports them for this module),
// so depending on queueflow-core directly is optional.
pub use queueflow_core::{
    BackoffStrategy, CreateCronRequest, CreateWorkflowRequest, CronSchedule, DeadLetter,
    HandlerError, Job, JobConfig, JobStatus, LeasedJob, Map, OnFailure, QueueStats, StatsSnapshot,
    Workflow, WorkflowStatus, WorkflowStep,
};

/// Errors returned by [`Client`] calls.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Transport-level failure (connection refused, timeout, TLS, ...).
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    /// The server answered with a non-success status.
    #[error("api error ({status}): {message}")]
    Api { status: u16, message: String },
    /// A response body could not be decoded.
    #[error("decode error: {0}")]
    Decode(#[from] serde_json::Error),
    /// Waiting for a job to finish exceeded the caller's deadline.
    #[error("timed out waiting for job {0}")]
    WaitTimeout(String),
}

impl Error {
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Api { status, .. } => Some(*status),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: String,
}

/// Optional settings for [`Client::create_job`].
#[derive(Clone, Debug, Default)]
pub struct CreateJobOptions {
    pub queue: Option<String>,
    pub priority: Option<i32>,
    pub max_retries: Option<u32>,
    pub timeout_secs: Option<u64>,
    /// How retry delays grow between attempts (server default: exponential).
    pub retry_backoff: Option<BackoffStrategy>,
    /// Base retry delay, in seconds.
    pub retry_delay_secs: Option<u64>,
    /// Upper bound on any computed retry delay, in seconds.
    pub retry_max_delay_secs: Option<u64>,
    /// Retry-delay jitter in `0.0..=1.0` (e.g. `0.1` = +/-10%).
    pub jitter_factor: Option<f64>,
    /// Makes the create idempotent per tenant (sent as `Idempotency-Key`).
    pub idempotency_key: Option<String>,
    /// Don't run before this instant. The job is created immediately but
    /// invisible to workers until then.
    pub run_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Query for the list endpoints.
#[derive(Clone, Debug, Default)]
pub struct ListQuery {
    pub status: Option<String>,
    pub queue: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Ask the server for the exact total (extra count query server-side).
    pub include_total: bool,
    /// Opaque keyset cursor from a previous page's `next_cursor`. When set,
    /// `offset` is ignored server-side; cheaper than deep OFFSET paging.
    pub cursor: Option<String>,
    /// Only rows created at or after this instant (inclusive).
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    /// Only rows created strictly before this instant (exclusive).
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    /// Only jobs or dead letters for this task (since 0.5.0).
    pub task_name: Option<String>,
    /// Dead letters only (since 0.5.0): `Some(false)` lists entries not yet
    /// replayed, `Some(true)` the replayed ones.
    pub replayed: Option<bool>,
}

/// Which jobs [`Client::cancel_jobs`] applies to. Every field is optional;
/// the default cancels the oldest non-terminal jobs of the tenant up to the
/// server's default limit (100).
#[derive(Clone, Debug, Default, Serialize)]
pub struct CancelJobsFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_name: Option<String>,
    /// `pending`, `retrying` or `running`; all three when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    /// At most this many, oldest first (capped by the server's `--max-batch`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// Which dead letters [`Client::replay_dead_letters`] applies to; only
/// unreplayed entries are ever considered.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ReplayDeadLettersFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// One dead letter's outcome in [`Client::replay_dead_letters`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplayedDeadLetter {
    pub dead_letter_id: i64,
    /// The fresh job, when the replay succeeded.
    #[serde(default)]
    pub job_id: Option<String>,
    /// Why it did not (for example the original job was purged).
    #[serde(default)]
    pub error: Option<String>,
}

/// The queue's state after [`Client::pause_queue`] or
/// [`Client::resume_queue`]: a paused queue is drained once `running` is 0.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueueControl {
    pub queue: String,
    pub paused: bool,
    pub running: u64,
    pub pending: u64,
}

/// The token's identity as the server resolved it ([`Client::whoami`]).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WhoAmI {
    pub tenant: String,
    /// `read_write` or `read_only`.
    pub access: String,
}

/// One page of jobs or workflows.
#[derive(Clone, Debug, Deserialize)]
pub struct JobsPage {
    pub jobs: Vec<Job>,
    pub has_more: bool,
    #[serde(default)]
    pub total: Option<i64>,
    /// Pass back as [`ListQuery::cursor`] to fetch the next page.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkflowsPage {
    pub workflows: Vec<Workflow>,
    pub has_more: bool,
    #[serde(default)]
    pub total: Option<i64>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// One page of dead letters.
#[derive(Clone, Debug, Deserialize)]
pub struct DeadLettersPage {
    pub dead_letters: Vec<DeadLetter>,
    pub has_more: bool,
    #[serde(default)]
    pub total: Option<i64>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// One page of cron schedules.
#[derive(Clone, Debug, Deserialize)]
pub struct CronsPage {
    pub crons: Vec<CronSchedule>,
    pub has_more: bool,
    #[serde(default)]
    pub total: Option<i64>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Serialize)]
struct JobConfigBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_retries: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_backoff: Option<BackoffStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_delay_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_max_delay_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    jitter_factor: Option<f64>,
}

#[derive(Deserialize)]
struct CreateJobResponse {
    job_id: String,
}

#[derive(Deserialize)]
struct CreateBatchJobsResponse {
    job_ids: Vec<String>,
}

#[derive(Deserialize)]
struct CreateWorkflowResponse {
    workflow_id: String,
}

#[derive(Deserialize)]
struct ReplayDeadLetterResponse {
    job_id: String,
}

#[derive(Deserialize)]
struct CreateCronResponse {
    cron_id: String,
}

#[derive(Deserialize)]
struct WorkflowDiagramResponse {
    diagram: String,
}

#[derive(Deserialize)]
struct TasksResponse {
    tasks: Vec<String>,
}

/// Live state of one workflow step (`GET /workflows/{id}/steps`).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkflowStepState {
    pub name: String,
    pub status: queueflow_core::StepStatus,
    #[serde(default)]
    pub job_id: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub child_workflow_id: Option<String>,
    #[serde(default)]
    pub fan_out: Option<FanOutProgress>,
}

/// Counts over a fan-out step's children.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct FanOutProgress {
    pub total: u64,
    pub completed: u64,
    pub failed: u64,
}

#[derive(Deserialize)]
struct WorkflowStepStatesResponse {
    steps: Vec<WorkflowStepState>,
}

#[derive(Deserialize)]
struct QueuesResponse {
    queues: Vec<QueueStats>,
}

#[derive(Deserialize)]
struct CancelJobsResponse {
    cancelled: u64,
}

#[derive(Deserialize)]
struct ReplayDeadLettersResponse {
    results: Vec<ReplayedDeadLetter>,
}

#[derive(Deserialize)]
struct LeaseJobsResponse {
    jobs: Vec<LeasedJob>,
}

#[derive(Deserialize)]
struct HeartbeatResponse {
    status: JobStatus,
}

/// Response of the liveness probe (`GET /health`).
#[derive(Clone, Debug, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub version: String,
}

/// Response of the readiness probe (`GET /ready`).
#[derive(Clone, Debug, Deserialize)]
pub struct ReadyStatus {
    pub status: String,
}

/// A live stream of a job's status transitions, from the server-sent-events
/// endpoint (`GET /api/v1/jobs/{id}/events`). Each item is the full job at a
/// status change; the stream ends after the job reaches a terminal state.
/// Obtained from [`Client::watch_job`].
pub struct JobEvents {
    resp: reqwest::Response,
    buf: String,
}

impl JobEvents {
    /// The job at its next status transition, or `None` when the server
    /// closes the stream (after a terminal status, or its idle cap).
    pub async fn next(&mut self) -> Result<Option<Job>, Error> {
        loop {
            // A complete SSE event is terminated by a blank line.
            if let Some(pos) = self.buf.find("\n\n") {
                let event: String = self.buf.drain(..pos + 2).collect();
                let mut name = "";
                let mut data = String::new();
                for line in event.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        name = v.trim();
                    } else if let Some(v) = line.strip_prefix("data:") {
                        // Per the SSE spec, multiple data lines join with a
                        // newline.
                        if !data.is_empty() {
                            data.push('\n');
                        }
                        data.push_str(v.trim_start());
                    }
                }
                if name == "status" && !data.is_empty() {
                    return Ok(Some(serde_json::from_str(&data)?));
                }
                continue; // keep-alive comment or unknown event: skip
            }
            match self.resp.chunk().await? {
                Some(bytes) => self.buf.push_str(&String::from_utf8_lossy(&bytes)),
                None => return Ok(None),
            }
        }
    }
}

/// A QueueFlow API client. Cheap to clone (shares the connection pool).
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl Client {
    /// `base_url` like `http://localhost:8000` (no trailing slash needed);
    /// `token` is sent as a bearer token on every request.
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, self.url(path))
            .bearer_auth(&self.token)
    }

    /// Check the response status and decode the body, mapping API error bodies
    /// to [`Error::Api`].
    async fn decode<T: serde::de::DeserializeOwned>(resp: reqwest::Response) -> Result<T, Error> {
        let status = resp.status();
        if status.is_success() {
            Ok(resp.json::<T>().await?)
        } else {
            let message = resp
                .json::<ErrorBody>()
                .await
                .map(|b| b.error)
                .unwrap_or_else(|_| status.to_string());
            Err(Error::Api {
                status: status.as_u16(),
                message,
            })
        }
    }

    async fn expect_no_content(resp: reqwest::Response) -> Result<(), Error> {
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let message = resp
                .json::<ErrorBody>()
                .await
                .map(|b| b.error)
                .unwrap_or_else(|_| status.to_string());
            Err(Error::Api {
                status: status.as_u16(),
                message,
            })
        }
    }

    // ---- Jobs ---------------------------------------------------------------

    /// Enqueue a job; returns its id.
    pub async fn create_job(
        &self,
        task_name: &str,
        payload: Map,
        opts: CreateJobOptions,
    ) -> Result<String, Error> {
        let mut body = serde_json::json!({
            "task_name": task_name,
            "payload": payload,
            "config": JobConfigBody {
                priority: opts.priority,
                max_retries: opts.max_retries,
                timeout: opts.timeout_secs,
                queue: opts.queue.as_deref(),
                retry_backoff: opts.retry_backoff,
                retry_delay_secs: opts.retry_delay_secs,
                retry_max_delay_secs: opts.retry_max_delay_secs,
                jitter_factor: opts.jitter_factor,
            },
        });
        if let Some(run_at) = &opts.run_at {
            body["run_at"] = serde_json::json!(run_at);
        }
        let mut req = self
            .request(reqwest::Method::POST, "/api/v1/jobs")
            .json(&body);
        if let Some(key) = &opts.idempotency_key {
            req = req.header("Idempotency-Key", key);
        }
        let resp: CreateJobResponse = Self::decode(req.send().await?).await?;
        Ok(resp.job_id)
    }

    /// Enqueue many jobs in one call; returns their ids.
    pub async fn create_batch_jobs(&self, jobs: Vec<(String, Map)>) -> Result<Vec<String>, Error> {
        let body = serde_json::json!({
            "jobs": jobs
                .into_iter()
                .map(|(task_name, payload)| serde_json::json!({
                    "task_name": task_name,
                    "payload": payload,
                }))
                .collect::<Vec<_>>(),
        });
        let resp: CreateBatchJobsResponse = Self::decode(
            self.request(reqwest::Method::POST, "/api/v1/jobs/batch")
                .json(&body)
                .send()
                .await?,
        )
        .await?;
        Ok(resp.job_ids)
    }

    pub async fn get_job(&self, id: &str) -> Result<Job, Error> {
        Self::decode(
            self.request(reqwest::Method::GET, &format!("/api/v1/jobs/{id}"))
                .send()
                .await?,
        )
        .await
    }

    pub async fn list_jobs(&self, q: &ListQuery) -> Result<JobsPage, Error> {
        let mut req = self.request(reqwest::Method::GET, "/api/v1/jobs");
        req = apply_list_query(req, q);
        Self::decode(req.send().await?).await
    }

    /// Cancel every non-terminal job matching `filter`, oldest first, up to
    /// its `limit`; returns how many were cancelled. Repeat until the count
    /// drops below the limit to drain a large set.
    pub async fn cancel_jobs(&self, filter: &CancelJobsFilter) -> Result<u64, Error> {
        let resp: CancelJobsResponse = Self::decode(
            self.request(reqwest::Method::POST, "/api/v1/jobs/cancel")
                .json(filter)
                .send()
                .await?,
        )
        .await?;
        Ok(resp.cancelled)
    }

    pub async fn cancel_job(&self, id: &str) -> Result<(), Error> {
        Self::expect_no_content(
            self.request(reqwest::Method::POST, &format!("/api/v1/jobs/{id}/cancel"))
                .send()
                .await?,
        )
        .await
    }

    // ---- Cron schedules -----------------------------------------------------

    /// Create a recurring enqueue (5-field crontab, UTC); returns the
    /// schedule id. A duplicate name for the tenant is a 409.
    pub async fn create_cron(&self, req: &CreateCronRequest) -> Result<String, Error> {
        let resp: CreateCronResponse = Self::decode(
            self.request(reqwest::Method::POST, "/api/v1/cron")
                .json(req)
                .send()
                .await?,
        )
        .await?;
        Ok(resp.cron_id)
    }

    pub async fn list_crons(&self, q: &ListQuery) -> Result<CronsPage, Error> {
        let mut req = self.request(reqwest::Method::GET, "/api/v1/cron");
        req = apply_list_query(req, q);
        Self::decode(req.send().await?).await
    }

    pub async fn get_cron(&self, id: &str) -> Result<CronSchedule, Error> {
        Self::decode(
            self.request(reqwest::Method::GET, &format!("/api/v1/cron/{id}"))
                .send()
                .await?,
        )
        .await
    }

    pub async fn delete_cron(&self, id: &str) -> Result<(), Error> {
        Self::expect_no_content(
            self.request(reqwest::Method::DELETE, &format!("/api/v1/cron/{id}"))
                .send()
                .await?,
        )
        .await
    }

    /// Stop firings until [`Client::resume_cron`].
    pub async fn pause_cron(&self, id: &str) -> Result<(), Error> {
        Self::expect_no_content(
            self.request(reqwest::Method::POST, &format!("/api/v1/cron/{id}/pause"))
                .send()
                .await?,
        )
        .await
    }

    /// Resume firings at the next future occurrence (missed runs are skipped).
    pub async fn resume_cron(&self, id: &str) -> Result<(), Error> {
        Self::expect_no_content(
            self.request(reqwest::Method::POST, &format!("/api/v1/cron/{id}/resume"))
                .send()
                .await?,
        )
        .await
    }

    // ---- Dead letters -------------------------------------------------------

    /// List dead-lettered jobs, newest first (`status` in the query is
    /// ignored; `queue`/paging apply).
    pub async fn list_dead_letters(&self, q: &ListQuery) -> Result<DeadLettersPage, Error> {
        let mut req = self.request(reqwest::Method::GET, "/api/v1/dlq");
        req = apply_list_query(req, q);
        Self::decode(req.send().await?).await
    }

    pub async fn get_dead_letter(&self, id: i64) -> Result<DeadLetter, Error> {
        Self::decode(
            self.request(reqwest::Method::GET, &format!("/api/v1/dlq/{id}"))
                .send()
                .await?,
        )
        .await
    }

    /// Replay a dead-lettered job as a fresh, detached job; returns the new
    /// job id. Each entry replays at most once (a second replay is a 409).
    pub async fn replay_dead_letter(&self, id: i64) -> Result<String, Error> {
        let resp: ReplayDeadLetterResponse = Self::decode(
            self.request(reqwest::Method::POST, &format!("/api/v1/dlq/{id}/replay"))
                .send()
                .await?,
        )
        .await?;
        Ok(resp.job_id)
    }

    /// Replay every unreplayed dead letter matching `filter`, oldest first;
    /// one outcome per entry considered.
    pub async fn replay_dead_letters(
        &self,
        filter: &ReplayDeadLettersFilter,
    ) -> Result<Vec<ReplayedDeadLetter>, Error> {
        let resp: ReplayDeadLettersResponse = Self::decode(
            self.request(reqwest::Method::POST, "/api/v1/dlq/replay")
                .json(filter)
                .send()
                .await?,
        )
        .await?;
        Ok(resp.results)
    }

    /// Stream the job's status transitions as they happen (Server-Sent
    /// Events). Lower-latency alternative to [`Client::wait_for_job`] when
    /// you want every transition, not just the terminal one:
    ///
    /// ```no_run
    /// # async fn run(client: queueflow_client::Client) -> Result<(), queueflow_client::Error> {
    /// let mut events = client.watch_job("some-job-id").await?;
    /// while let Some(job) = events.next().await? {
    ///     println!("{}", job.status.as_str());
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn watch_job(&self, id: &str) -> Result<JobEvents, Error> {
        let resp = self
            .request(reqwest::Method::GET, &format!("/api/v1/jobs/{id}/events"))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let message = resp
                .json::<ErrorBody>()
                .await
                .map(|b| b.error)
                .unwrap_or_else(|_| status.to_string());
            return Err(Error::Api {
                status: status.as_u16(),
                message,
            });
        }
        Ok(JobEvents {
            resp,
            buf: String::new(),
        })
    }

    /// Poll until the job reaches a terminal state, or `timeout` elapses.
    pub async fn wait_for_job(
        &self,
        id: &str,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<Job, Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let job = self.get_job(id).await?;
            if job.status.is_terminal() {
                return Ok(job);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::WaitTimeout(id.to_string()));
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    // ---- Workflows ------------------------------------------------------------

    pub async fn create_workflow(&self, req: &CreateWorkflowRequest) -> Result<String, Error> {
        let resp: CreateWorkflowResponse = Self::decode(
            self.request(reqwest::Method::POST, "/api/v1/workflows")
                .json(req)
                .send()
                .await?,
        )
        .await?;
        Ok(resp.workflow_id)
    }

    pub async fn get_workflow(&self, id: &str) -> Result<Workflow, Error> {
        Self::decode(
            self.request(reqwest::Method::GET, &format!("/api/v1/workflows/{id}"))
                .send()
                .await?,
        )
        .await
    }

    pub async fn list_workflows(&self, q: &ListQuery) -> Result<WorkflowsPage, Error> {
        let mut req = self.request(reqwest::Method::GET, "/api/v1/workflows");
        req = apply_list_query(req, q);
        Self::decode(req.send().await?).await
    }

    pub async fn cancel_workflow(&self, id: &str) -> Result<(), Error> {
        Self::expect_no_content(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/workflows/{id}/cancel"),
            )
            .send()
            .await?,
        )
        .await
    }

    /// Re-run a failed, partially failed or cancelled workflow from its
    /// unfinished steps (server 0.6.0). A running or completed workflow is
    /// a 409.
    pub async fn retry_workflow(&self, id: &str) -> Result<(), Error> {
        Self::expect_no_content(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/workflows/{id}/retry"),
            )
            .send()
            .await?,
        )
        .await
    }

    /// Live state of every step, in declaration order.
    pub async fn workflow_steps(&self, id: &str) -> Result<Vec<WorkflowStepState>, Error> {
        let resp: WorkflowStepStatesResponse = Self::decode(
            self.request(
                reqwest::Method::GET,
                &format!("/api/v1/workflows/{id}/steps"),
            )
            .send()
            .await?,
        )
        .await?;
        Ok(resp.steps)
    }

    /// The workflow's DAG as a Mermaid document.
    pub async fn workflow_diagram(&self, id: &str) -> Result<String, Error> {
        let resp: WorkflowDiagramResponse = Self::decode(
            self.request(
                reqwest::Method::GET,
                &format!("/api/v1/workflows/{id}/diagram"),
            )
            .send()
            .await?,
        )
        .await?;
        Ok(resp.diagram)
    }

    // ---- System ----------------------------------------------------------------

    /// Names of the task handlers registered in the server binary. Remote
    /// workers (this crate's [`worker`] module) do not appear here.
    pub async fn tasks(&self) -> Result<Vec<String>, Error> {
        let resp: TasksResponse = Self::decode(
            self.request(reqwest::Method::GET, "/api/v1/tasks")
                .send()
                .await?,
        )
        .await?;
        Ok(resp.tasks)
    }

    pub async fn stats(&self) -> Result<StatsSnapshot, Error> {
        Self::decode(
            self.request(reqwest::Method::GET, "/api/v1/stats")
                .send()
                .await?,
        )
        .await
    }

    /// Every queue of the token's tenant with its live backlog and `paused`
    /// flag (`GET /api/v1/queues`).
    pub async fn list_queues(&self) -> Result<Vec<QueueStats>, Error> {
        let resp: QueuesResponse = Self::decode(
            self.request(reqwest::Method::GET, "/api/v1/queues")
                .send()
                .await?,
        )
        .await?;
        Ok(resp.queues)
    }

    /// Stop claims of this tenant's jobs on `queue`; running jobs finish.
    /// Idempotent. Drain by polling until the returned `running` is 0.
    pub async fn pause_queue(&self, queue: &str) -> Result<QueueControl, Error> {
        Self::decode(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/queues/{queue}/pause"),
            )
            .send()
            .await?,
        )
        .await
    }

    /// Resume claims paused by [`Client::pause_queue`]. Idempotent.
    pub async fn resume_queue(&self, queue: &str) -> Result<QueueControl, Error> {
        Self::decode(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/queues/{queue}/resume"),
            )
            .send()
            .await?,
        )
        .await
    }

    /// The tenant and access level the server grants this token.
    pub async fn whoami(&self) -> Result<WhoAmI, Error> {
        Self::decode(
            self.request(reqwest::Method::GET, "/api/v1/whoami")
                .send()
                .await?,
        )
        .await
    }

    /// Liveness probe (`GET /health`; no auth required).
    pub async fn health(&self) -> Result<HealthStatus, Error> {
        Self::decode(self.http.get(self.url("/health")).send().await?).await
    }

    /// Readiness probe (`GET /ready`; no auth required).
    pub async fn ready(&self) -> Result<ReadyStatus, Error> {
        Self::decode(self.http.get(self.url("/ready")).send().await?).await
    }

    // ---- Remote worker protocol --------------------------------------------------

    /// Lease up to `max_jobs` jobs for `lease_secs`, waiting up to `wait_secs`
    /// when the queue is empty. Returns an empty vec when nothing arrived.
    pub async fn lease_jobs(
        &self,
        queue: &str,
        max_jobs: usize,
        lease_secs: u32,
        wait_secs: u32,
    ) -> Result<Vec<LeasedJob>, Error> {
        let body = serde_json::json!({
            "max_jobs": max_jobs,
            "lease_secs": lease_secs,
            "wait_secs": wait_secs,
        });
        let resp: LeaseJobsResponse = Self::decode(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/queues/{queue}/lease"),
            )
            .json(&body)
            .send()
            .await?,
        )
        .await?;
        Ok(resp.jobs)
    }

    /// Extend a lease (heartbeat) so a still-running job is not reaped.
    /// Returns the job's current status: [`JobStatus::Running`] means the
    /// lease was extended; anything else (e.g. cancelled mid-run) means it
    /// was not, and the worker should stop working on the job.
    pub async fn heartbeat_job(
        &self,
        lease: &LeasedJob,
        extend_secs: u32,
    ) -> Result<JobStatus, Error> {
        let body = serde_json::json!({
            "lease_token": lease.lease_token,
            "extend_secs": extend_secs,
        });
        let resp: HeartbeatResponse = Self::decode(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/jobs/{}/heartbeat", lease.job.id),
            )
            .json(&body)
            .send()
            .await?,
        )
        .await?;
        Ok(resp.status)
    }

    /// Report success for a leased job.
    pub async fn complete_job(&self, lease: &LeasedJob, result: Map) -> Result<(), Error> {
        let body = serde_json::json!({
            "lease_token": lease.lease_token,
            "result": result,
        });
        Self::expect_no_content(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/jobs/{}/complete", lease.job.id),
            )
            .json(&body)
            .send()
            .await?,
        )
        .await
    }

    /// Report failure for a leased job; the server applies the retry policy.
    pub async fn fail_job(
        &self,
        lease: &LeasedJob,
        error: &str,
        retryable: bool,
    ) -> Result<(), Error> {
        let body = serde_json::json!({
            "lease_token": lease.lease_token,
            "error": error,
            "retryable": retryable,
        });
        Self::expect_no_content(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/jobs/{}/fail", lease.job.id),
            )
            .json(&body)
            .send()
            .await?,
        )
        .await
    }
}

fn apply_list_query(req: reqwest::RequestBuilder, q: &ListQuery) -> reqwest::RequestBuilder {
    let mut params: Vec<(&str, String)> = Vec::new();
    if let Some(s) = &q.status {
        params.push(("status", s.clone()));
    }
    if let Some(qu) = &q.queue {
        params.push(("queue", qu.clone()));
    }
    if let Some(l) = q.limit {
        params.push(("limit", l.to_string()));
    }
    if let Some(o) = q.offset {
        params.push(("offset", o.to_string()));
    }
    if q.include_total {
        params.push(("include_total", "true".into()));
    }
    if let Some(c) = &q.cursor {
        params.push(("cursor", c.clone()));
    }
    if let Some(t) = &q.created_after {
        params.push(("created_after", t.to_rfc3339()));
    }
    if let Some(t) = &q.created_before {
        params.push(("created_before", t.to_rfc3339()));
    }
    if let Some(t) = &q.task_name {
        params.push(("task_name", t.clone()));
    }
    if let Some(r) = q.replayed {
        params.push(("replayed", r.to_string()));
    }
    req.query(&params)
}
