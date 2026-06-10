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
    BackoffStrategy, CreateWorkflowRequest, HandlerError, Job, JobConfig, JobStatus, LeasedJob,
    Map, OnFailure, StatsSnapshot, Workflow, WorkflowStatus, WorkflowStep,
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
    /// Makes the create idempotent per tenant (sent as `Idempotency-Key`).
    pub idempotency_key: Option<String>,
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
}

/// One page of jobs or workflows.
#[derive(Clone, Debug, Deserialize)]
pub struct JobsPage {
    pub jobs: Vec<Job>,
    pub has_more: bool,
    #[serde(default)]
    pub total: Option<i64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkflowsPage {
    pub workflows: Vec<Workflow>,
    pub has_more: bool,
    #[serde(default)]
    pub total: Option<i64>,
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
struct WorkflowDiagramResponse {
    diagram: String,
}

#[derive(Deserialize)]
struct TasksResponse {
    tasks: Vec<String>,
}

#[derive(Deserialize)]
struct LeaseJobsResponse {
    jobs: Vec<LeasedJob>,
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
        let body = serde_json::json!({
            "task_name": task_name,
            "payload": payload,
            "config": JobConfigBody {
                priority: opts.priority,
                max_retries: opts.max_retries,
                timeout: opts.timeout_secs,
                queue: opts.queue.as_deref(),
            },
        });
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

    pub async fn cancel_job(&self, id: &str) -> Result<(), Error> {
        Self::expect_no_content(
            self.request(reqwest::Method::POST, &format!("/api/v1/jobs/{id}/cancel"))
                .send()
                .await?,
        )
        .await
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

    /// Extend a lease (heartbeat) so a still-running job is not redelivered.
    pub async fn heartbeat_job(&self, lease: &LeasedJob, extend_secs: u32) -> Result<(), Error> {
        let body = serde_json::json!({
            "queue": lease.queue,
            "lease_id": lease.lease_id,
            "extend_secs": extend_secs,
        });
        Self::expect_no_content(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/jobs/{}/heartbeat", lease.job.id),
            )
            .json(&body)
            .send()
            .await?,
        )
        .await
    }

    /// Report success for a leased job.
    pub async fn complete_job(&self, lease: &LeasedJob, result: Map) -> Result<(), Error> {
        let body = serde_json::json!({
            "queue": lease.queue,
            "lease_id": lease.lease_id,
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
            "queue": lease.queue,
            "lease_id": lease.lease_id,
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
    req.query(&params)
}
