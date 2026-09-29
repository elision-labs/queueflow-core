//! HTTP request/response bodies.
//!
//! Durations are expressed as plain integer seconds for clean SDKs across
//! every language.

use chrono::{DateTime, Utc};
use queueflow_core::{BackoffStrategy, DeadLetter, Job, JobConfig, Map, PageCursor, Workflow};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// Optional per-job configuration overrides.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct JobConfigRequest {
    /// Higher is claimed first within a queue (ties: oldest first).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
    /// Per-attempt timeout, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    /// Override the destination queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// How retry delays grow between attempts (default exponential).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_backoff: Option<BackoffStrategy>,
    /// Base retry delay, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_delay_secs: Option<u64>,
    /// Upper bound on any computed retry delay, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_delay_secs: Option<u64>,
    /// Retry-delay jitter in `0.0..=1.0` (e.g. `0.1` = +/-10%).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter_factor: Option<f64>,
}

impl JobConfigRequest {
    /// Resolve into `(config, queue_override)`.
    pub fn resolve(self) -> (Option<JobConfig>, Option<String>) {
        let queue = self.queue;
        let needs_config = self.priority.is_some()
            || self.max_retries.is_some()
            || self.timeout.is_some()
            || self.retry_backoff.is_some()
            || self.retry_delay_secs.is_some()
            || self.retry_max_delay_secs.is_some()
            || self.jitter_factor.is_some();
        let config = if needs_config {
            let mut c = JobConfig::default();
            if let Some(p) = self.priority {
                c.priority = p;
            }
            if let Some(r) = self.max_retries {
                c.max_retries = r;
            }
            if let Some(t) = self.timeout {
                c.timeout_secs = t;
            }
            if let Some(b) = self.retry_backoff {
                c.retry_backoff = b;
            }
            if let Some(d) = self.retry_delay_secs {
                c.retry_delay_secs = d;
            }
            if let Some(m) = self.retry_max_delay_secs {
                c.retry_max_delay_secs = m;
            }
            if let Some(j) = self.jitter_factor {
                c.jitter_factor = Some(j);
            }
            Some(c)
        } else {
            None
        };
        (config, queue)
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct CreateJobRequest {
    /// The registered task handler to invoke.
    pub task_name: String,
    /// Arbitrary JSON object passed to the handler.
    #[serde(default)]
    #[schema(value_type = std::collections::HashMap<String, serde_json::Value>)]
    pub payload: Map,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<JobConfigRequest>,
    /// Don't run before this instant (RFC 3339). The job is created
    /// immediately but stays invisible to workers until then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct CreateJobResponse {
    pub job_id: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct CreateBatchJobsRequest {
    pub jobs: Vec<CreateJobRequest>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct CreateBatchJobsResponse {
    pub job_ids: Vec<String>,
    pub count: usize,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ListJobsResponse {
    pub jobs: Vec<Job>,
    /// Exact total match count. Only present when the request set
    /// `include_total=true`; computing it costs a full count over the filtered
    /// set, so it is opt-in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
    pub limit: i64,
    pub offset: i64,
    pub has_more: bool,
    /// Opaque keyset cursor for the next page (present when `has_more`).
    /// Pass it back as `cursor` to continue where this page ended; cheaper
    /// than deep OFFSET paging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct CreateWorkflowResponse {
    pub workflow_id: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ListWorkflowsResponse {
    pub workflows: Vec<Workflow>,
    /// Exact total match count; only present when `include_total=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
    pub limit: i64,
    pub offset: i64,
    pub has_more: bool,
    /// Opaque keyset cursor for the next page (present when `has_more`).
    /// Pass it back as `cursor` to continue where this page ended; cheaper
    /// than deep OFFSET paging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct CreateCronResponse {
    pub cron_id: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ListCronsResponse {
    pub crons: Vec<queueflow_core::CronSchedule>,
    /// Exact total match count; only present when `include_total=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
    pub limit: i64,
    pub offset: i64,
    pub has_more: bool,
    /// Opaque keyset cursor for the next page (present when `has_more`).
    /// Pass it back as `cursor` to continue where this page ended; cheaper
    /// than deep OFFSET paging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ListDeadLettersResponse {
    pub dead_letters: Vec<DeadLetter>,
    /// Exact total match count; only present when `include_total=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
    pub limit: i64,
    pub offset: i64,
    pub has_more: bool,
    /// Opaque keyset cursor for the next page (present when `has_more`).
    /// Pass it back as `cursor` to continue where this page ended; cheaper
    /// than deep OFFSET paging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ReplayDeadLetterResponse {
    /// The fresh job created from the dead-lettered one.
    pub job_id: String,
}

/// Runtime status of one workflow step (the live progress view).
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct WorkflowStepState {
    /// The step's name (its address within the workflow).
    pub name: String,
    pub status: queueflow_core::StepStatus,
    /// The job executing this step, once one has been scheduled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct WorkflowStepStatesResponse {
    /// One entry per step, in declaration order.
    pub steps: Vec<WorkflowStepState>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct WorkflowDiagramResponse {
    /// Diagram source format. Always `mermaid` today.
    pub format: String,
    /// The diagram document (Mermaid `graph TD`).
    pub diagram: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct TasksResponse {
    pub tasks: Vec<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ErrorBody {
    pub error: String,
    pub timestamp: DateTime<Utc>,
}

impl ErrorBody {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            timestamp: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct HealthStatus {
    pub status: String,
    pub timestamp: DateTime<Utc>,
    pub version: String,
}

impl HealthStatus {
    pub fn ok() -> Self {
        Self {
            status: "healthy".into(),
            timestamp: Utc::now(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ReadyStatus {
    pub status: String,
}

/// Query parameters for the list endpoints.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListQuery {
    /// Filter by status (e.g. `pending`, `completed`).
    pub status: Option<String>,
    /// Filter by queue name (jobs only).
    pub queue: Option<String>,
    /// Page size, 1..=100 (default 50).
    pub limit: Option<i64>,
    /// Number of records to skip (default 0).
    pub offset: Option<i64>,
    /// `created_at ASC` or `created_at DESC` (default DESC).
    pub order_by: Option<String>,
    /// Include the exact `total` count in the response (default false; the
    /// count is an extra full scan over the filtered set).
    pub include_total: Option<bool>,
    /// Opaque keyset cursor from a previous page's `next_cursor`. When set,
    /// `offset` is ignored and listing continues where that page ended.
    pub cursor: Option<String>,
}

impl ListQuery {
    /// Convert to a core `ListFilter`, scoping to `tenant` and clamping
    /// paging. A malformed cursor is an error, not a silently full listing.
    pub fn into_filter(self, tenant: Option<String>) -> Result<queueflow_core::ListFilter, String> {
        let limit = self.limit.unwrap_or(50).clamp(1, 100);
        let offset = self.offset.unwrap_or(0).max(0);
        let order_desc = !matches!(self.order_by.as_deref(), Some("created_at ASC"));
        let after = match &self.cursor {
            None => None,
            Some(c) => Some(decode_cursor(c).ok_or_else(|| "invalid cursor".to_string())?),
        };
        Ok(queueflow_core::ListFilter {
            tenant_id: tenant,
            status: self.status,
            queue: self.queue,
            limit,
            offset,
            order_desc,
            include_total: self.include_total.unwrap_or(false),
            after,
        })
    }
}

/// Encode a keyset cursor: microsecond timestamp and row id, `:`-separated.
/// Opaque to clients; matches the timestamptz resolution Postgres stores.
pub(crate) fn encode_cursor(created_at: DateTime<Utc>, id: &str) -> String {
    format!("{}:{}", created_at.timestamp_micros(), id)
}

pub(crate) fn decode_cursor(s: &str) -> Option<PageCursor> {
    let (micros, id) = s.split_once(':')?;
    let created_at = DateTime::from_timestamp_micros(micros.parse().ok()?)?;
    if id.is_empty() {
        return None;
    }
    Some(PageCursor {
        created_at,
        id: id.to_string(),
    })
}

// ---- Remote worker protocol --------------------------------------------------

/// Body for `POST /api/v1/queues/{queue}/lease`.
#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
pub struct LeaseJobsRequest {
    /// Maximum jobs to lease in one call (1..=100, default 1).
    #[serde(default)]
    pub max_jobs: Option<usize>,
    /// Lease duration in seconds (1..=3600, default 30). Heartbeat to extend.
    #[serde(default)]
    pub lease_secs: Option<u32>,
    /// Long-poll wait when the queue is empty, in seconds (0..=30, default 0).
    #[serde(default)]
    pub wait_secs: Option<u32>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct LeaseJobsResponse {
    pub jobs: Vec<queueflow_core::LeasedJob>,
}

/// Body for `POST /api/v1/jobs/{id}/complete`.
#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct CompleteJobRequest {
    /// The lease token returned by the lease call.
    pub lease_token: String,
    /// Handler result, recorded on the job and merged into workflow context.
    #[serde(default)]
    #[schema(value_type = std::collections::HashMap<String, serde_json::Value>)]
    pub result: Map,
}

/// Body for `POST /api/v1/jobs/{id}/fail`.
#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct FailJobRequest {
    /// The lease token returned by the lease call.
    pub lease_token: String,
    /// Human-readable failure reason.
    pub error: String,
    /// Whether the engine may retry (subject to the job's max_retries).
    /// Defaults to true; send false for permanent failures (e.g. bad input).
    #[serde(default = "default_true")]
    pub retryable: bool,
}

fn default_true() -> bool {
    true
}

/// Body for `POST /api/v1/jobs/{id}/heartbeat`.
#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct HeartbeatRequest {
    /// The lease token returned by the lease call.
    pub lease_token: String,
    /// New lease duration in seconds, measured from now (1..=3600).
    pub extend_secs: u32,
}

/// Response for `POST /api/v1/jobs/{id}/heartbeat`.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct HeartbeatResponse {
    /// The job's current status. `running` means the lease was extended;
    /// anything else (`cancelled`, `completed`, ...) means it was not, and
    /// the worker should stop working on the job.
    pub status: queueflow_core::JobStatus,
}
