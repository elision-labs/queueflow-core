//! HTTP request/response bodies.
//!
//! Field names mirror the Go API so existing/generated SDKs stay compatible —
//! except durations are seconds (the Go API leaked nanoseconds).

use chrono::{DateTime, Utc};
use queueflow_core::{Job, JobConfig, Map, Workflow};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// Optional per-job configuration overrides.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct JobConfigRequest {
    /// Higher is dequeued first (FIFO within a priority on PGMQ).
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
}

impl JobConfigRequest {
    /// Resolve into `(config, queue_override)`.
    pub fn resolve(self) -> (Option<JobConfig>, Option<String>) {
        let queue = self.queue;
        let needs_config =
            self.priority.is_some() || self.max_retries.is_some() || self.timeout.is_some();
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
    #[schema(value_type = Object)]
    pub payload: Map,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<JobConfigRequest>,
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
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
    pub has_more: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct CreateWorkflowResponse {
    pub workflow_id: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ListWorkflowsResponse {
    pub workflows: Vec<Workflow>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
    pub has_more: bool,
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
}

impl ListQuery {
    /// Convert to a core `ListFilter`, scoping to `tenant` and clamping paging.
    pub fn into_filter(self, tenant: Option<String>) -> queueflow_core::ListFilter {
        let limit = self.limit.unwrap_or(50).clamp(1, 100);
        let offset = self.offset.unwrap_or(0).max(0);
        let order_desc = !matches!(self.order_by.as_deref(), Some("created_at ASC"));
        queueflow_core::ListFilter {
            tenant_id: tenant,
            status: self.status,
            queue: self.queue,
            limit,
            offset,
            order_desc,
        }
    }
}
