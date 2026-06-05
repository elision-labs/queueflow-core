//! Ports (hexagonal architecture): the abstract interfaces the engine depends
//! on. Concrete adapters live in [`crate::adapters`].
//!
//! This is the heart of the testability improvement over the Go version: the
//! engine, workflow scheduler, and HTTP API are written against these traits,
//! so the *entire* system runs against fast, deterministic in-memory adapters
//! in unit tests — no PostgreSQL required — while production wires the
//! Postgres + PGMQ adapters.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::*;

/// Abstract clock so time-dependent behaviour (retry backoff, timestamps) is
/// deterministic in tests via [`crate::adapters::clock::TestClock`].
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Errors a [`JobStore`] can produce.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("job not found: {0}")]
    JobNotFound(String),
    #[error("workflow not found: {0}")]
    WorkflowNotFound(String),
    #[error("workflow step not found: {workflow}/{step}")]
    StepNotFound { workflow: String, step: String },
    #[error("database error: {0}")]
    Database(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Errors a [`MessageQueue`] can produce.
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("queue not found: {0}")]
    QueueNotFound(String),
    #[error("delivery error: {0}")]
    Delivery(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
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
        }
    }
}

/// The JSON body that travels on the queue. Deliberately small — the durable
/// record of truth is the job row in the [`JobStore`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct QueueMessage {
    pub job_id: String,
    pub task_name: String,
    #[serde(default)]
    pub payload: Map,
    pub config: JobConfig,
}

impl QueueMessage {
    /// Build the queue body for a persisted job.
    pub fn for_job(job: &Job) -> Self {
        Self {
            job_id: job.id.clone(),
            task_name: job.task_name.clone(),
            payload: job.payload.clone(),
            config: job.config.clone(),
        }
    }
}

/// A message leased from the queue, with its acknowledgement handle (`msg_id`).
#[derive(Clone, Debug)]
pub struct ReadMessage {
    pub msg_id: i64,
    pub message: QueueMessage,
    pub read_count: u32,
    pub enqueued_at: DateTime<Utc>,
}

/// Durable storage for jobs and workflows. PostgreSQL is the production source
/// of truth; the in-memory adapter mirrors its observable behaviour.
#[async_trait]
pub trait JobStore: Send + Sync {
    async fn create_job(&self, job: &Job) -> Result<(), StorageError>;
    async fn batch_create_jobs(&self, jobs: &[Job]) -> Result<Vec<String>, StorageError>;
    async fn get_job(&self, id: &str) -> Result<Job, StorageError>;
    async fn list_jobs(&self, filter: &ListFilter) -> Result<(Vec<Job>, i64), StorageError>;

    /// Update a job's status, optionally recording an error and/or result.
    /// Implementations stamp `started_at`/`completed_at` as appropriate.
    async fn update_status(
        &self,
        id: &str,
        status: JobStatus,
        error: Option<&str>,
        result: Option<&Json>,
    ) -> Result<(), StorageError>;

    /// Record that a job is scheduled for retry.
    async fn mark_retrying(
        &self,
        id: &str,
        retry_count: u32,
        next_retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<(), StorageError>;

    /// Move a permanently-failed (or undeliverable) job to the dead-letter
    /// table for later inspection / replay.
    async fn move_to_dlq(&self, id: &str, reason: &str, error: &str) -> Result<(), StorageError>;

    async fn count_dead_letters(&self) -> Result<i64, StorageError>;

    async fn ping(&self) -> Result<(), StorageError>;

    // ---- Workflow persistence (same store; one source of truth) ----

    async fn create_workflow(&self, wf: &Workflow) -> Result<(), StorageError>;
    async fn get_workflow(&self, id: &str) -> Result<Workflow, StorageError>;
    async fn list_workflows(
        &self,
        filter: &ListFilter,
    ) -> Result<(Vec<Workflow>, i64), StorageError>;

    /// `(step_name, status, linked_job_id)` for every step of a workflow.
    async fn workflow_step_statuses(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StepRecord>, StorageError>;

    /// Attach a created job to a step (also marks the step as scheduled so it is
    /// never double-enqueued).
    async fn link_step_job(
        &self,
        workflow_id: &str,
        step_name: &str,
        job_id: &str,
    ) -> Result<(), StorageError>;

    async fn set_step_status(
        &self,
        workflow_id: &str,
        step_name: &str,
        status: StepStatus,
        error: Option<&str>,
    ) -> Result<(), StorageError>;

    async fn set_workflow_status(
        &self,
        workflow_id: &str,
        status: WorkflowStatus,
    ) -> Result<(), StorageError>;

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

/// Durable, at-least-once message queue with native delayed delivery and
/// visibility timeouts. The Postgres adapter is backed by PGMQ.
#[async_trait]
pub trait MessageQueue: Send + Sync {
    /// Enqueue for immediate delivery. Returns the queue's message id.
    async fn send(&self, queue: &str, msg: &QueueMessage, priority: i32)
        -> Result<i64, QueueError>;

    /// Enqueue but hide the message for `delay_secs`. This is the backbone of
    /// durable retries: the delay lives *in the queue*, so a retry survives a
    /// process restart (unlike the Go version's in-process timer goroutine).
    async fn send_delayed(
        &self,
        queue: &str,
        msg: &QueueMessage,
        priority: i32,
        delay_secs: u64,
    ) -> Result<i64, QueueError>;

    /// Lease up to `count` visible messages, hiding them for `vt_secs`. If the
    /// worker does not `delete` within the timeout, the message reappears.
    async fn read(
        &self,
        queue: &str,
        vt_secs: u32,
        count: usize,
    ) -> Result<Vec<ReadMessage>, QueueError>;

    /// Acknowledge (remove) a leased message.
    async fn delete(&self, queue: &str, msg_id: i64) -> Result<(), QueueError>;

    /// Remove all messages from a queue; returns the number purged.
    async fn purge(&self, queue: &str) -> Result<u64, QueueError>;

    /// Number of currently-visible messages (best effort; for metrics/stats).
    async fn queue_depth(&self, queue: &str) -> Result<u64, QueueError>;
}
