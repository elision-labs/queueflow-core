//! Core domain types shared across the engine, the workflow orchestrator, and
//! the HTTP API.
//!
//! Design notes:
//! * Durations are plain integer **seconds** (`u64`). Seconds serialize as
//!   `integer/int64` and read naturally in Python/TypeScript/Rust/Go/Java
//!   clients.
//! * Statuses and the backoff strategy are real enums, not free-form strings,
//!   so illegal states are unrepresentable and `match` is exhaustive.
//! * IDs are opaque strings (UUID v4 text), matching the Postgres schema and
//!   keeping JSON payloads consistent across the SDKs.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Arbitrary JSON value (task payloads, results, metadata values).
pub type Json = serde_json::Value;

/// A string-keyed JSON object: payloads, metadata, workflow context, etc.
pub type Map = HashMap<String, serde_json::Value>;

/// Lifecycle state of a single job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying,
    Cancelled,
}

impl JobStatus {
    /// Terminal states are never re-queued or retried.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Retrying => "retrying",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Lifecycle state of a workflow instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Created,
    Running,
    Completed,
    Failed,
    PartiallyFailed,
    Cancelled,
}

impl WorkflowStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::PartiallyFailed | Self::Cancelled
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::PartiallyFailed => "partially_failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Lifecycle state of a single workflow step.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Skipped,
}

impl StepStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
        }
    }
}

/// How retry delays grow between attempts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BackoffStrategy {
    /// Always wait `retry_delay_secs`.
    Fixed,
    /// `retry_delay_secs * (attempt + 1)`.
    Linear,
    /// `retry_delay_secs * 2^attempt`, capped by `retry_max_delay_secs`.
    #[default]
    Exponential,
}

/// Per-job execution configuration. All durations are in seconds.
///
/// Deserialization is partial-friendly: any omitted field takes its
/// [`JobConfig::default`] value (via per-field serde defaults), so
/// workflow-step and cron config overrides can name just the fields they
/// change, and out-of-band rows with sparse `config` JSONB still load.
/// Per-field functions rather than a struct-level `#[serde(default)]`:
/// the struct-level form makes utoipa attach a `default` beside the
/// `BackoffStrategy` `$ref`, which forces a synthetic wrapper type into
/// every generated SDK.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct JobConfig {
    #[serde(default = "defaults::max_retries")]
    pub max_retries: u32,
    #[serde(default = "defaults::retry_delay_secs")]
    pub retry_delay_secs: u64,
    #[serde(default = "defaults::timeout_secs")]
    pub timeout_secs: u64,
    /// Higher is claimed first within a queue; ties break on `scheduled_at`,
    /// then `created_at`.
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub retry_backoff: BackoffStrategy,
    #[serde(default = "defaults::retry_max_delay_secs")]
    pub retry_max_delay_secs: u64,
    /// Optional jitter in `0.0..=1.0`. `0.1` => +/-10% randomization of each
    /// retry delay, which spreads out thundering-herd retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter_factor: Option<f64>,
}

/// Per-field serde defaults for [`JobConfig`]; keep in sync with
/// [`JobConfig::default`].
mod defaults {
    pub(super) fn max_retries() -> u32 {
        3
    }
    pub(super) fn retry_delay_secs() -> u64 {
        60
    }
    pub(super) fn timeout_secs() -> u64 {
        300
    }
    pub(super) fn retry_max_delay_secs() -> u64 {
        3600
    }
}

impl Default for JobConfig {
    /// Sensible defaults: 3 retries, 60s base delay, 5m timeout, exponential
    /// backoff capped at 1h, plus a little jitter.
    fn default() -> Self {
        Self {
            max_retries: defaults::max_retries(),
            retry_delay_secs: defaults::retry_delay_secs(),
            timeout_secs: defaults::timeout_secs(),
            priority: 0,
            retry_backoff: BackoffStrategy::Exponential,
            retry_max_delay_secs: defaults::retry_max_delay_secs(),
            jitter_factor: Some(0.1),
        }
    }
}

/// Bounds accepted by [`JobConfig::validate`]. Generous by design: they exist
/// to reject nonsense (and the arithmetic overflow it causes), not to police
/// reasonable configurations.
pub mod limits {
    /// Maximum accepted `max_retries`.
    pub const MAX_RETRIES: u32 = 1_000;
    /// Maximum accepted per-attempt timeout: 1 day.
    pub const MAX_TIMEOUT_SECS: u64 = 86_400;
    /// Maximum accepted retry delay (base and cap): 30 days.
    pub const MAX_RETRY_DELAY_SECS: u64 = 2_592_000;
    /// Default and maximum number of children a fan-out step may spawn.
    pub const MAX_FAN_OUT: u32 = 10_000;
    /// Maximum accepted workflow timeout: 30 days.
    pub const MAX_WORKFLOW_TIMEOUT_SECS: u64 = 2_592_000;
    /// How deep inline sub-workflows may nest.
    pub const MAX_WORKFLOW_DEPTH: usize = 8;
}

impl JobConfig {
    /// Check this configuration against the documented [`limits`]. The engine
    /// validates every config it accepts (single enqueue, batches, workflow
    /// steps), so an absurd value is a 400 at the boundary instead of an
    /// overflow deep in the retry math.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_retries > limits::MAX_RETRIES {
            return Err(format!(
                "max_retries must be <= {} (got {})",
                limits::MAX_RETRIES,
                self.max_retries
            ));
        }
        if self.timeout_secs == 0 || self.timeout_secs > limits::MAX_TIMEOUT_SECS {
            return Err(format!(
                "timeout_secs must be within 1..={} (got {})",
                limits::MAX_TIMEOUT_SECS,
                self.timeout_secs
            ));
        }
        if self.retry_delay_secs > limits::MAX_RETRY_DELAY_SECS {
            return Err(format!(
                "retry_delay_secs must be <= {} (got {})",
                limits::MAX_RETRY_DELAY_SECS,
                self.retry_delay_secs
            ));
        }
        if self.retry_max_delay_secs > limits::MAX_RETRY_DELAY_SECS {
            return Err(format!(
                "retry_max_delay_secs must be <= {} (got {})",
                limits::MAX_RETRY_DELAY_SECS,
                self.retry_max_delay_secs
            ));
        }
        if let Some(j) = self.jitter_factor {
            if !j.is_finite() || !(0.0..=1.0).contains(&j) {
                return Err(format!("jitter_factor must be within 0.0..=1.0 (got {j})"));
            }
        }
        Ok(())
    }
}

/// A single unit of work.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct Job {
    pub id: String,
    pub queue_name: String,
    pub task_name: String,
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub payload: Map,
    pub config: JobConfig,
    pub status: JobStatus,
    pub created_at: DateTime<Utc>,
    /// When the job becomes claimable. `created_at` for immediate jobs, the
    /// requested `run_at` for scheduled jobs, and the next backoff instant
    /// while retrying — the durable delay lives in the row itself.
    pub scheduled_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// How many times this job has been claimed (delivered to a worker).
    /// Greater than `retry_count + 1` means a lease expired without a report —
    /// i.e. a worker crashed mid-run.
    #[serde(default)]
    pub delivery_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub retry_count: u32,
    /// When this job's next retry becomes claimable (mirrors `scheduled_at`
    /// while the job is `retrying`; kept for audit/inspection).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// The owning workflow step's name (steps are addressed by name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = HashMap<String, serde_json::Value>, nullable)]
    pub result: Option<Json>,
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub metadata: Map,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Client-supplied key that makes job creation idempotent per tenant:
    /// re-submitting the same key returns the original job instead of creating
    /// a duplicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

/// A job leased to a (possibly remote) worker, together with the lease token
/// needed to heartbeat, complete, or fail it.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct LeasedJob {
    pub job: Job,
    /// Opaque, unguessable proof of lease ownership, regenerated on every
    /// claim. Pass it back on heartbeat/complete/fail; a stale token (the
    /// lease expired and the job was reclaimed) is rejected.
    pub lease_token: String,
}

/// What to do with downstream steps when a step fails.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    /// Fail the whole workflow; cancel pending steps. (default)
    #[default]
    Halt,
    /// Treat the failure as a skip and keep going. Dependents are skipped.
    Skip,
    /// Leave the step failed but keep scheduling independent steps; the
    /// workflow ends `partially_failed`. Dependents are skipped.
    Continue,
}

/// How a [`StepCondition`] leaf compares the value at `path` with `value`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConditionOp {
    /// JSON equality (the default when `value` is given).
    #[default]
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    /// The path resolves to a value other than `null` (the default when no
    /// `value` is given).
    Exists,
    /// The value at `path` is an array containing `value`, a string
    /// containing the string `value`, or an object with the key `value`.
    Contains,
    /// `value` is an array containing the value at `path`.
    In,
}

/// A predicate over the workflow context, evaluated when a step becomes
/// ready. Exactly one of `path`, `all`, `any` or `not` is set. A leaf names
/// a dotted `path` into the context (step results live under their step
/// name, so `validate.ok` is the `ok` key of the `validate` step's result)
/// and compares it with `value` using `op`. A false condition skips the step
/// with the reason `condition not met`; its dependents still run, seeing no
/// result for it.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StepCondition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<ConditionOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object, nullable)]
    pub value: Option<Json>,
    /// True when every member is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(no_recursion)]
    pub all: Option<Vec<StepCondition>>,
    /// True when at least one member is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(no_recursion)]
    pub any: Option<Vec<StepCondition>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(no_recursion)]
    pub not: Option<Box<StepCondition>>,
}

/// Dynamic fan-out: run the step's task once per element of the array found
/// at `path` in the workflow context. Each child job receives the step
/// payload plus the element under `item_key` and its position under
/// `_index`. The step completes when every child has, with the result
/// `{"results": [...]}` in element order; a failed child follows the step's
/// `on_failure` policy.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FanOut {
    /// Dotted path into the context, for example `fetch.images`.
    pub path: String,
    /// Payload key the element is passed under (default `item`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_key: Option<String>,
    /// Upper bound on elements; more than this fails the step before any
    /// child runs. Default [`limits::MAX_FAN_OUT`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_items: Option<u32>,
}

/// Reserved for future expansion; only `Continue` is meaningful today.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnSuccess {
    #[default]
    Continue,
}

/// A node in a workflow DAG. Steps are addressed by their unique `name`;
/// `depends_on` lists the names of steps that must complete first.
///
/// A step is one of three kinds: a plain task (`task_name`), a fan-out
/// (`task_name` plus `fan_out`), or a sub-workflow (`workflow`, in which case
/// `task_name` may be empty). `when` gates any kind on the context.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct WorkflowStep {
    pub name: String,
    /// The task handler to run. May be omitted for a sub-workflow step.
    #[serde(default)]
    pub task_name: String,
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub payload: Map,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<JobConfig>,
    /// Queue for this step's job(s); the server's default queue when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    #[serde(default)]
    pub on_success: OnSuccess,
    #[serde(default)]
    pub on_failure: OnFailure,
    /// Run only when this condition holds against the workflow context at
    /// the moment the step becomes ready; otherwise the step is skipped
    /// with reason `condition not met` and its dependents proceed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<StepCondition>,
    /// Run the task once per element of an array in the context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_out: Option<FanOut>,
    /// Run an inline child workflow instead of a task. The child's context
    /// is its own declared context plus this step's payload, with the
    /// parent's context under `_parent`; the step's result is the child's
    /// final context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(no_recursion)]
    pub workflow: Option<Box<CreateWorkflowRequest>>,
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub metadata: Map,
}

impl WorkflowStep {
    /// A step that runs an inline child workflow.
    pub fn is_sub_workflow(&self) -> bool {
        self.workflow.is_some()
    }
}

/// A workflow instance and its steps.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct Workflow {
    pub id: String,
    pub name: String,
    pub steps: Vec<WorkflowStep>,
    pub status: WorkflowStatus,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Why the workflow ended `failed` or `cancelled`, when known (an
    /// upstream failure, a timeout, a user cancel).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    /// The whole-workflow time budget it was created with, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// When the janitor fails the workflow if it is still running (set from
    /// `timeout_secs`; pushed forward by a retry).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<DateTime<Utc>>,
    /// How many times `POST /workflows/{id}/retry` re-ran this workflow.
    #[serde(default)]
    pub retry_count: u32,
    /// Set on a child workflow spawned by a sub-workflow step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_workflow_id: Option<String>,
    /// The parent's step that spawned this child.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_step_name: Option<String>,
    /// Accumulated step results, keyed by step name. Passed to downstream steps
    /// under the `_context` payload key.
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub context: Map,
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub metadata: Map,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
}

/// Request body for creating a workflow. Shared by the builder DSL and the API
/// so callers and SDKs use the same contract.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CreateWorkflowRequest {
    pub name: String,
    pub steps: Vec<WorkflowStep>,
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub context: Map,
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub metadata: Map,
    /// Whole-workflow time budget in seconds. When it runs out the janitor
    /// cancels every unfinished step and fails the workflow with
    /// `workflow timed out`. Unset means no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

/// The reason recorded on a step skipped because its `when` condition was
/// false. Dependents of such a step still run; dependents of a step skipped
/// by its failure policy do not.
pub const CONDITION_NOT_MET: &str = "condition not met";

/// The context key under which a child workflow sees its parent's context.
pub const PARENT_CONTEXT_KEY: &str = "_parent";

/// Payload key carrying a fan-out child's position.
pub const FAN_OUT_INDEX_KEY: &str = "_index";

/// Job metadata keys stamped on fan-out children.
pub const FAN_OUT_INDEX_META: &str = "fan_out_index";
pub const FAN_OUT_TOTAL_META: &str = "fan_out_total";

/// A dead-lettered job: a terminal failure recorded for inspection and
/// replay. The original job row remains (subject to retention); this entry
/// captures why it died and, once replayed, which fresh job took its place.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct DeadLetter {
    pub id: i64,
    pub job_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_name: Option<String>,
    /// Why the job dead-lettered: `max_attempts_exceeded`, `non_retryable`,
    /// or `handler_not_found`.
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub created_at: DateTime<Utc>,
    /// Set once this entry has been replayed; a dead letter replays at most
    /// once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replayed_at: Option<DateTime<Utc>>,
    /// The fresh job created by the replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_job_id: Option<String>,
}

/// A recurring enqueue schedule. Expressions are standard 5-field crontab
/// (`minute hour day-of-month month day-of-week`), evaluated in **UTC**; a
/// 6/7-field form with leading seconds is also accepted.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CronSchedule {
    pub id: String,
    /// Unique per tenant.
    pub name: String,
    pub cron_expr: String,
    pub task_name: String,
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub payload: Map,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<JobConfig>,
    /// Queue for the enqueued jobs (the engine default when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub enabled: bool,
    /// The next instant this schedule fires. Missed occurrences (server
    /// down) collapse into at most one catch-up firing.
    pub next_run_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_enqueued_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Request body for creating a cron schedule.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CreateCronRequest {
    /// Unique per tenant.
    pub name: String,
    /// 5-field crontab (UTC); 6/7 fields with leading seconds also accepted.
    pub cron_expr: String,
    pub task_name: String,
    #[serde(default)]
    #[schema(value_type = HashMap<String, serde_json::Value>)]
    pub payload: Map,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<JobConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
}

/// The key under which a step's payload receives upstream workflow context.
pub const CONTEXT_KEY: &str = "_context";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_status_round_trips_snake_case() {
        let json = serde_json::to_string(&JobStatus::Retrying).unwrap();
        assert_eq!(json, "\"retrying\"");
        let back: JobStatus = serde_json::from_str("\"completed\"").unwrap();
        assert_eq!(back, JobStatus::Completed);
    }

    #[test]
    fn default_job_config_matches_go_reference() {
        let c = JobConfig::default();
        assert_eq!(c.max_retries, 3);
        assert_eq!(c.retry_delay_secs, 60);
        assert_eq!(c.timeout_secs, 300);
        assert_eq!(c.retry_backoff, BackoffStrategy::Exponential);
        assert_eq!(c.retry_max_delay_secs, 3600);
    }

    #[test]
    fn workflow_status_partially_failed_serializes_with_underscore() {
        assert_eq!(
            serde_json::to_string(&WorkflowStatus::PartiallyFailed).unwrap(),
            "\"partially_failed\""
        );
    }

    #[test]
    fn on_failure_defaults_to_halt() {
        assert_eq!(OnFailure::default(), OnFailure::Halt);
    }

    #[test]
    fn partial_job_config_fills_defaults() {
        // Workflow-step and cron config overrides name only the fields they
        // change; everything else must come from the defaults.
        let c: JobConfig = serde_json::from_str(r#"{"max_retries": 7}"#).unwrap();
        assert_eq!(c.max_retries, 7);
        assert_eq!(c.timeout_secs, JobConfig::default().timeout_secs);
        assert_eq!(c.retry_backoff, BackoffStrategy::Exponential);

        // jitter_factor keeps its field-level default (None when absent):
        // stored configs omit a None jitter, so absent-means-None is what
        // keeps round-trips faithful.
        let empty: JobConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            empty,
            JobConfig {
                jitter_factor: None,
                ..JobConfig::default()
            }
        );
    }

    #[test]
    fn job_serializes_without_optional_nulls() {
        let job = Job {
            id: "j1".into(),
            queue_name: "default".into(),
            task_name: "echo".into(),
            payload: Map::new(),
            config: JobConfig::default(),
            status: JobStatus::Pending,
            created_at: Utc::now(),
            scheduled_at: Utc::now(),
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
            tenant_id: None,
            idempotency_key: None,
        };
        let v = serde_json::to_value(&job).unwrap();
        assert!(v.get("started_at").is_none());
        assert!(v.get("workflow_id").is_none());
        assert_eq!(v["status"], "pending");
    }
}
