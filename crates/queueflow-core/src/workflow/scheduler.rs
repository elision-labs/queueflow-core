//! Runtime workflow orchestration.
//!
//! The scheduler turns a validated DAG into actual queued jobs, gates each
//! step on its dependencies and its `when` condition, propagates per-step
//! results through the workflow context, applies failure policies, spawns
//! fan-out children and sub-workflows, enforces the workflow deadline, and
//! aggregates the final workflow status.
//!
//! It is intentionally generic over the [`JobStore`] port so the full
//! orchestration is exercised by fast in-memory tests as well as the Postgres
//! adapter.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use uuid::Uuid;

use crate::domain::*;
use crate::engine::{validate_queue_name, KnownQueues};
use crate::error::EngineError;
use crate::ports::{Clock, JobStore, StepJobOutcome, StepRecord, StorageError};
use crate::stats::EngineStats;
use crate::workflow::condition;
use crate::workflow::dag::DependencyGraph;

/// Schedules and advances workflows over the storage port.
pub struct WorkflowScheduler<JS> {
    store: Arc<JS>,
    clock: Arc<dyn Clock>,
    default_queue: String,
    stats: Arc<EngineStats>,
    known_queues: Arc<KnownQueues>,
}

/// Validate a workflow definition (recursively, for inline sub-workflows):
/// the DAG shape, every step's kind and config, conditions, fan-outs,
/// queue names, nesting depth and the timeout.
pub fn validate_request(req: &CreateWorkflowRequest, depth: usize) -> Result<(), EngineError> {
    if depth > limits::MAX_WORKFLOW_DEPTH {
        return Err(EngineError::Validation(format!(
            "sub-workflows nest deeper than {}",
            limits::MAX_WORKFLOW_DEPTH
        )));
    }
    if req.name.trim().is_empty() {
        return Err(EngineError::Validation("workflow name is required".into()));
    }
    if let Some(t) = req.timeout_secs {
        if t == 0 || t > limits::MAX_WORKFLOW_TIMEOUT_SECS {
            return Err(EngineError::Validation(format!(
                "timeout_secs must be within 1..={}",
                limits::MAX_WORKFLOW_TIMEOUT_SECS
            )));
        }
    }
    DependencyGraph::new(&req.steps)?;
    for step in &req.steps {
        let at = |e: String| EngineError::Validation(format!("step '{}': {e}", step.name));
        if step.name.trim().is_empty() {
            return Err(EngineError::Validation("step name is required".into()));
        }
        match &step.workflow {
            Some(child) => {
                if step.fan_out.is_some() {
                    return Err(at(
                        "a step is either a sub-workflow or a fan-out, not both".into()
                    ));
                }
                validate_request(child, depth + 1).map_err(|e| at(format!("sub-workflow: {e}")))?;
            }
            None => {
                if step.task_name.trim().is_empty() {
                    return Err(at(
                        "task_name is required unless the step runs a workflow".into()
                    ));
                }
            }
        }
        if let Some(cfg) = &step.config {
            cfg.validate().map_err(at)?;
        }
        if let Some(q) = &step.queue {
            validate_queue_name(q).map_err(|e| at(e.to_string()))?;
        }
        if let Some(cond) = &step.when {
            condition::validate(cond).map_err(|e| at(format!("when: {e}")))?;
        }
        if let Some(f) = &step.fan_out {
            if f.path.trim().is_empty() {
                return Err(at("fan_out.path is required".into()));
            }
            if f.max_items
                .is_some_and(|m| m == 0 || m > limits::MAX_FAN_OUT)
            {
                return Err(at(format!(
                    "fan_out.max_items must be within 1..={}",
                    limits::MAX_FAN_OUT
                )));
            }
            if f.item_key
                .as_deref()
                .is_some_and(|k| k.is_empty() || k == CONTEXT_KEY)
            {
                return Err(at(
                    "fan_out.item_key must be a non-empty key other than _context".into(),
                ));
            }
        }
    }
    Ok(())
}

/// Is this skipped step one whose dependents may still run (skipped by its
/// `when` condition) rather than one skipped by a failure policy?
fn skipped_by_condition(rec: &StepRecord) -> bool {
    rec.status == StepStatus::Skipped && rec.error.as_deref() == Some(CONDITION_NOT_MET)
}

/// A dependency counts as satisfied when it completed or was skipped by its
/// condition.
fn satisfies_dependents(rec: &StepRecord) -> bool {
    rec.status == StepStatus::Completed || skipped_by_condition(rec)
}

/// The deadline a timeout implies from `now`.
pub(crate) fn deadline_from(
    now: DateTime<Utc>,
    timeout_secs: Option<u64>,
) -> Option<DateTime<Utc>> {
    timeout_secs.map(|t| now + ChronoDuration::seconds(t.min(i64::MAX as u64) as i64))
}

fn fan_out_error(failed: &[&StepJobOutcome], total: usize) -> String {
    let first = failed[0];
    format!(
        "{} of {} items failed; item {}: {}",
        failed.len(),
        total,
        first.index,
        first.error.as_deref().unwrap_or("unknown error")
    )
}

fn json_kind(v: &Json) -> &'static str {
    match v {
        Json::Null => "null",
        Json::Bool(_) => "a boolean",
        Json::Number(_) => "a number",
        Json::String(_) => "a string",
        Json::Array(_) => "an array",
        Json::Object(_) => "an object",
    }
}

impl<JS> WorkflowScheduler<JS>
where
    JS: JobStore,
{
    pub(crate) fn new(
        store: Arc<JS>,
        clock: Arc<dyn Clock>,
        default_queue: String,
        stats: Arc<EngineStats>,
        known_queues: Arc<KnownQueues>,
    ) -> Self {
        Self {
            store,
            clock,
            default_queue,
            stats,
            known_queues,
        }
    }

    fn build_workflow(
        &self,
        req: CreateWorkflowRequest,
        tenant_id: Option<String>,
        parent: Option<(String, String)>,
    ) -> Workflow {
        let now = self.clock.now();
        let (parent_workflow_id, parent_step_name) = match parent {
            Some((w, s)) => (Some(w), Some(s)),
            None => (None, None),
        };
        Workflow {
            id: Uuid::new_v4().to_string(),
            name: req.name,
            steps: req.steps,
            status: WorkflowStatus::Created,
            created_at: now,
            started_at: None,
            completed_at: None,
            error_message: None,
            timeout_secs: req.timeout_secs,
            deadline_at: deadline_from(now, req.timeout_secs),
            retry_count: 0,
            parent_workflow_id,
            parent_step_name,
            context: req.context,
            metadata: req.metadata,
            tenant_id,
        }
    }

    /// Validate, persist, and start a workflow. Returns the new workflow id.
    /// A cyclic or otherwise invalid definition yields [`EngineError`] (HTTP
    /// 400) and persists nothing.
    pub async fn create(
        &self,
        req: CreateWorkflowRequest,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError> {
        validate_request(&req, 0)?;
        let wf = self.build_workflow(req, tenant_id, None);
        self.store.create_workflow(&wf).await?;
        let _ = self
            .store
            .set_workflow_status(&wf.id, WorkflowStatus::Running, None)
            .await?;
        EngineStats::incr(&self.stats.workflows_created);
        self.advance(&wf.id).await?;
        Ok(wf.id)
    }

    /// Called by a worker when a workflow-linked job completes successfully.
    ///
    /// Write ordering is load-bearing: the result is merged into the shared
    /// context *before* the step is marked `Completed`, so any observer that
    /// sees the step terminal is guaranteed a context read that already
    /// contains its result ([`Self::advance`] relies on this to hand fan-in
    /// steps a complete `_context`). A crash between the two writes leaves a
    /// terminal job under a non-terminal step, which the janitor's
    /// stalled-step sweep re-drives through this method (the merge is
    /// idempotent per step). For a fan-out step the job is one child, and
    /// the step settles only once every child has.
    pub async fn on_step_completed(
        &self,
        workflow_id: &str,
        step_name: &str,
        result: &Map,
    ) -> Result<(), EngineError> {
        let wf = self.store.get_workflow(workflow_id).await?;
        if let Some(step) = wf.steps.iter().find(|s| s.name == step_name) {
            if step.fan_out.is_some() {
                return self.settle_fan_out(&wf, step).await;
            }
        }
        self.complete_step(&wf, step_name, result).await
    }

    async fn complete_step(
        &self,
        wf: &Workflow,
        step_name: &str,
        result: &Map,
    ) -> Result<(), EngineError> {
        let value = serde_json::to_value(result).unwrap_or(Json::Null);
        self.store
            .merge_workflow_context(&wf.id, step_name, &value)
            .await?;
        self.store
            .set_step_status(&wf.id, step_name, StepStatus::Completed, None)
            .await?;
        Box::pin(self.advance(&wf.id)).await
    }

    /// Called by a worker when a workflow-linked job fails permanently. For
    /// a fan-out step the job is one child; the step's policy decides
    /// whether its siblings are cancelled now or allowed to finish.
    pub async fn on_step_failed(
        &self,
        workflow_id: &str,
        step_name: &str,
        error: &str,
    ) -> Result<(), EngineError> {
        let wf = self.store.get_workflow(workflow_id).await?;
        if let Some(step) = wf.steps.iter().find(|s| s.name == step_name) {
            if step.fan_out.is_some() {
                return self.settle_fan_out(&wf, step).await;
            }
        }
        self.apply_failure(&wf, step_name, error).await
    }

    /// Apply a step's `on_failure` policy after it failed with `error`.
    async fn apply_failure(
        &self,
        wf: &Workflow,
        step_name: &str,
        error: &str,
    ) -> Result<(), EngineError> {
        let policy = wf
            .steps
            .iter()
            .find(|s| s.name == step_name)
            .map(|s| s.on_failure)
            .unwrap_or(OnFailure::Halt);

        match policy {
            OnFailure::Halt => {
                self.store
                    .set_step_status(&wf.id, step_name, StepStatus::Failed, Some(error))
                    .await?;
                // Halt everything that has not already reached a terminal
                // state, live jobs and child workflows included: an unclaimed
                // sibling job would otherwise stay claimable and start fresh
                // work for a dead workflow. A currently-running sibling keeps
                // its handler alive, but flipping its job to cancelled means
                // every lease-guarded outcome write (and the next heartbeat)
                // is refused.
                let reason = format!("step '{step_name}' failed: {error}");
                Box::pin(self.abort(
                    wf,
                    WorkflowStatus::Failed,
                    Some(&reason),
                    "workflow halted by an upstream failure",
                ))
                .await
            }
            OnFailure::Skip => {
                self.store
                    .set_step_status(&wf.id, step_name, StepStatus::Skipped, Some(error))
                    .await?;
                Box::pin(self.advance(&wf.id)).await
            }
            OnFailure::Continue => {
                self.store
                    .set_step_status(&wf.id, step_name, StepStatus::Failed, Some(error))
                    .await?;
                Box::pin(self.advance(&wf.id)).await
            }
        }
    }

    /// Decide a fan-out step from its children's outcomes: wait while any
    /// child is live (unless the policy is `halt` and one already failed),
    /// complete with every result in order, or fail with the first error.
    async fn settle_fan_out(&self, wf: &Workflow, step: &WorkflowStep) -> Result<(), EngineError> {
        let outcomes = self.store.step_job_outcomes(&wf.id, &step.name).await?;
        let failed: Vec<&StepJobOutcome> = outcomes
            .iter()
            .filter(|o| matches!(o.status, JobStatus::Failed | JobStatus::Cancelled))
            .collect();
        let all_terminal = outcomes.iter().all(|o| o.status.is_terminal());
        if !failed.is_empty() && step.on_failure == OnFailure::Halt {
            // Fail fast: the remaining children are cancelled with the rest
            // of the workflow.
            let msg = fan_out_error(&failed, outcomes.len());
            self.store
                .cancel_step_jobs(&wf.id, &step.name, "a sibling fan-out item failed")
                .await?;
            return self.apply_failure(wf, &step.name, &msg).await;
        }
        if !all_terminal {
            return Ok(()); // the step settles with its last child
        }
        if failed.is_empty() {
            let results: Vec<Json> = outcomes
                .iter()
                .map(|o| o.result.clone().unwrap_or(Json::Null))
                .collect();
            let mut result = Map::new();
            result.insert("results".into(), Json::Array(results));
            return self.complete_step(wf, &step.name, &result).await;
        }
        let msg = fan_out_error(&failed, outcomes.len());
        self.apply_failure(wf, &step.name, &msg).await
    }

    /// A child workflow reached a terminal status: settle the parent step
    /// that spawned it. The child's final context is the step's result.
    pub async fn on_child_workflow_finished(&self, child: &Workflow) -> Result<(), EngineError> {
        let (Some(parent_id), Some(step_name)) =
            (&child.parent_workflow_id, &child.parent_step_name)
        else {
            return Ok(());
        };
        let parent = match self.store.get_workflow(parent_id).await {
            Ok(p) => p,
            Err(StorageError::WorkflowNotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if parent.status.is_terminal() {
            return Ok(());
        }
        // A step that already settled (for example cancelled by the parent's
        // own abort, which then cascaded to this child) is never re-settled
        // by its child's outcome.
        let step_done = self
            .store
            .workflow_step_statuses(parent_id)
            .await?
            .iter()
            .any(|r| &r.name == step_name && r.status.is_terminal());
        if step_done {
            return Ok(());
        }
        match child.status {
            WorkflowStatus::Completed => {
                self.complete_step(&parent, step_name, &child.context).await
            }
            status => {
                let msg = format!(
                    "child workflow {} {}{}",
                    child.id,
                    status.as_str(),
                    child
                        .error_message
                        .as_deref()
                        .map(|e| format!(": {e}"))
                        .unwrap_or_default()
                );
                self.apply_failure(&parent, step_name, &msg).await
            }
        }
    }

    /// End a workflow now: cancel every step that has not finished (their
    /// jobs and child workflows included), then record `status` with
    /// `error`. Used for user cancels, halts and timeouts. A workflow that
    /// is already terminal is left alone.
    pub(crate) async fn abort(
        &self,
        wf: &Workflow,
        status: WorkflowStatus,
        error: Option<&str>,
        step_reason: &str,
    ) -> Result<(), EngineError> {
        for rec in self.store.workflow_step_statuses(&wf.id).await? {
            if rec.status.is_terminal() {
                continue;
            }
            self.store
                .set_step_status(&wf.id, &rec.name, StepStatus::Cancelled, Some(step_reason))
                .await?;
            // Cancel the step's live jobs too, or a worker would still
            // execute work for a dead workflow: a pending job stays
            // claimable, and a running one could land its completion.
            self.store
                .cancel_step_jobs(&wf.id, &rec.name, step_reason)
                .await?;
            if let Some(child_id) = &rec.child_workflow_id {
                if let Ok(child) = self.store.get_workflow(child_id).await {
                    if !child.status.is_terminal() {
                        Box::pin(self.abort(
                            &child,
                            WorkflowStatus::Cancelled,
                            Some("parent workflow ended"),
                            "parent workflow ended",
                        ))
                        .await?;
                    }
                }
            }
        }
        self.finalize(wf, status, error).await
    }

    /// Record a terminal status (guarded: a concurrent finalization wins
    /// and this is a no-op), count it, and settle the parent step if this
    /// is a child workflow.
    async fn finalize(
        &self,
        wf: &Workflow,
        status: WorkflowStatus,
        error: Option<&str>,
    ) -> Result<(), EngineError> {
        if !self
            .store
            .set_workflow_status(&wf.id, status, error)
            .await?
        {
            return Ok(());
        }
        match status {
            WorkflowStatus::Completed => EngineStats::incr(&self.stats.workflows_completed),
            WorkflowStatus::Failed | WorkflowStatus::PartiallyFailed => {
                EngineStats::incr(&self.stats.workflows_failed)
            }
            _ => {}
        }
        if wf.parent_workflow_id.is_some() {
            // Re-read: the context accumulated since `wf` was loaded, and
            // the parent needs the final version.
            let fresh = self.store.get_workflow(&wf.id).await?;
            if let Err(e) = Box::pin(self.on_child_workflow_finished(&fresh)).await {
                tracing::error!(workflow_id = %wf.id, error = %e, "parent step settlement failed; janitor will retry");
            }
        }
        Ok(())
    }

    /// Fail a workflow whose deadline passed.
    pub(crate) async fn timeout(&self, workflow_id: &str) -> Result<(), EngineError> {
        let wf = self.store.get_workflow(workflow_id).await?;
        if wf.status.is_terminal() {
            return Ok(());
        }
        let msg = match wf.timeout_secs {
            Some(t) => format!("workflow timed out after {t}s"),
            None => "workflow timed out".to_string(),
        };
        self.abort(
            &wf,
            WorkflowStatus::Failed,
            Some(&msg),
            "workflow timed out",
        )
        .await
    }

    /// Re-run a failed, partially failed or cancelled workflow from its
    /// unfinished steps: every step that did not complete goes back to
    /// pending (completed steps keep their results and are not re-run), the
    /// deadline is re-armed from the original timeout, and scheduling
    /// resumes. A running or completed workflow is a conflict.
    pub async fn retry(&self, workflow_id: &str) -> Result<(), EngineError> {
        let wf = self.store.get_workflow(workflow_id).await?;
        match wf.status {
            WorkflowStatus::Failed | WorkflowStatus::PartiallyFailed | WorkflowStatus::Cancelled => {}
            other => {
                return Err(EngineError::Conflict(format!(
                    "workflow is {}; only failed, partially failed or cancelled workflows can be retried",
                    other.as_str()
                )))
            }
        }
        let to_reset: Vec<String> = self
            .store
            .workflow_step_statuses(&wf.id)
            .await?
            .into_iter()
            .filter(|r| !satisfies_dependents(r))
            .map(|r| r.name)
            .collect();
        let deadline = deadline_from(self.clock.now(), wf.timeout_secs);
        if !self
            .store
            .reset_workflow_for_retry(&wf.id, &to_reset, deadline)
            .await?
        {
            return Err(StorageError::WorkflowNotFound(wf.id.clone()).into());
        }
        self.advance(&wf.id).await
    }

    /// Render the workflow's DAG as a Mermaid document.
    pub async fn diagram(&self, workflow_id: &str) -> Result<String, EngineError> {
        let wf = self.store.get_workflow(workflow_id).await?;
        let graph = DependencyGraph::new(&wf.steps)?;
        Ok(graph.mermaid())
    }

    /// Schedule newly-ready steps and cascade skips, to a fixpoint, then
    /// recompute the workflow's aggregate status. Idempotent and a no-op once
    /// the workflow is terminal. Also re-driven by the janitor for workflows
    /// stalled by a crash mid-advance.
    pub(crate) async fn advance(&self, workflow_id: &str) -> Result<(), EngineError> {
        let wf = self.store.get_workflow(workflow_id).await?;
        if wf.status.is_terminal() {
            return Ok(());
        }

        loop {
            let records = self.store.workflow_step_statuses(workflow_id).await?;
            let by_name: HashMap<&str, &StepRecord> =
                records.iter().map(|r| (r.name.as_str(), r)).collect();
            let scheduled: HashSet<&str> = records
                .iter()
                .filter(|r| r.job_id.is_some() || r.child_workflow_id.is_some())
                .map(|r| r.name.as_str())
                .collect();

            let mut changed = false;
            // The context is read at most once per round, lazily, on the
            // first step that becomes ready. Reading it *after* the status
            // snapshot above preserves the guarantee fan-in steps rely on:
            // a dependency observed Completed has already merged its result
            // (merge-before-status ordering in `complete_step`).
            let mut context: Option<Map> = None;
            for step in &wf.steps {
                let name = step.name.as_str();
                let current = by_name.get(name).map(|r| r.status).unwrap_or_default();
                if current != StepStatus::Pending || scheduled.contains(name) {
                    continue;
                }

                let deps_satisfied = step.depends_on.iter().all(|d| {
                    by_name
                        .get(d.as_str())
                        .is_some_and(|r| satisfies_dependents(r))
                });
                if deps_satisfied {
                    if context.is_none() {
                        context = Some(self.store.get_workflow_context(workflow_id).await?);
                    }
                    let ctx = context.as_ref().unwrap();
                    if let Some(cond) = &step.when {
                        if !condition::evaluate(cond, ctx) {
                            self.store
                                .set_step_status(
                                    workflow_id,
                                    name,
                                    StepStatus::Skipped,
                                    Some(CONDITION_NOT_MET),
                                )
                                .await?;
                            changed = true;
                            continue;
                        }
                    }
                    Box::pin(self.schedule_step(&wf, step, ctx)).await?;
                    changed = true;
                } else {
                    let dep_unsatisfiable = step.depends_on.iter().any(|d| {
                        by_name
                            .get(d.as_str())
                            .is_some_and(|r| r.status.is_terminal() && !satisfies_dependents(r))
                    });
                    if dep_unsatisfiable {
                        self.store
                            .set_step_status(
                                workflow_id,
                                name,
                                StepStatus::Skipped,
                                Some("a dependency did not complete"),
                            )
                            .await?;
                        changed = true;
                    }
                }
            }

            if !changed {
                break;
            }
        }

        self.aggregate(&wf).await
    }

    /// Recompute and persist the workflow's aggregate status when all steps
    /// are terminal. Steps skipped by their condition count as successes.
    async fn aggregate(&self, wf: &Workflow) -> Result<(), EngineError> {
        // Don't override an already-decided (terminal) workflow. Status-only
        // read: the full workflow (steps included) is not needed here.
        if self.store.get_workflow_status(&wf.id).await?.is_terminal() {
            return Ok(());
        }

        let records = self.store.workflow_step_statuses(&wf.id).await?;
        if !records.iter().all(|r| r.status.is_terminal()) {
            return Ok(());
        }

        let succeeded = records.iter().filter(|r| satisfies_dependents(r)).count();
        let skipped = records
            .iter()
            .filter(|r| r.status == StepStatus::Skipped && !skipped_by_condition(r))
            .count();
        let failed_or_cancelled = records
            .iter()
            .filter(|r| matches!(r.status, StepStatus::Failed | StepStatus::Cancelled))
            .count();

        let status = if failed_or_cancelled == 0 && skipped == 0 {
            WorkflowStatus::Completed
        } else if succeeded == 0 && skipped == 0 {
            WorkflowStatus::Failed
        } else {
            WorkflowStatus::PartiallyFailed
        };
        let error = records
            .iter()
            .find(|r| r.status == StepStatus::Failed)
            .map(|r| {
                format!(
                    "step '{}' failed: {}",
                    r.name,
                    r.error.as_deref().unwrap_or("unknown error")
                )
            });
        self.finalize(wf, status, error.as_deref()).await
    }

    /// Schedule one ready step according to its kind.
    async fn schedule_step(
        &self,
        wf: &Workflow,
        step: &WorkflowStep,
        context: &Map,
    ) -> Result<(), EngineError> {
        if let Some(child_req) = &step.workflow {
            return self.spawn_child(wf, step, child_req, context).await;
        }
        if let Some(fan_out) = &step.fan_out {
            return self.fan_out(wf, step, fan_out, context).await;
        }
        let job = self.step_job(wf, step, step.payload.clone(), context, Map::new());
        self.enqueue_task(std::slice::from_ref(&job)).await
    }

    /// Build the job for a step (or one fan-out child): the given payload
    /// with the workflow context under `_context`.
    fn step_job(
        &self,
        wf: &Workflow,
        step: &WorkflowStep,
        mut payload: Map,
        context: &Map,
        mut metadata: Map,
    ) -> Job {
        if !context.is_empty() {
            payload.insert(
                CONTEXT_KEY.to_string(),
                serde_json::to_value(context).unwrap_or(Json::Null),
            );
        }
        for (k, v) in &step.metadata {
            metadata.entry(k.clone()).or_insert_with(|| v.clone());
        }
        let now = self.clock.now();
        Job {
            id: Uuid::new_v4().to_string(),
            queue_name: step
                .queue
                .clone()
                .unwrap_or_else(|| self.default_queue.clone()),
            task_name: step.task_name.clone(),
            payload,
            config: step.config.clone().unwrap_or_default(),
            status: JobStatus::Pending,
            created_at: now,
            scheduled_at: now,
            started_at: None,
            completed_at: None,
            delivery_count: 0,
            error_message: None,
            retry_count: 0,
            next_retry_at: None,
            workflow_id: Some(wf.id.clone()),
            workflow_step_id: Some(step.name.clone()),
            result: None,
            metadata,
            tenant_id: wf.tenant_id.clone(),
            idempotency_key: None,
        }
    }

    /// Claim the step and create its job(s) in one atomic store operation.
    /// Two workers completing sibling steps concurrently both run `advance`
    /// and can both see a join step as unscheduled; the claim guarantees
    /// exactly one job set is ever persisted. Atomicity matters because the
    /// insert is the publish: a separately-inserted loser row would be
    /// claimable by a worker before it could be retired.
    async fn enqueue_task(&self, jobs: &[Job]) -> Result<(), EngineError> {
        let Some(first) = jobs.first() else {
            return Ok(());
        };
        self.known_queues
            .ensure(&*self.store, first.tenant_id.as_deref(), &first.queue_name)
            .await;
        if self.store.create_step_jobs(jobs).await? {
            EngineStats::add(&self.stats.jobs_created, jobs.len() as u64);
        }
        Ok(())
    }

    /// Spawn the children of a fan-out step from the array at its path. A
    /// missing or non-array value, or too many items, fails the step (its
    /// policy applies); an empty array completes it at once.
    async fn fan_out(
        &self,
        wf: &Workflow,
        step: &WorkflowStep,
        fan_out: &FanOut,
        context: &Map,
    ) -> Result<(), EngineError> {
        let items = match condition::lookup(context, &fan_out.path) {
            Some(Json::Array(items)) => items.clone(),
            Some(other) => {
                let msg = format!(
                    "fan_out path '{}' is {} rather than an array",
                    fan_out.path,
                    json_kind(other)
                );
                return self.apply_failure(wf, &step.name, &msg).await;
            }
            None => {
                let msg = format!(
                    "fan_out path '{}' is not in the workflow context",
                    fan_out.path
                );
                return self.apply_failure(wf, &step.name, &msg).await;
            }
        };
        let max = fan_out.max_items.unwrap_or(limits::MAX_FAN_OUT) as usize;
        if items.len() > max {
            let msg = format!("fan_out has {} items; the limit is {max}", items.len());
            return self.apply_failure(wf, &step.name, &msg).await;
        }
        if items.is_empty() {
            let mut result = Map::new();
            result.insert("results".into(), Json::Array(vec![]));
            return self.complete_step(wf, &step.name, &result).await;
        }
        let item_key = fan_out
            .item_key
            .clone()
            .unwrap_or_else(|| "item".to_string());
        let total = items.len();
        let jobs: Vec<Job> = items
            .into_iter()
            .enumerate()
            .map(|(i, item)| {
                let mut payload = step.payload.clone();
                payload.insert(item_key.clone(), item);
                payload.insert(FAN_OUT_INDEX_KEY.into(), Json::from(i));
                let mut metadata = Map::new();
                metadata.insert(FAN_OUT_INDEX_META.into(), Json::from(i));
                metadata.insert(FAN_OUT_TOTAL_META.into(), Json::from(total));
                self.step_job(wf, step, payload, context, metadata)
            })
            .collect();
        self.enqueue_task(&jobs).await
    }

    /// Create and start the child workflow of a sub-workflow step. The
    /// child's context is its declared context, then the step payload, then
    /// the parent's context under `_parent`.
    async fn spawn_child(
        &self,
        wf: &Workflow,
        step: &WorkflowStep,
        child_req: &CreateWorkflowRequest,
        context: &Map,
    ) -> Result<(), EngineError> {
        let mut req = (*child_req).clone();
        for (k, v) in &step.payload {
            req.context.insert(k.clone(), v.clone());
        }
        req.context.insert(
            PARENT_CONTEXT_KEY.into(),
            serde_json::to_value(context).unwrap_or(Json::Null),
        );
        let child = self.build_workflow(
            req,
            wf.tenant_id.clone(),
            Some((wf.id.clone(), step.name.clone())),
        );
        if !self.store.create_child_workflow(&child).await? {
            return Ok(()); // lost the claim: another advance spawned it
        }
        let _ = self
            .store
            .set_workflow_status(&child.id, WorkflowStatus::Running, None)
            .await?;
        EngineStats::incr(&self.stats.workflows_created);
        Box::pin(self.advance(&child.id)).await
    }
}
