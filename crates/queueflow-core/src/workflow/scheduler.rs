//! Runtime workflow orchestration.
//!
//! The scheduler turns a validated DAG into actual queued jobs, gates each step
//! on its dependencies, propagates per-step results through the workflow
//! context, applies failure policies, and aggregates the final workflow status.
//!
//! It is intentionally generic over the [`JobStore`] port so the full
//! orchestration is exercised by fast in-memory tests as well as the Postgres
//! adapter.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use crate::domain::*;
use crate::error::EngineError;
use crate::ports::{Clock, JobStore};
use crate::stats::EngineStats;
use crate::workflow::dag::DependencyGraph;

/// Schedules and advances workflows over the storage port.
pub struct WorkflowScheduler<JS> {
    store: Arc<JS>,
    clock: Arc<dyn Clock>,
    default_queue: String,
    stats: Arc<EngineStats>,
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
    ) -> Self {
        Self {
            store,
            clock,
            default_queue,
            stats,
        }
    }

    /// Validate, persist, and start a workflow. Returns the new workflow id.
    /// A cyclic or otherwise invalid DAG yields [`EngineError::Workflow`] (HTTP
    /// 400) and persists nothing.
    pub async fn create(
        &self,
        req: CreateWorkflowRequest,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError> {
        let now = self.clock.now();
        let wf = Workflow {
            id: Uuid::new_v4().to_string(),
            name: req.name,
            steps: req.steps,
            status: WorkflowStatus::Created,
            created_at: now,
            started_at: None,
            completed_at: None,
            context: req.context,
            metadata: req.metadata,
            tenant_id,
        };

        // Validate before persisting anything: the DAG shape and every
        // per-step config override.
        let roots: Vec<String> = {
            let graph = DependencyGraph::new(&wf.steps)?;
            graph.roots().iter().map(|s| s.to_string()).collect()
        };
        for step in &wf.steps {
            if let Some(cfg) = &step.config {
                cfg.validate()
                    .map_err(|e| EngineError::Validation(format!("step '{}': {e}", step.name)))?;
            }
        }

        self.store.create_workflow(&wf).await?;
        let _ = self
            .store
            .set_workflow_status(&wf.id, WorkflowStatus::Running)
            .await?;
        EngineStats::incr(&self.stats.workflows_created);

        for root in &roots {
            if let Some(step) = wf.steps.iter().find(|s| &s.name == root) {
                self.enqueue_step(&wf, step).await?;
            }
        }

        Ok(wf.id)
    }

    /// Called by a worker when a workflow-linked job completes successfully.
    ///
    /// Write ordering is load-bearing: the result is merged into the shared
    /// context *before* the step is marked `Completed`, so any observer that
    /// sees the step terminal is guaranteed a context read that already
    /// contains its result ([`Self::enqueue_step`] relies on this to hand
    /// fan-in steps a complete `_context`). A crash between the two writes
    /// leaves a terminal job under a non-terminal step, which the janitor's
    /// stalled-step sweep re-drives through this method (the merge is
    /// idempotent per step).
    pub async fn on_step_completed(
        &self,
        workflow_id: &str,
        step_name: &str,
        result: &Map,
    ) -> Result<(), EngineError> {
        let value = serde_json::to_value(result).unwrap_or(Json::Null);
        self.store
            .merge_workflow_context(workflow_id, step_name, &value)
            .await?;
        self.store
            .set_step_status(workflow_id, step_name, StepStatus::Completed, None)
            .await?;
        self.advance(workflow_id).await
    }

    /// Called by a worker when a workflow-linked job fails permanently.
    pub async fn on_step_failed(
        &self,
        workflow_id: &str,
        step_name: &str,
        error: &str,
    ) -> Result<(), EngineError> {
        let wf = self.store.get_workflow(workflow_id).await?;
        let policy = wf
            .steps
            .iter()
            .find(|s| s.name == step_name)
            .map(|s| s.on_failure)
            .unwrap_or(OnFailure::Halt);

        match policy {
            OnFailure::Halt => {
                self.store
                    .set_step_status(workflow_id, step_name, StepStatus::Failed, Some(error))
                    .await?;
                // Halt everything that has not already reached a terminal
                // state, live jobs included: an unclaimed sibling job would
                // otherwise stay claimable and start fresh work for a dead
                // workflow. A currently-running sibling keeps its handler
                // alive, but flipping its job to cancelled means every
                // lease-guarded outcome write (and the next heartbeat) is
                // refused, exactly as in `Engine::cancel_workflow`.
                for rec in self.store.workflow_step_statuses(workflow_id).await? {
                    if rec.name == step_name || rec.status.is_terminal() {
                        continue;
                    }
                    self.store
                        .set_step_status(
                            workflow_id,
                            &rec.name,
                            StepStatus::Cancelled,
                            Some("workflow halted by an upstream failure"),
                        )
                        .await?;
                    if let Some(job_id) = &rec.job_id {
                        self.store
                            .cancel_job_if_active(job_id, "workflow halted by an upstream failure")
                            .await?;
                    }
                }
                // Count the failure only if this call performed the transition
                // (a concurrent halt may have already finished the workflow).
                if self
                    .store
                    .set_workflow_status(workflow_id, WorkflowStatus::Failed)
                    .await?
                {
                    EngineStats::incr(&self.stats.workflows_failed);
                }
                Ok(())
            }
            OnFailure::Skip => {
                self.store
                    .set_step_status(workflow_id, step_name, StepStatus::Skipped, Some(error))
                    .await?;
                self.advance(workflow_id).await
            }
            OnFailure::Continue => {
                self.store
                    .set_step_status(workflow_id, step_name, StepStatus::Failed, Some(error))
                    .await?;
                self.advance(workflow_id).await
            }
        }
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
            let status_by_name: HashMap<&str, StepStatus> = records
                .iter()
                .map(|r| (r.name.as_str(), r.status))
                .collect();
            let scheduled: HashSet<&str> = records
                .iter()
                .filter(|r| r.job_id.is_some())
                .map(|r| r.name.as_str())
                .collect();

            let mut changed = false;
            for step in &wf.steps {
                let name = step.name.as_str();
                let current = status_by_name
                    .get(name)
                    .copied()
                    .unwrap_or(StepStatus::Pending);
                if current != StepStatus::Pending || scheduled.contains(name) {
                    continue;
                }

                let deps_completed = step.depends_on.iter().all(|d| {
                    status_by_name.get(d.as_str()).copied() == Some(StepStatus::Completed)
                });
                if step.depends_on.is_empty() || deps_completed {
                    self.enqueue_step(&wf, step).await?;
                    changed = true;
                } else {
                    let dep_unsatisfiable = step.depends_on.iter().any(|d| {
                        matches!(
                            status_by_name.get(d.as_str()).copied(),
                            Some(StepStatus::Skipped)
                                | Some(StepStatus::Failed)
                                | Some(StepStatus::Cancelled)
                        )
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

        self.aggregate(workflow_id).await
    }

    /// Recompute and persist the workflow's aggregate status when all steps are
    /// terminal.
    async fn aggregate(&self, workflow_id: &str) -> Result<(), EngineError> {
        // Don't override an already-decided (terminal) workflow.
        if self
            .store
            .get_workflow(workflow_id)
            .await?
            .status
            .is_terminal()
        {
            return Ok(());
        }

        let records = self.store.workflow_step_statuses(workflow_id).await?;
        if !records.iter().all(|r| r.status.is_terminal()) {
            return Ok(());
        }

        let count = |s: StepStatus| records.iter().filter(|r| r.status == s).count();
        let completed = count(StepStatus::Completed);
        let skipped = count(StepStatus::Skipped);
        let cancelled = count(StepStatus::Cancelled);
        let failed = count(StepStatus::Failed);

        let status = if failed == 0 && skipped == 0 && cancelled == 0 {
            WorkflowStatus::Completed
        } else if completed == 0 && skipped == 0 {
            WorkflowStatus::Failed
        } else {
            WorkflowStatus::PartiallyFailed
        };

        // The guarded transition returns false when another worker's aggregate
        // already finalized the workflow, so each terminal transition is
        // counted exactly once.
        if self.store.set_workflow_status(workflow_id, status).await? {
            match status {
                WorkflowStatus::Completed => EngineStats::incr(&self.stats.workflows_completed),
                _ => EngineStats::incr(&self.stats.workflows_failed),
            }
        }
        Ok(())
    }

    /// Create a job for a single step, injecting the workflow context. The
    /// insert *is* the publish, so the only ordering that matters is the
    /// step->job claim below.
    async fn enqueue_step(
        &self,
        wf: &Workflow,
        step: &WorkflowStep,
    ) -> Result<String, EngineError> {
        // Read the context *after* the caller observed the dependency
        // statuses that made this step ready. Combined with the
        // merge-before-status ordering in [`Self::on_step_completed`], seeing
        // a dependency `Completed` guarantees its result is already visible
        // to this read, so a fan-in step can never be scheduled with a
        // partial `_context`.
        let context = self.store.get_workflow(&wf.id).await?.context;

        let mut payload = step.payload.clone();
        if !context.is_empty() {
            payload.insert(
                CONTEXT_KEY.to_string(),
                serde_json::to_value(&context).unwrap_or(Json::Null),
            );
        }

        let now = self.clock.now();
        let job = Job {
            id: Uuid::new_v4().to_string(),
            queue_name: self.default_queue.clone(),
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
            metadata: step.metadata.clone(),
            tenant_id: wf.tenant_id.clone(),
            idempotency_key: None,
        };

        // CLAIM the step and create its job in one atomic store operation.
        // Two workers completing sibling steps concurrently both run `advance`
        // and can both see a join step as unscheduled; the claim guarantees
        // exactly one job is ever persisted. Atomicity matters because the
        // insert is the publish: a separately-inserted loser row would be
        // claimable by a worker before it could be retired.
        let claimed = self.store.create_step_job(&job).await?;
        if !claimed {
            // Lost the race: nothing was persisted.
            return Ok(job.id);
        }
        EngineStats::incr(&self.stats.jobs_created);
        Ok(job.id)
    }
}
