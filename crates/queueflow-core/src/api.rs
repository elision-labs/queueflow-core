//! Object-safe engine facade.
//!
//! The HTTP layer must not be generic over the storage adapter, so the
//! generic [`Engine`] is erased behind `Arc<dyn JobApi>`. axum handlers depend
//! only on this trait.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use std::time::Duration;

use crate::domain::*;
use crate::engine::{BatchItem, Engine, EnqueueOptions};
use crate::error::EngineError;
use crate::ports::{JobStore, ListFilter, Page, StepRecord};
use crate::stats::StatsSnapshot;

/// The operations the HTTP API needs from the engine.
#[async_trait]
pub trait JobApi: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn enqueue(
        &self,
        task_name: &str,
        payload: Map,
        config: Option<JobConfig>,
        queue: Option<String>,
        tenant_id: Option<String>,
        idempotency_key: Option<String>,
        run_at: Option<DateTime<Utc>>,
    ) -> Result<String, EngineError>;

    async fn enqueue_batch(
        &self,
        jobs: Vec<BatchItem>,
        tenant_id: Option<String>,
    ) -> Result<Vec<String>, EngineError>;

    async fn get_job(&self, id: &str) -> Result<Job, EngineError>;

    /// Park until the job may have changed status (or `max_wait` passes).
    /// May wake spuriously; callers re-read the job and loop.
    async fn await_job_change(&self, id: &str, max_wait: Duration) -> Result<(), EngineError>;
    async fn list_jobs(&self, filter: ListFilter) -> Result<Page<Job>, EngineError>;
    async fn cancel_job(&self, id: &str) -> Result<(), EngineError>;

    async fn create_workflow(
        &self,
        req: CreateWorkflowRequest,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError>;
    async fn get_workflow(&self, id: &str) -> Result<Workflow, EngineError>;
    async fn list_workflows(&self, filter: ListFilter) -> Result<Page<Workflow>, EngineError>;
    async fn cancel_workflow(&self, id: &str) -> Result<(), EngineError>;
    async fn workflow_diagram(&self, id: &str) -> Result<String, EngineError>;
    /// Runtime status of every step, in declaration order (the live
    /// progress view; the workflow record itself carries only definitions).
    async fn workflow_step_statuses(&self, id: &str) -> Result<Vec<StepRecord>, EngineError>;

    // Dead-letter admin: inspect and replay terminally-failed jobs.
    async fn list_dead_letters(&self, filter: ListFilter) -> Result<Page<DeadLetter>, EngineError>;
    async fn get_dead_letter(&self, id: i64) -> Result<DeadLetter, EngineError>;
    async fn replay_dead_letter(&self, id: i64) -> Result<String, EngineError>;

    // Cron schedules: recurring enqueues.
    async fn create_cron(
        &self,
        req: CreateCronRequest,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError>;
    async fn get_cron(&self, id: &str) -> Result<CronSchedule, EngineError>;
    async fn list_crons(&self, filter: ListFilter) -> Result<Page<CronSchedule>, EngineError>;
    async fn delete_cron(&self, id: &str) -> Result<(), EngineError>;
    async fn set_cron_enabled(&self, id: &str, enabled: bool) -> Result<(), EngineError>;

    // Remote worker protocol. Lease ownership rides on the lease token.
    async fn lease_jobs(
        &self,
        queue: &str,
        count: usize,
        lease_secs: u32,
        wait_secs: u32,
    ) -> Result<Vec<LeasedJob>, EngineError>;
    async fn heartbeat_lease(
        &self,
        job_id: &str,
        lease_token: &str,
        extend_secs: u32,
    ) -> Result<JobStatus, EngineError>;
    async fn complete_leased(
        &self,
        job_id: &str,
        lease_token: &str,
        result: Map,
    ) -> Result<(), EngineError>;
    async fn fail_leased(
        &self,
        job_id: &str,
        lease_token: &str,
        error: &str,
        retryable: bool,
    ) -> Result<(), EngineError>;

    async fn ping(&self) -> Result<(), EngineError>;
    fn is_running(&self) -> bool;
    fn registered_tasks(&self) -> Vec<String>;
    /// Process-local engine counters (reset on restart, not tenant-scoped).
    /// Meant for the metrics endpoint, not for tenant-facing responses.
    fn stats(&self) -> StatsSnapshot;
    /// Durable counts scoped to `tenant_id` (see
    /// [`crate::JobStore::count_stats`]); this is what `/api/v1/stats` serves.
    async fn tenant_stats(&self, tenant_id: Option<&str>) -> Result<StatsSnapshot, EngineError>;
}

#[async_trait]
impl<JS> JobApi for Engine<JS>
where
    JS: JobStore + 'static,
{
    async fn enqueue(
        &self,
        task_name: &str,
        payload: Map,
        config: Option<JobConfig>,
        queue: Option<String>,
        tenant_id: Option<String>,
        idempotency_key: Option<String>,
        run_at: Option<DateTime<Utc>>,
    ) -> Result<String, EngineError> {
        Engine::enqueue(
            self,
            task_name,
            payload,
            EnqueueOptions {
                config,
                queue,
                tenant_id,
                metadata: Map::new(),
                idempotency_key,
                run_at,
            },
        )
        .await
    }

    async fn enqueue_batch(
        &self,
        jobs: Vec<BatchItem>,
        tenant_id: Option<String>,
    ) -> Result<Vec<String>, EngineError> {
        Engine::enqueue_batch(self, jobs, tenant_id).await
    }

    async fn get_job(&self, id: &str) -> Result<Job, EngineError> {
        Engine::get_job(self, id).await
    }

    async fn await_job_change(&self, id: &str, max_wait: Duration) -> Result<(), EngineError> {
        Engine::await_job_change(self, id, max_wait).await
    }

    async fn list_jobs(&self, filter: ListFilter) -> Result<Page<Job>, EngineError> {
        Engine::list_jobs(self, &filter).await
    }

    async fn cancel_job(&self, id: &str) -> Result<(), EngineError> {
        Engine::cancel_job(self, id).await
    }

    async fn create_workflow(
        &self,
        req: CreateWorkflowRequest,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError> {
        Engine::create_workflow(self, req, tenant_id).await
    }

    async fn get_workflow(&self, id: &str) -> Result<Workflow, EngineError> {
        Engine::get_workflow(self, id).await
    }

    async fn list_workflows(&self, filter: ListFilter) -> Result<Page<Workflow>, EngineError> {
        Engine::list_workflows(self, &filter).await
    }

    async fn cancel_workflow(&self, id: &str) -> Result<(), EngineError> {
        Engine::cancel_workflow(self, id).await
    }

    async fn workflow_diagram(&self, id: &str) -> Result<String, EngineError> {
        Engine::workflow_diagram(self, id).await
    }

    async fn workflow_step_statuses(&self, id: &str) -> Result<Vec<StepRecord>, EngineError> {
        Engine::workflow_step_statuses(self, id).await
    }

    async fn create_cron(
        &self,
        req: CreateCronRequest,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError> {
        Engine::create_cron(self, req, tenant_id).await
    }

    async fn get_cron(&self, id: &str) -> Result<CronSchedule, EngineError> {
        Engine::get_cron(self, id).await
    }

    async fn list_crons(&self, filter: ListFilter) -> Result<Page<CronSchedule>, EngineError> {
        Engine::list_crons(self, &filter).await
    }

    async fn delete_cron(&self, id: &str) -> Result<(), EngineError> {
        Engine::delete_cron(self, id).await
    }

    async fn set_cron_enabled(&self, id: &str, enabled: bool) -> Result<(), EngineError> {
        Engine::set_cron_enabled(self, id, enabled).await
    }

    async fn list_dead_letters(&self, filter: ListFilter) -> Result<Page<DeadLetter>, EngineError> {
        Engine::list_dead_letters(self, &filter).await
    }

    async fn get_dead_letter(&self, id: i64) -> Result<DeadLetter, EngineError> {
        Engine::get_dead_letter(self, id).await
    }

    async fn replay_dead_letter(&self, id: i64) -> Result<String, EngineError> {
        Engine::replay_dead_letter(self, id).await
    }

    async fn lease_jobs(
        &self,
        queue: &str,
        count: usize,
        lease_secs: u32,
        wait_secs: u32,
    ) -> Result<Vec<LeasedJob>, EngineError> {
        Engine::lease_jobs(self, queue, count, lease_secs, wait_secs).await
    }

    async fn heartbeat_lease(
        &self,
        job_id: &str,
        lease_token: &str,
        extend_secs: u32,
    ) -> Result<JobStatus, EngineError> {
        Engine::heartbeat_lease(self, job_id, lease_token, extend_secs).await
    }

    async fn complete_leased(
        &self,
        job_id: &str,
        lease_token: &str,
        result: Map,
    ) -> Result<(), EngineError> {
        Engine::complete_leased(self, job_id, lease_token, result).await
    }

    async fn fail_leased(
        &self,
        job_id: &str,
        lease_token: &str,
        error: &str,
        retryable: bool,
    ) -> Result<(), EngineError> {
        Engine::fail_leased(self, job_id, lease_token, error, retryable).await
    }

    async fn ping(&self) -> Result<(), EngineError> {
        Engine::ping(self).await
    }

    fn is_running(&self) -> bool {
        Engine::is_running(self)
    }

    fn registered_tasks(&self) -> Vec<String> {
        Engine::registered_tasks(self)
    }

    fn stats(&self) -> StatsSnapshot {
        Engine::stats(self).snapshot()
    }

    async fn tenant_stats(&self, tenant_id: Option<&str>) -> Result<StatsSnapshot, EngineError> {
        Engine::tenant_stats(self, tenant_id).await
    }
}
