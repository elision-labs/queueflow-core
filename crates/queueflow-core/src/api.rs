//! Object-safe engine facade.
//!
//! The HTTP layer must not be generic over the storage/queue adapters, so the
//! generic [`Engine`] is erased behind `Arc<dyn JobApi>`. axum handlers depend
//! only on this trait.

use async_trait::async_trait;

use crate::domain::*;
use crate::engine::{Engine, EnqueueOptions};
use crate::error::EngineError;
use crate::ports::{JobStore, ListFilter, MessageQueue, Page};
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
    ) -> Result<String, EngineError>;

    async fn enqueue_batch(
        &self,
        jobs: Vec<(String, Map, Option<JobConfig>)>,
        tenant_id: Option<String>,
    ) -> Result<Vec<String>, EngineError>;

    async fn get_job(&self, id: &str) -> Result<Job, EngineError>;
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

    // Remote worker protocol.
    async fn lease_jobs(
        &self,
        queue: &str,
        count: usize,
        lease_secs: u32,
        wait_secs: u32,
    ) -> Result<Vec<LeasedJob>, EngineError>;
    async fn heartbeat_lease(
        &self,
        queue: &str,
        lease_id: i64,
        extend_secs: u32,
    ) -> Result<(), EngineError>;
    async fn complete_leased(
        &self,
        queue: &str,
        lease_id: i64,
        job_id: &str,
        result: Map,
    ) -> Result<(), EngineError>;
    async fn fail_leased(
        &self,
        queue: &str,
        lease_id: i64,
        job_id: &str,
        error: &str,
        retryable: bool,
    ) -> Result<(), EngineError>;

    async fn ping(&self) -> Result<(), EngineError>;
    fn is_running(&self) -> bool;
    fn registered_tasks(&self) -> Vec<String>;
    fn stats(&self) -> StatsSnapshot;
}

#[async_trait]
impl<JS, MQ> JobApi for Engine<JS, MQ>
where
    JS: JobStore + 'static,
    MQ: MessageQueue + 'static,
{
    async fn enqueue(
        &self,
        task_name: &str,
        payload: Map,
        config: Option<JobConfig>,
        queue: Option<String>,
        tenant_id: Option<String>,
        idempotency_key: Option<String>,
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
            },
        )
        .await
    }

    async fn enqueue_batch(
        &self,
        jobs: Vec<(String, Map, Option<JobConfig>)>,
        tenant_id: Option<String>,
    ) -> Result<Vec<String>, EngineError> {
        Engine::enqueue_batch(self, jobs, tenant_id).await
    }

    async fn get_job(&self, id: &str) -> Result<Job, EngineError> {
        Engine::get_job(self, id).await
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
        queue: &str,
        lease_id: i64,
        extend_secs: u32,
    ) -> Result<(), EngineError> {
        Engine::heartbeat_lease(self, queue, lease_id, extend_secs).await
    }

    async fn complete_leased(
        &self,
        queue: &str,
        lease_id: i64,
        job_id: &str,
        result: Map,
    ) -> Result<(), EngineError> {
        Engine::complete_leased(self, queue, lease_id, job_id, result).await
    }

    async fn fail_leased(
        &self,
        queue: &str,
        lease_id: i64,
        job_id: &str,
        error: &str,
        retryable: bool,
    ) -> Result<(), EngineError> {
        Engine::fail_leased(self, queue, lease_id, job_id, error, retryable).await
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
}
