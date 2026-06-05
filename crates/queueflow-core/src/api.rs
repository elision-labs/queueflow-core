//! Object-safe engine facade.
//!
//! The HTTP layer must not be generic over the storage/queue adapters, so the
//! generic [`Engine`] is erased behind `Arc<dyn JobApi>`. axum handlers depend
//! only on this trait.

use async_trait::async_trait;

use crate::domain::*;
use crate::engine::{Engine, EnqueueOptions};
use crate::error::EngineError;
use crate::ports::{JobStore, ListFilter, MessageQueue};
use crate::stats::StatsSnapshot;

/// The operations the HTTP API needs from the engine.
#[async_trait]
pub trait JobApi: Send + Sync {
    async fn enqueue(
        &self,
        task_name: &str,
        payload: Map,
        config: Option<JobConfig>,
        queue: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError>;

    async fn enqueue_batch(
        &self,
        jobs: Vec<(String, Map, Option<JobConfig>)>,
        tenant_id: Option<String>,
    ) -> Result<Vec<String>, EngineError>;

    async fn get_job(&self, id: &str) -> Result<Job, EngineError>;
    async fn list_jobs(&self, filter: ListFilter) -> Result<(Vec<Job>, i64), EngineError>;
    async fn cancel_job(&self, id: &str) -> Result<(), EngineError>;

    async fn create_workflow(
        &self,
        req: CreateWorkflowRequest,
        tenant_id: Option<String>,
    ) -> Result<String, EngineError>;
    async fn get_workflow(&self, id: &str) -> Result<Workflow, EngineError>;
    async fn list_workflows(&self, filter: ListFilter)
        -> Result<(Vec<Workflow>, i64), EngineError>;
    async fn cancel_workflow(&self, id: &str) -> Result<(), EngineError>;
    async fn workflow_diagram(&self, id: &str) -> Result<String, EngineError>;

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

    async fn list_jobs(&self, filter: ListFilter) -> Result<(Vec<Job>, i64), EngineError> {
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

    async fn list_workflows(
        &self,
        filter: ListFilter,
    ) -> Result<(Vec<Workflow>, i64), EngineError> {
        Engine::list_workflows(self, &filter).await
    }

    async fn cancel_workflow(&self, id: &str) -> Result<(), EngineError> {
        Engine::cancel_workflow(self, id).await
    }

    async fn workflow_diagram(&self, id: &str) -> Result<String, EngineError> {
        Engine::workflow_diagram(self, id).await
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
