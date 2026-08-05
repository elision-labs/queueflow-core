//! The single source of truth for the OpenAPI 3.1 document, generated from the
//! handler annotations and domain types via utoipa. The `queueflow spec`
//! subcommand serializes [`ApiDoc`] to `openapi.json`/`openapi.yaml`, which the
//! SDK generators consume — so the spec can never drift from the code.

use queueflow_core::{
    BackoffStrategy, CreateCronRequest, CreateWorkflowRequest, CronSchedule, DeadLetter, Job,
    JobConfig, JobStatus, LeasedJob, OnFailure, OnSuccess, StatsSnapshot, Workflow, WorkflowStatus,
    WorkflowStep,
};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::dto::*;
use crate::handlers;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearerAuth",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("API Key")
                        .build(),
                ),
            );
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "QueueFlow API",
        version = "1.0.0",
        description = "REST API for QueueFlow, a PostgreSQL-native distributed job queue and workflow engine.",
        license(name = "MIT", url = "https://opensource.org/licenses/MIT"),
        contact(name = "QueueFlow", url = "https://queueflow.dev"),
    ),
    servers(
        (url = "http://localhost:8000", description = "Local development"),
        (url = "https://api.queueflow.dev", description = "Production"),
    ),
    paths(
        handlers::create_job,
        handlers::create_batch_jobs,
        handlers::list_jobs,
        handlers::get_job,
        handlers::cancel_job,
        handlers::stream_job_events,
        handlers::lease_jobs,
        handlers::complete_job,
        handlers::fail_job,
        handlers::heartbeat_job,
        handlers::create_workflow,
        handlers::list_workflows,
        handlers::get_workflow,
        handlers::cancel_workflow,
        handlers::get_workflow_diagram,
        handlers::create_cron,
        handlers::list_crons,
        handlers::get_cron,
        handlers::delete_cron,
        handlers::pause_cron,
        handlers::resume_cron,
        handlers::list_dead_letters,
        handlers::get_dead_letter,
        handlers::replay_dead_letter,
        handlers::list_tasks,
        handlers::get_stats,
        handlers::health,
        handlers::ready,
    ),
    components(schemas(
        Job,
        JobConfig,
        JobStatus,
        BackoffStrategy,
        Workflow,
        WorkflowStep,
        WorkflowStatus,
        OnFailure,
        OnSuccess,
        CreateWorkflowRequest,
        StatsSnapshot,
        LeasedJob,
        LeaseJobsRequest,
        LeaseJobsResponse,
        CompleteJobRequest,
        FailJobRequest,
        HeartbeatRequest,
        HeartbeatResponse,
        JobConfigRequest,
        CreateJobRequest,
        CreateJobResponse,
        CreateBatchJobsRequest,
        CreateBatchJobsResponse,
        ListJobsResponse,
        ListWorkflowsResponse,
        CreateWorkflowResponse,
        DeadLetter,
        ListDeadLettersResponse,
        ReplayDeadLetterResponse,
        CronSchedule,
        CreateCronRequest,
        CreateCronResponse,
        ListCronsResponse,
        WorkflowDiagramResponse,
        TasksResponse,
        ErrorBody,
        HealthStatus,
        ReadyStatus,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "health", description = "Liveness and readiness probes"),
        (name = "jobs", description = "Job lifecycle"),
        (name = "workflows", description = "Workflow orchestration (DAG of steps)"),
        (name = "cron", description = "Recurring enqueues on a cron schedule (UTC)"),
        (name = "dlq", description = "Dead-letter queue: inspect terminally-failed jobs and replay them as fresh jobs"),
        (name = "worker", description = "Remote worker protocol: lease jobs, heartbeat, report completion/failure. \
                                         Lets handlers run in any language, outside the server binary."),
        (name = "system", description = "Introspection and metrics"),
    )
)]
pub struct ApiDoc;
