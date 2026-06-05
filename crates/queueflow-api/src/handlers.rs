//! axum handlers. Each is annotated with `#[utoipa::path]` so the OpenAPI spec
//! is generated from the same source of truth that serves the requests.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use queueflow_core::{CreateWorkflowRequest, EngineError, Job, Workflow};

use crate::auth::Tenant;
use crate::dto::*;
use crate::error::ApiError;
use crate::ApiState;

const MAX_BATCH: usize = 1000;

/// Enforce tenant ownership of a resource. A resource with no tenant is treated
/// as accessible (e.g. created out-of-band); a mismatched tenant is forbidden.
fn ensure_owner(owner: Option<&str>, tenant: &Tenant) -> Result<(), ApiError> {
    match owner {
        Some(o) if o != tenant.0 => Err(EngineError::Forbidden.into()),
        _ => Ok(()),
    }
}

// ---- Jobs ------------------------------------------------------------------

#[utoipa::path(
    post, path = "/api/v1/jobs", tag = "jobs", operation_id = "createJob",
    request_body = CreateJobRequest,
    responses(
        (status = 201, description = "Job created", body = CreateJobResponse),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 401, description = "Unauthorized", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn create_job(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Json(req): Json<CreateJobRequest>,
) -> Result<Response, ApiError> {
    if req.task_name.trim().is_empty() {
        return Err(EngineError::Validation("task_name is required".into()).into());
    }
    let (config, queue) = req.config.unwrap_or_default().resolve();
    let id = s
        .engine
        .enqueue(&req.task_name, req.payload, config, queue, Some(t.0))
        .await?;
    Ok((StatusCode::CREATED, Json(CreateJobResponse { job_id: id })).into_response())
}

#[utoipa::path(
    post, path = "/api/v1/jobs/batch", tag = "jobs", operation_id = "createBatchJobs",
    request_body = CreateBatchJobsRequest,
    responses(
        (status = 201, description = "Jobs created", body = CreateBatchJobsResponse),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 401, description = "Unauthorized", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn create_batch_jobs(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Json(req): Json<CreateBatchJobsRequest>,
) -> Result<Response, ApiError> {
    if req.jobs.is_empty() {
        return Err(EngineError::Validation("jobs array cannot be empty".into()).into());
    }
    if req.jobs.len() > MAX_BATCH {
        return Err(
            EngineError::Validation(format!("batch size cannot exceed {MAX_BATCH}")).into(),
        );
    }
    let jobs = req
        .jobs
        .into_iter()
        .map(|j| {
            let (config, _queue) = j.config.unwrap_or_default().resolve();
            (j.task_name, j.payload, config)
        })
        .collect();
    let ids = s.engine.enqueue_batch(jobs, Some(t.0)).await?;
    let count = ids.len();
    Ok((
        StatusCode::CREATED,
        Json(CreateBatchJobsResponse {
            job_ids: ids,
            count,
        }),
    )
        .into_response())
}

#[utoipa::path(
    get, path = "/api/v1/jobs", tag = "jobs", operation_id = "listJobs",
    params(ListQuery),
    responses(
        (status = 200, description = "Page of jobs", body = ListJobsResponse),
        (status = 401, description = "Unauthorized", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn list_jobs(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Query(q): Query<ListQuery>,
) -> Result<Json<ListJobsResponse>, ApiError> {
    let filter = q.into_filter(Some(t.0));
    let (jobs, total) = s.engine.list_jobs(filter.clone()).await?;
    let has_more = filter.offset + (jobs.len() as i64) < total;
    Ok(Json(ListJobsResponse {
        jobs,
        total,
        limit: filter.limit,
        offset: filter.offset,
        has_more,
    }))
}

#[utoipa::path(
    get, path = "/api/v1/jobs/{id}", tag = "jobs", operation_id = "getJob",
    params(("id" = String, Path, description = "Job id")),
    responses(
        (status = 200, description = "Job", body = Job),
        (status = 403, description = "Forbidden", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn get_job(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Path(id): Path<String>,
) -> Result<Json<queueflow_core::Job>, ApiError> {
    let job = s.engine.get_job(&id).await?;
    if let Some(owner) = &job.tenant_id {
        if owner != &t.0 {
            return Err(EngineError::Forbidden.into());
        }
    }
    Ok(Json(job))
}

#[utoipa::path(
    post, path = "/api/v1/jobs/{id}/cancel", tag = "jobs", operation_id = "cancelJob",
    params(("id" = String, Path, description = "Job id")),
    responses(
        (status = 204, description = "Cancelled"),
        (status = 401, description = "Unauthorized", body = ErrorBody),
        (status = 403, description = "Forbidden", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn cancel_job(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    // Verify ownership before cancelling (404 if missing, 403 if another tenant).
    let job = s.engine.get_job(&id).await?;
    ensure_owner(job.tenant_id.as_deref(), &t)?;
    s.engine.cancel_job(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Workflows (implemented; the Go version returned 501 for all of these) --

#[utoipa::path(
    post, path = "/api/v1/workflows", tag = "workflows", operation_id = "createWorkflow",
    request_body = CreateWorkflowRequest,
    responses(
        (status = 201, description = "Workflow created", body = CreateWorkflowResponse),
        (status = 400, description = "Invalid workflow (e.g. dependency cycle)", body = ErrorBody),
        (status = 401, description = "Unauthorized", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn create_workflow(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Json(req): Json<CreateWorkflowRequest>,
) -> Result<Response, ApiError> {
    if req.steps.is_empty() {
        return Err(EngineError::Validation("a workflow needs at least one step".into()).into());
    }
    let id = s.engine.create_workflow(req, Some(t.0)).await?;
    Ok((
        StatusCode::CREATED,
        Json(CreateWorkflowResponse { workflow_id: id }),
    )
        .into_response())
}

#[utoipa::path(
    get, path = "/api/v1/workflows", tag = "workflows", operation_id = "listWorkflows",
    params(ListQuery),
    responses(
        (status = 200, description = "Page of workflows", body = ListWorkflowsResponse),
        (status = 401, description = "Unauthorized", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn list_workflows(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Query(q): Query<ListQuery>,
) -> Result<Json<ListWorkflowsResponse>, ApiError> {
    let filter = q.into_filter(Some(t.0));
    let (workflows, total) = s.engine.list_workflows(filter.clone()).await?;
    let has_more = filter.offset + (workflows.len() as i64) < total;
    Ok(Json(ListWorkflowsResponse {
        workflows,
        total,
        limit: filter.limit,
        offset: filter.offset,
        has_more,
    }))
}

#[utoipa::path(
    get, path = "/api/v1/workflows/{id}", tag = "workflows", operation_id = "getWorkflow",
    params(("id" = String, Path, description = "Workflow id")),
    responses(
        (status = 200, description = "Workflow", body = Workflow),
        (status = 403, description = "Forbidden", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn get_workflow(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Path(id): Path<String>,
) -> Result<Json<queueflow_core::Workflow>, ApiError> {
    let wf = s.engine.get_workflow(&id).await?;
    if let Some(owner) = &wf.tenant_id {
        if owner != &t.0 {
            return Err(EngineError::Forbidden.into());
        }
    }
    Ok(Json(wf))
}

#[utoipa::path(
    post, path = "/api/v1/workflows/{id}/cancel", tag = "workflows", operation_id = "cancelWorkflow",
    params(("id" = String, Path, description = "Workflow id")),
    responses(
        (status = 204, description = "Cancelled"),
        (status = 401, description = "Unauthorized", body = ErrorBody),
        (status = 403, description = "Forbidden", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn cancel_workflow(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let wf = s.engine.get_workflow(&id).await?;
    ensure_owner(wf.tenant_id.as_deref(), &t)?;
    s.engine.cancel_workflow(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get, path = "/api/v1/workflows/{id}/diagram", tag = "workflows", operation_id = "getWorkflowDiagram",
    params(("id" = String, Path, description = "Workflow id")),
    responses(
        (status = 200, description = "Mermaid diagram of the workflow DAG", body = WorkflowDiagramResponse),
        (status = 401, description = "Unauthorized", body = ErrorBody),
        (status = 403, description = "Forbidden", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn get_workflow_diagram(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowDiagramResponse>, ApiError> {
    // Validate ownership before exposing the workflow's structure.
    let wf = s.engine.get_workflow(&id).await?;
    ensure_owner(wf.tenant_id.as_deref(), &t)?;
    let diagram = s.engine.workflow_diagram(&id).await?;
    Ok(Json(WorkflowDiagramResponse {
        format: "mermaid".into(),
        diagram,
    }))
}

// ---- System ----------------------------------------------------------------

#[utoipa::path(
    get, path = "/api/v1/tasks", tag = "system", operation_id = "listTasks",
    responses((status = 200, description = "Registered task handlers", body = TasksResponse)),
    security(("bearerAuth" = []))
)]
pub async fn list_tasks(State(s): State<ApiState>) -> Json<TasksResponse> {
    Json(TasksResponse {
        tasks: s.engine.registered_tasks(),
    })
}

#[utoipa::path(
    get, path = "/api/v1/stats", tag = "system", operation_id = "getStats",
    responses((status = 200, description = "Engine counters", body = queueflow_core::StatsSnapshot)),
    security(("bearerAuth" = []))
)]
pub async fn get_stats(State(s): State<ApiState>) -> Json<queueflow_core::StatsSnapshot> {
    Json(s.engine.stats())
}

// ---- Health (no auth) ------------------------------------------------------

#[utoipa::path(
    get, path = "/health", tag = "health", operation_id = "getHealth",
    responses(
        (status = 200, description = "Healthy", body = HealthStatus),
        (status = 503, description = "Database unavailable", body = ErrorBody),
    )
)]
pub async fn health(State(s): State<ApiState>) -> Response {
    match s.engine.ping().await {
        Ok(_) => (StatusCode::OK, Json(HealthStatus::ok())).into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody::new("database unavailable")),
        )
            .into_response(),
    }
}

#[utoipa::path(
    get, path = "/ready", tag = "health", operation_id = "getReady",
    responses(
        (status = 200, description = "Ready", body = ReadyStatus),
        (status = 503, description = "Not ready", body = ErrorBody),
    )
)]
pub async fn ready(State(s): State<ApiState>) -> Response {
    if !s.engine.is_running() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody::new("workers not running")),
        )
            .into_response();
    }
    match s.engine.ping().await {
        Ok(_) => (
            StatusCode::OK,
            Json(ReadyStatus {
                status: "ready".into(),
            }),
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody::new("database not ready")),
        )
            .into_response(),
    }
}
