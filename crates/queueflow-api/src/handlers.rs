//! axum handlers. Each is annotated with `#[utoipa::path]` so the OpenAPI spec
//! is generated from the same source of truth that serves the requests.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures::stream::Stream;
use queueflow_core::{CreateWorkflowRequest, EngineError, Job, Workflow};

use crate::auth::Tenant;
use crate::dto::*;
use crate::error::ApiError;
use crate::ApiState;

const MAX_BATCH: usize = 1000;

/// How often the SSE job stream re-reads the job, and its maximum lifetime.
const SSE_POLL_INTERVAL: Duration = Duration::from_millis(500);
const SSE_MAX_LIFETIME: Duration = Duration::from_secs(15 * 60);

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
    params(
        ("Idempotency-Key" = Option<String>, Header,
         description = "Optional client-supplied key making this create idempotent per tenant: \
                        retrying with the same key returns the original job instead of creating a duplicate."),
    ),
    responses(
        (status = 201, description = "Job created (or replayed idempotently)", body = CreateJobResponse),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 401, description = "Unauthorized", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn create_job(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    headers: HeaderMap,
    Json(req): Json<CreateJobRequest>,
) -> Result<Response, ApiError> {
    if req.task_name.trim().is_empty() {
        return Err(EngineError::Validation("task_name is required".into()).into());
    }
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(String::from);
    let (config, queue) = req.config.unwrap_or_default().resolve();
    let id = s
        .engine
        .enqueue(
            &req.task_name,
            req.payload,
            config,
            queue,
            Some(t.0),
            idempotency_key,
            req.run_at,
        )
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
    let page = s.engine.list_jobs(filter.clone()).await?;
    Ok(Json(ListJobsResponse {
        jobs: page.items,
        total: page.total,
        limit: filter.limit,
        offset: filter.offset,
        has_more: page.has_more,
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

// ---- Workflows -------------------------------------------------------------

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
    let page = s.engine.list_workflows(filter.clone()).await?;
    Ok(Json(ListWorkflowsResponse {
        workflows: page.items,
        total: page.total,
        limit: filter.limit,
        offset: filter.offset,
        has_more: page.has_more,
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

/// Stream a job's status transitions as Server-Sent Events until it reaches a
/// terminal state. Lets clients await completion without polling the REST
/// endpoint themselves.
#[utoipa::path(
    get, path = "/api/v1/jobs/{id}/events", tag = "jobs", operation_id = "streamJobEvents",
    params(("id" = String, Path, description = "Job id")),
    responses(
        (status = 200, description = "SSE stream; each `status` event carries the full job JSON. \
                                      Closes after the job reaches a terminal state.",
         content_type = "text/event-stream", body = String),
        (status = 401, description = "Unauthorized", body = ErrorBody),
        (status = 403, description = "Forbidden", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn stream_job_events(
    State(s): State<ApiState>,
    Extension(t): Extension<Tenant>,
    Path(id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // Validate existence and ownership up front so errors are proper HTTP
    // statuses rather than a silently empty stream.
    let job = s.engine.get_job(&id).await?;
    ensure_owner(job.tenant_id.as_deref(), &t)?;

    let engine = s.engine.clone();
    let stream = futures::stream::unfold(
        (engine, id, None::<String>, std::time::Instant::now(), false),
        |(engine, id, last_status, started, done)| async move {
            if done || started.elapsed() > SSE_MAX_LIFETIME {
                return None;
            }
            loop {
                match engine.get_job(&id).await {
                    Ok(job) => {
                        let status = job.status.as_str().to_string();
                        if last_status.as_deref() != Some(&status) {
                            let event = Event::default()
                                .event("status")
                                .data(serde_json::to_string(&job).unwrap_or_default());
                            let terminal = job.status.is_terminal();
                            return Some((
                                Ok(event),
                                (engine, id, Some(status), started, terminal),
                            ));
                        }
                    }
                    Err(_) => return None, // job deleted: end the stream
                }
                if started.elapsed() > SSE_MAX_LIFETIME {
                    return None;
                }
                tokio::time::sleep(SSE_POLL_INTERVAL).await;
            }
        },
    );
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

// ---- Remote worker protocol --------------------------------------------------
//
// Workers in any language lease jobs, heartbeat while running, and report
// completion/failure. Leasing is queue-scoped, not tenant-scoped: workers are
// deployment infrastructure (they execute arbitrary tenants' jobs), unlike the
// tenant-scoped producer endpoints above.

#[utoipa::path(
    post, path = "/api/v1/queues/{queue}/lease", tag = "worker", operation_id = "leaseJobs",
    params(("queue" = String, Path, description = "Queue to lease from")),
    request_body = LeaseJobsRequest,
    responses(
        (status = 200, description = "Zero or more leased jobs (empty if none became available within wait_secs)", body = LeaseJobsResponse),
        (status = 401, description = "Unauthorized", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn lease_jobs(
    State(s): State<ApiState>,
    Path(queue): Path<String>,
    Json(req): Json<LeaseJobsRequest>,
) -> Result<Json<LeaseJobsResponse>, ApiError> {
    let count = req.max_jobs.unwrap_or(1).clamp(1, 100);
    let lease_secs = req.lease_secs.unwrap_or(30).clamp(1, 3600);
    let wait_secs = req.wait_secs.unwrap_or(0).min(30);
    let jobs = s
        .engine
        .lease_jobs(&queue, count, lease_secs, wait_secs)
        .await?;
    Ok(Json(LeaseJobsResponse { jobs }))
}

#[utoipa::path(
    post, path = "/api/v1/jobs/{id}/complete", tag = "worker", operation_id = "completeJob",
    params(("id" = String, Path, description = "Job id")),
    request_body = CompleteJobRequest,
    responses(
        (status = 204, description = "Completed (idempotent: replaying against an already-finished job also succeeds)"),
        (status = 401, description = "Unauthorized", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
        (status = 409, description = "Lease no longer held (expired and reclaimed)", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn complete_job(
    State(s): State<ApiState>,
    Path(id): Path<String>,
    Json(req): Json<CompleteJobRequest>,
) -> Result<StatusCode, ApiError> {
    s.engine
        .complete_leased(&id, &req.lease_token, req.result)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post, path = "/api/v1/jobs/{id}/fail", tag = "worker", operation_id = "failJob",
    params(("id" = String, Path, description = "Job id")),
    request_body = FailJobRequest,
    responses(
        (status = 204, description = "Failure recorded; the job is retried or dead-lettered per its config"),
        (status = 401, description = "Unauthorized", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
        (status = 409, description = "Lease no longer held (expired and reclaimed)", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn fail_job(
    State(s): State<ApiState>,
    Path(id): Path<String>,
    Json(req): Json<FailJobRequest>,
) -> Result<StatusCode, ApiError> {
    s.engine
        .fail_leased(&id, &req.lease_token, &req.error, req.retryable)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post, path = "/api/v1/jobs/{id}/heartbeat", tag = "worker", operation_id = "heartbeatJob",
    params(("id" = String, Path, description = "Job id")),
    request_body = HeartbeatRequest,
    responses(
        (status = 200, description = "Current job status. `running` = lease extended; anything else \
                                      (e.g. `cancelled`) = not extended, stop working on the job.",
         body = HeartbeatResponse),
        (status = 401, description = "Unauthorized", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
        (status = 409, description = "Lease no longer held (expired and reclaimed)", body = ErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn heartbeat_job(
    State(s): State<ApiState>,
    Path(id): Path<String>,
    Json(req): Json<HeartbeatRequest>,
) -> Result<Json<HeartbeatResponse>, ApiError> {
    let extend = req.extend_secs.clamp(1, 3600);
    let status = s
        .engine
        .heartbeat_lease(&id, &req.lease_token, extend)
        .await?;
    Ok(Json(HeartbeatResponse { status }))
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
    responses((status = 200,
        description = "Engine counters. Process-local and reset on restart: in a split \
                       api/worker deployment this reflects only the process serving the \
                       request; query the database for fleet-wide history.",
        body = queueflow_core::StatsSnapshot)),
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
