//! HTTP router assembly.

use axum::http::header;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{middleware, Json, Router};
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;

use crate::auth::{bearer_auth, worker_auth};
use crate::handlers;
use crate::openapi::ApiDoc;
use crate::ApiState;

/// Build the full application router for the given engine state.
pub fn build_router(state: ApiState) -> Router {
    // Tenant surface: requires a tenant bearer token.
    let tenant = Router::new()
        .route("/jobs", post(handlers::create_job).get(handlers::list_jobs))
        .route("/jobs/batch", post(handlers::create_batch_jobs))
        .route("/jobs/{id}", get(handlers::get_job))
        .route("/jobs/{id}/cancel", post(handlers::cancel_job))
        .route("/jobs/{id}/events", get(handlers::stream_job_events))
        .route(
            "/workflows",
            post(handlers::create_workflow).get(handlers::list_workflows),
        )
        .route("/workflows/{id}", get(handlers::get_workflow))
        .route("/workflows/{id}/cancel", post(handlers::cancel_workflow))
        .route(
            "/workflows/{id}/diagram",
            get(handlers::get_workflow_diagram),
        )
        .route(
            "/cron",
            post(handlers::create_cron).get(handlers::list_crons),
        )
        .route(
            "/cron/{id}",
            get(handlers::get_cron).delete(handlers::delete_cron),
        )
        .route("/cron/{id}/pause", post(handlers::pause_cron))
        .route("/cron/{id}/resume", post(handlers::resume_cron))
        .route("/dlq", get(handlers::list_dead_letters))
        .route("/dlq/{id}", get(handlers::get_dead_letter))
        .route("/dlq/{id}/replay", post(handlers::replay_dead_letter))
        .route("/tasks", get(handlers::list_tasks))
        .route("/stats", get(handlers::get_stats))
        .route_layer(middleware::from_fn_with_state(state.clone(), bearer_auth));

    // Worker protocol: lease / heartbeat / complete / fail. Workers execute
    // arbitrary tenants' jobs, so these routes take the worker credential,
    // not a tenant token.
    let worker = Router::new()
        .route("/queues/{queue}/lease", post(handlers::lease_jobs))
        .route("/jobs/{id}/complete", post(handlers::complete_job))
        .route("/jobs/{id}/fail", post(handlers::fail_job))
        .route("/jobs/{id}/heartbeat", post(handlers::heartbeat_job))
        .route_layer(middleware::from_fn_with_state(state.clone(), worker_auth));

    Router::new()
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/openapi.json", get(openapi_json))
        .route("/openapi.yaml", get(openapi_yaml))
        .route("/docs", get(docs_ui))
        .nest("/api/v1", tenant.merge(worker))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .layer(CompressionLayer::new())
        .with_state(state)
}

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

async fn openapi_yaml() -> impl IntoResponse {
    let yaml = ApiDoc::openapi().to_yaml().unwrap_or_default();
    ([(header::CONTENT_TYPE, "application/yaml")], yaml)
}

/// Minimal Swagger UI page (loaded from a CDN) pointed at `/openapi.json`.
async fn docs_ui() -> Html<&'static str> {
    Html(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QueueFlow API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.addEventListener('load', () => {
      window.ui = SwaggerUIBundle({ url: '/openapi.json', dom_id: '#swagger-ui', deepLinking: true });
    });
  </script>
</body>
</html>"#,
    )
}
