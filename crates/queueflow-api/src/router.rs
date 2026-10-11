//! HTTP router assembly.

use std::sync::LazyLock;

use axum::http::{header, HeaderValue};
use axum::response::{Html, IntoResponse, Redirect};
use axum::routing::{get, post};
use axum::{middleware, Router};
use tower_http::compression::CompressionLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;

use crate::auth::{bearer_auth, worker_auth};
use crate::handlers;
use crate::openapi::ApiDoc;
use crate::ApiState;

/// The OpenAPI document, rendered once: it is immutable for the process
/// lifetime, so serializing it per request is pure waste.
static OPENAPI_JSON: LazyLock<String> = LazyLock::new(|| {
    serde_json::to_string(&ApiDoc::openapi()).unwrap_or_else(|e| {
        tracing::error!(error = %e, "failed to serialize the OpenAPI document");
        String::from("{}")
    })
});

static OPENAPI_YAML: LazyLock<String> =
    LazyLock::new(|| ApiDoc::openapi().to_yaml().unwrap_or_default());

/// The read-only dashboard, embedded at compile time and served under `/ui/`.
/// Static files only (no data), so they take no authentication; the page
/// itself asks for a tenant token and calls `/api/v1` with it. These are not
/// API routes and are deliberately absent from the OpenAPI document.
const UI_INDEX_HTML: &str = include_str!("../ui/index.html");
const UI_APP_CSS: &str = include_str!("../ui/app.css");
const UI_APP_JS: &str = include_str!("../ui/app.js");
const UI_API_JS: &str = include_str!("../ui/api.js");
const UI_FMT_JS: &str = include_str!("../ui/fmt.js");

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
            "/workflows/{id}/steps",
            get(handlers::get_workflow_step_states),
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
        .route("/queues", get(handlers::list_queues))
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

    // CORS: permissive is the development default; configured origins
    // restrict it (invalid entries are skipped with a warning rather than
    // silently allowing everything).
    let cors = if state.cors_origins.is_empty() {
        CorsLayer::permissive()
    } else {
        let origins: Vec<HeaderValue> = state
            .cors_origins
            .iter()
            .filter_map(|o| match o.parse::<HeaderValue>() {
                Ok(v) => Some(v),
                Err(_) => {
                    tracing::warn!(origin = %o, "ignoring invalid CORS origin");
                    None
                }
            })
            .collect();
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins))
            .allow_methods(tower_http::cors::Any)
            .allow_headers(tower_http::cors::Any)
    };

    Router::new()
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/openapi.json", get(openapi_json))
        .route("/openapi.yaml", get(openapi_yaml))
        .route("/docs", get(docs_ui))
        .route("/ui", get(|| async { Redirect::permanent("/ui/") }))
        .route(
            "/ui/",
            get(|| async { ui_asset("text/html; charset=utf-8", UI_INDEX_HTML) }),
        )
        .route(
            "/ui/app.css",
            get(|| async { ui_asset("text/css; charset=utf-8", UI_APP_CSS) }),
        )
        .route(
            "/ui/app.js",
            get(|| async { ui_asset("text/javascript; charset=utf-8", UI_APP_JS) }),
        )
        .route(
            "/ui/api.js",
            get(|| async { ui_asset("text/javascript; charset=utf-8", UI_API_JS) }),
        )
        .route(
            "/ui/fmt.js",
            get(|| async { ui_asset("text/javascript; charset=utf-8", UI_FMT_JS) }),
        )
        .nest("/api/v1", tenant.merge(worker))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .layer(CompressionLayer::new())
        .with_state(state)
}

async fn openapi_json() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json")],
        OPENAPI_JSON.as_str(),
    )
}

async fn openapi_yaml() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/yaml")],
        OPENAPI_YAML.as_str(),
    )
}

/// One embedded dashboard file. `no-cache` makes browsers revalidate on every
/// load, so a server upgrade is picked up immediately; the files are small.
fn ui_asset(content_type: &'static str, body: &'static str) -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
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
