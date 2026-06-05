//! Prometheus metrics endpoint.
//!
//! The core engine exposes cheap atomic counters ([`StatsSnapshot`]); here we
//! render them as Prometheus exposition text. No metrics backend is linked into
//! the engine, keeping the hot path dependency-free.

use std::future::Future;
use std::sync::Arc;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use queueflow_core::{JobApi, StatsSnapshot};
use tokio::net::TcpListener;

/// Serve `/metrics` (and a `/health` ok) on `port` until `shutdown` resolves.
pub async fn serve(
    port: u16,
    api: Arc<dyn JobApi>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(|| async { "ok" }))
        .with_state(api);

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "metrics server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

async fn metrics_handler(State(api): State<Arc<dyn JobApi>>) -> impl IntoResponse {
    let body = render(&api.stats());
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
    ));
}

fn render(s: &StatsSnapshot) -> String {
    let mut out = String::new();
    counter(
        &mut out,
        "queueflow_jobs_created_total",
        "Jobs created",
        s.jobs_created,
    );
    counter(
        &mut out,
        "queueflow_jobs_completed_total",
        "Jobs completed",
        s.jobs_completed,
    );
    counter(
        &mut out,
        "queueflow_jobs_failed_total",
        "Jobs failed permanently",
        s.jobs_failed,
    );
    counter(
        &mut out,
        "queueflow_jobs_retried_total",
        "Job retries scheduled",
        s.jobs_retried,
    );
    counter(
        &mut out,
        "queueflow_jobs_dead_lettered_total",
        "Jobs moved to the dead-letter queue",
        s.jobs_dead_lettered,
    );
    counter(
        &mut out,
        "queueflow_workflows_created_total",
        "Workflows created",
        s.workflows_created,
    );
    counter(
        &mut out,
        "queueflow_workflows_completed_total",
        "Workflows completed",
        s.workflows_completed,
    );
    counter(
        &mut out,
        "queueflow_workflows_failed_total",
        "Workflows failed or partially failed",
        s.workflows_failed,
    );
    out
}
