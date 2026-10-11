//! Prometheus metrics endpoint.
//!
//! Three kinds of series, rendered as exposition text with no metrics
//! backend linked into the engine:
//!
//! * **Counters** from the engine's atomic [`StatsSnapshot`]: process-local,
//!   reset on restart.
//! * **Histograms** from the engine's fixed-bucket latency histograms
//!   (handler duration, queue wait): also process-local.
//! * **Gauges** read from the store on every scrape via
//!   [`JobApi::queue_stats`]: per-queue pending / scheduled / running counts
//!   and the oldest claimable job's age. These are fleet-wide and survive
//!   restarts because the database is the source of truth; every replica
//!   reports the same numbers, so scrape one or dedupe in your TSDB.

use std::future::Future;
use std::sync::Arc;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use queueflow_core::{HistogramSnapshot, JobApi, LatencySnapshot, QueueStats, StatsSnapshot};
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
    // A store outage must not take the scrape down with it: the process
    // counters still render, and `queueflow_store_up` flips to 0.
    let queues = match api.queue_stats(None).await {
        Ok(q) => Some(q),
        Err(e) => {
            tracing::warn!(error = %e, "metrics: queue stats unavailable");
            None
        }
    };
    let body = render(&api.stats(), &api.latency(), queues.as_deref());
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
    ));
}

fn gauge_header(out: &mut String, name: &str, help: &str) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} gauge\n"));
}

/// Escape a label value per the exposition format.
fn label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn histogram(out: &mut String, name: &str, help: &str, h: &HistogramSnapshot) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));
    for (le, count) in &h.buckets {
        out.push_str(&format!("{name}_bucket{{le=\"{le}\"}} {count}\n"));
    }
    out.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {}\n", h.count));
    out.push_str(&format!("{name}_sum {}\n", h.sum_secs));
    out.push_str(&format!("{name}_count {}\n", h.count));
}

fn render(s: &StatsSnapshot, lat: &LatencySnapshot, queues: Option<&[QueueStats]>) -> String {
    let mut out = String::new();
    counter(
        &mut out,
        "queueflow_jobs_created_total",
        "Jobs created by this process",
        s.jobs_created,
    );
    counter(
        &mut out,
        "queueflow_jobs_completed_total",
        "Jobs completed by this process",
        s.jobs_completed,
    );
    counter(
        &mut out,
        "queueflow_jobs_failed_total",
        "Jobs failed permanently by this process",
        s.jobs_failed,
    );
    counter(
        &mut out,
        "queueflow_jobs_retried_total",
        "Job retries scheduled by this process",
        s.jobs_retried,
    );
    counter(
        &mut out,
        "queueflow_jobs_dead_lettered_total",
        "Jobs moved to the dead-letter queue by this process",
        s.jobs_dead_lettered,
    );
    counter(
        &mut out,
        "queueflow_workflows_created_total",
        "Workflows created by this process",
        s.workflows_created,
    );
    counter(
        &mut out,
        "queueflow_workflows_completed_total",
        "Workflows completed by this process",
        s.workflows_completed,
    );
    counter(
        &mut out,
        "queueflow_workflows_failed_total",
        "Workflows failed or partially failed by this process",
        s.workflows_failed,
    );

    histogram(
        &mut out,
        "queueflow_handler_duration_seconds",
        "Wall time in-process handlers spent running a job",
        &lat.handler_duration,
    );
    histogram(
        &mut out,
        "queueflow_job_queue_wait_seconds",
        "Time from a job becoming due to an in-process worker claiming it",
        &lat.queue_wait,
    );

    gauge_header(
        &mut out,
        "queueflow_store_up",
        "1 when the per-queue gauges below were read from the store on this scrape, 0 when the store was unreachable",
    );
    out.push_str(&format!(
        "queueflow_store_up {}\n",
        u8::from(queues.is_some())
    ));
    if let Some(queues) = queues {
        gauge_header(
            &mut out,
            "queueflow_queue_pending_jobs",
            "Claimable jobs (pending or retrying, due now), all tenants, from the store",
        );
        for q in queues {
            out.push_str(&format!(
                "queueflow_queue_pending_jobs{{queue=\"{}\"}} {}\n",
                label(&q.queue),
                q.pending
            ));
        }
        gauge_header(
            &mut out,
            "queueflow_queue_scheduled_jobs",
            "Jobs waiting for a future run time (run_at or backoff), all tenants",
        );
        for q in queues {
            out.push_str(&format!(
                "queueflow_queue_scheduled_jobs{{queue=\"{}\"}} {}\n",
                label(&q.queue),
                q.scheduled
            ));
        }
        gauge_header(
            &mut out,
            "queueflow_queue_running_jobs",
            "Jobs currently leased by a worker, all tenants",
        );
        for q in queues {
            out.push_str(&format!(
                "queueflow_queue_running_jobs{{queue=\"{}\"}} {}\n",
                label(&q.queue),
                q.running
            ));
        }
        gauge_header(
            &mut out,
            "queueflow_queue_oldest_pending_age_seconds",
            "Seconds the oldest claimable job has waited; 0 when nothing is claimable",
        );
        for q in queues {
            out.push_str(&format!(
                "queueflow_queue_oldest_pending_age_seconds{{queue=\"{}\"}} {}\n",
                label(&q.queue),
                q.oldest_pending_age_secs.unwrap_or(0)
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use queueflow_core::{Histogram, QueueStats};

    #[test]
    fn renders_counters_histograms_and_queue_gauges() {
        let h = Histogram::default();
        h.observe(0.02);
        h.observe(3.0);
        let lat = LatencySnapshot {
            handler_duration: h.snapshot(),
            queue_wait: Histogram::default().snapshot(),
        };
        let stats = StatsSnapshot {
            jobs_completed: 7,
            ..Default::default()
        };
        let queues = vec![QueueStats {
            queue: "ord\"ers".into(),
            pending: 3,
            scheduled: 1,
            running: 2,
            oldest_pending_age_secs: Some(42),
        }];
        let out = render(&stats, &lat, Some(&queues));
        assert!(out.contains("queueflow_jobs_completed_total 7\n"));
        assert!(out.contains("queueflow_handler_duration_seconds_bucket{le=\"0.025\"} 1\n"));
        assert!(out.contains("queueflow_handler_duration_seconds_bucket{le=\"5\"} 2\n"));
        assert!(out.contains("queueflow_handler_duration_seconds_bucket{le=\"+Inf\"} 2\n"));
        assert!(out.contains("queueflow_handler_duration_seconds_count 2\n"));
        assert!(out.contains("queueflow_store_up 1\n"));
        assert!(out.contains("queueflow_queue_pending_jobs{queue=\"ord\\\"ers\"} 3\n"));
        assert!(
            out.contains("queueflow_queue_oldest_pending_age_seconds{queue=\"ord\\\"ers\"} 42\n")
        );
    }

    #[test]
    fn store_outage_still_renders_process_metrics() {
        let out = render(&StatsSnapshot::default(), &LatencySnapshot::default(), None);
        assert!(out.contains("queueflow_store_up 0\n"));
        assert!(!out.contains("queueflow_queue_pending_jobs{"));
        assert!(out.contains("queueflow_jobs_created_total 0\n"));
    }
}
