//! `queueflow serve` — wire Postgres adapters into the engine, run workers
//! and/or the API, and shut down gracefully.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use queueflow_api::{build_router, ApiState};
use queueflow_core::task::builtin;
use queueflow_core::{
    connect, migrate, Engine, PostgresJobStore, PostgresMessageQueue, SystemClock,
};
use tokio::net::TcpListener;

use crate::cli::{Mode, ServeArgs};
use crate::metrics;

pub async fn run(args: ServeArgs) -> anyhow::Result<()> {
    let pool = connect(&args.database_url, args.max_db_connections)
        .await
        .context("connect to PostgreSQL")?;

    if args.auto_migrate {
        migrate(&pool).await.context("apply migrations")?;
        tracing::info!("migrations applied");
    }

    let store = Arc::new(PostgresJobStore::new(pool.clone()));
    let queue = Arc::new(PostgresMessageQueue::new(pool));
    for q in [args.default_queue.as_str(), "priority", "retry"] {
        queue.ensure_queue(q).await.ok();
    }

    let engine = Engine::builder(store, queue, Arc::new(SystemClock))
        .default_queue(args.default_queue.clone())
        .worker_count(args.workers)
        .register("echo", builtin::echo())
        .register("log", builtin::log())
        .register("sleep", builtin::sleep())
        .register("fail", builtin::fail())
        .build();

    // Graceful shutdown on SIGINT/SIGTERM.
    {
        let engine = engine.clone();
        tokio::spawn(async move {
            wait_for_signal().await;
            tracing::info!("shutdown signal received");
            engine.shutdown();
        });
    }

    // Metrics server (best effort), shut down with the engine.
    let metrics_handle = {
        let api = engine.clone();
        let shutdown_token = engine.shutdown_token();
        let port = args.metrics_port;
        tokio::spawn(async move {
            let shutdown = async move { shutdown_token.cancelled().await };
            if let Err(e) = metrics::serve(port, api, shutdown).await {
                tracing::warn!(error = %e, "metrics server stopped");
            }
        })
    };

    // Workers (for `worker` and `all`).
    let workers = match args.mode {
        Mode::Api => None,
        _ => Some(engine.run_workers(args.default_queue.clone())),
    };

    match args.mode {
        Mode::Worker => {
            tracing::info!(workers = args.workers, queue = %args.default_queue, "running workers");
            if let Some(mut set) = workers {
                while set.join_next().await.is_some() {}
            }
        }
        Mode::Api | Mode::All => {
            let app = build_router(ApiState::new(engine.clone()));
            let listener = TcpListener::bind(("0.0.0.0", args.api_port))
                .await
                .with_context(|| format!("bind API port {}", args.api_port))?;
            tracing::info!(port = args.api_port, mode = ?args.mode, "API server listening");

            let shutdown_token = engine.shutdown_token();
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { shutdown_token.cancelled().await })
                .await
                .context("API server")?;

            // Let in-flight jobs drain (bounded).
            if let Some(mut set) = workers {
                let _ = tokio::time::timeout(Duration::from_secs(30), async {
                    while set.join_next().await.is_some() {}
                })
                .await;
            }
        }
    }

    metrics_handle.abort();
    tracing::info!("shutdown complete");
    Ok(())
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = term.recv() => {},
            _ = int.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
