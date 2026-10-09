//! `queueflow serve` — wire Postgres adapters into the engine, run workers
//! and/or the API, and shut down gracefully.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use queueflow_api::auth::{parse_api_keys, AuthConfig};
use queueflow_api::{build_router, ApiState};
use queueflow_core::task::builtin;
use queueflow_core::{connect, migrate, Engine, JanitorConfig, PostgresJobStore, SystemClock};
use tokio::net::TcpListener;

use crate::cli::{Mode, ServeArgs};
use crate::metrics;

pub async fn run(args: ServeArgs) -> anyhow::Result<()> {
    // Credentials are validated first: an API that would start without them
    // must not get as far as touching the database.
    let auth = match args.mode {
        Mode::Worker => None,
        Mode::Api | Mode::All => Some(auth_config(&args)?),
    };

    let pool = connect(&args.database_url, args.max_db_connections)
        .await
        .context("connect to PostgreSQL")?;

    if args.auto_migrate {
        migrate(&pool).await.context("apply migrations")?;
        tracing::info!("migrations applied");
    }

    let store = Arc::new(PostgresJobStore::new(pool));

    let engine = Engine::builder(store, Arc::new(SystemClock))
        .default_queue(args.default_queue.clone())
        .worker_count(args.workers)
        .janitor(JanitorConfig {
            retention: args
                .retention_hours
                .map(|h| Duration::from_secs(h.max(1) * 3600)),
            ..JanitorConfig::default()
        })
        .register("echo", builtin::echo())
        .register("log", builtin::log())
        .register("sleep", builtin::sleep())
        .register("fail", builtin::fail())
        .build();

    // The janitor runs on every server: expired-lease recovery and workflow
    // self-heal are SKIP LOCKED / idempotent, retention is advisory-locked.
    let janitor_handle = engine.run_janitor();

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

    // Workers (for `worker` and `all`). In API-only mode workers run in other
    // processes (or as remote workers over the lease API); mark the engine
    // running so /ready reflects this process's actual readiness instead of
    // permanently reporting a missing local worker pool.
    let workers = match args.mode {
        Mode::Api => {
            engine.mark_running();
            None
        }
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
            let auth = auth.expect("auth config is built for api/all mode");
            let cors_origins: Vec<String> = args
                .cors_origins
                .as_deref()
                .map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|o| !o.is_empty())
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            if cors_origins.is_empty() {
                tracing::warn!(
                    "no --cors-origins / QUEUEFLOW_CORS_ORIGINS configured: CORS is permissive. \
                     Restrict it before exposing this API to browsers."
                );
            }
            let app = build_router(
                ApiState::new(engine.clone())
                    .with_auth(auth)
                    .with_cors_origins(cors_origins),
            );
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
    let _ = janitor_handle.await;
    tracing::info!("shutdown complete");
    Ok(())
}

/// Build the API's authentication configuration from the CLI and refuse to
/// run without credentials unless `--dev` was passed. Called before anything
/// else (even the database connection) so a misconfigured deployment fails
/// immediately and obviously.
fn auth_config(args: &ServeArgs) -> anyhow::Result<AuthConfig> {
    let auth = AuthConfig {
        worker_token: args.worker_token.clone(),
        jwt_secret: args.jwt_secret.clone(),
        api_keys: args
            .api_keys
            .as_deref()
            .map(parse_api_keys)
            .transpose()
            .map_err(anyhow::Error::msg)
            .context("--api-keys")?
            .unwrap_or_default(),
        dev_mode: args.dev,
    };
    // Fail closed: the API only starts without credentials when
    // development mode was requested explicitly.
    if !args.dev {
        let mut missing = Vec::new();
        if !auth.strict() {
            missing.push("--jwt-secret or --api-keys (QUEUEFLOW_JWT_SECRET / QUEUEFLOW_API_KEYS)");
        }
        if auth.worker_token.is_none() {
            missing.push("--worker-token (QUEUEFLOW_WORKER_TOKEN)");
        }
        if !missing.is_empty() {
            anyhow::bail!(
                "refusing to serve the API without credentials: set {}. For local \
                 development only, pass --dev (QUEUEFLOW_DEV=1) to run with the \
                 placeholder tenant and open worker endpoints.",
                missing.join(" and ")
            );
        }
    } else {
        tracing::warn!(
            "--dev: development mode is ON. Never expose this server: any non-empty \
             token authenticates as tenant 'tenant1'{}",
            if auth.worker_token.is_none() {
                " and the worker-protocol endpoints accept any authenticated token"
            } else {
                ""
            }
        );
    }
    Ok(auth)
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
