//! Remote worker runtime: lease jobs over HTTP, run local handlers, heartbeat
//! while they run, and report completion/failure.
//!
//! This is the same at-least-once contract as the server's built-in worker
//! loop (handlers should be idempotent), with the lease heartbeat standing in
//! for the server-side visibility-timeout extension.
//!
//! ```no_run
//! use std::time::Duration;
//! use queueflow_client::{Client, HandlerError, Map};
//! use queueflow_client::worker::{Worker, WorkerOptions};
//!
//! # async fn run() {
//! let client = Client::new("http://localhost:8000", "my-token");
//! let worker = Worker::new(client, "default", WorkerOptions::default())
//!     .register("resize-image", |job| async move {
//!         let url = job.payload.get("url").cloned();
//!         // ... do the work ...
//!         let _ = url;
//!         Ok(Map::new())
//!     });
//! worker.run().await; // until the process is stopped
//! # }
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use queueflow_core::{HandlerError, Job, LeasedJob, Map};
use tokio_util::sync::CancellationToken;

use crate::Client;

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Map, HandlerError>> + Send>>;
type Handler = Arc<dyn Fn(Job) -> HandlerFuture + Send + Sync>;

/// Tuning for [`Worker`].
#[derive(Clone, Debug)]
pub struct WorkerOptions {
    /// Jobs leased per poll (1..=100).
    pub batch_size: usize,
    /// Initial lease duration; the worker heartbeats at half this interval
    /// while a handler runs, so it also bounds redelivery delay after a crash.
    pub lease_secs: u32,
    /// Server-side long-poll when the queue is empty (0..=30).
    pub wait_secs: u32,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            batch_size: 1,
            lease_secs: 30,
            wait_secs: 20,
        }
    }
}

/// A remote worker: a set of named handlers drained from one queue.
pub struct Worker {
    client: Client,
    queue: String,
    options: WorkerOptions,
    handlers: HashMap<String, Handler>,
    shutdown: CancellationToken,
}

impl Worker {
    pub fn new(client: Client, queue: impl Into<String>, options: WorkerOptions) -> Self {
        Self {
            client,
            queue: queue.into(),
            options,
            handlers: HashMap::new(),
            shutdown: CancellationToken::new(),
        }
    }

    /// Register an async handler for a task name.
    pub fn register<F, Fut>(mut self, task_name: impl Into<String>, f: F) -> Self
    where
        F: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Map, HandlerError>> + Send + 'static,
    {
        self.handlers
            .insert(task_name.into(), Arc::new(move |job| Box::pin(f(job))));
        self
    }

    /// A token that stops [`Worker::run`] when cancelled (e.g. from a signal
    /// handler). In-flight jobs finish and report before the loop exits.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Lease/execute/report until the shutdown token fires.
    pub async fn run(self) {
        tracing::info!(queue = %self.queue, tasks = ?self.handlers.keys().collect::<Vec<_>>(), "remote worker started");
        loop {
            if self.shutdown.is_cancelled() {
                break;
            }
            let leased = tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                r = self.client.lease_jobs(
                    &self.queue,
                    self.options.batch_size,
                    self.options.lease_secs,
                    self.options.wait_secs,
                ) => r,
            };
            match leased {
                Ok(jobs) => {
                    for lease in jobs {
                        self.process(lease).await;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "lease failed; backing off");
                    tokio::select! {
                        _ = self.shutdown.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
        tracing::info!(queue = %self.queue, "remote worker stopped");
    }

    /// Run one leased job: dispatch to its handler under a heartbeat, then
    /// report the outcome. Reporting errors are logged, not retried here — the
    /// lease simply expires and the server redelivers (at-least-once).
    async fn process(&self, lease: LeasedJob) {
        let task = lease.job.task_name.clone();
        let job_id = lease.job.id.clone();
        let Some(handler) = self.handlers.get(&task).cloned() else {
            tracing::error!(job_id = %job_id, task = %task, "no handler registered");
            if let Err(e) = self
                .client
                .fail_job(
                    &lease,
                    &format!("no remote handler for task '{task}'"),
                    false,
                )
                .await
            {
                tracing::warn!(job_id = %job_id, error = %e, "failed to report missing handler");
            }
            return;
        };

        // Heartbeat at half the lease interval while the handler runs.
        let heartbeat_every = Duration::from_secs((self.options.lease_secs / 2).max(1) as u64);
        let mut handler_fut = std::pin::pin!(handler(lease.job.clone()));
        let outcome = loop {
            tokio::select! {
                out = &mut handler_fut => break out,
                _ = tokio::time::sleep(heartbeat_every) => {
                    if let Err(e) = self.client.heartbeat_job(&lease, self.options.lease_secs).await {
                        tracing::warn!(job_id = %job_id, error = %e, "heartbeat failed");
                    }
                }
            }
        };

        let report = match outcome {
            Ok(result) => self.client.complete_job(&lease, result).await,
            Err(err) => {
                self.client
                    .fail_job(&lease, &err.message, err.retryable)
                    .await
            }
        };
        if let Err(e) = report {
            tracing::warn!(job_id = %job_id, error = %e, "failed to report job outcome; lease will expire and redeliver");
        }
    }
}
