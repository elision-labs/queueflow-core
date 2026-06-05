//! # queueflow-core
//!
//! The core engine for **QueueFlow**, a PostgreSQL/PGMQ-native distributed job
//! queue and workflow engine — a Rust rewrite of the Go `queueflow-core`.
//!
//! ## What's improved over the Go version
//!
//! * **Workflows are actually implemented.** The Go API returns `501` for every
//!   workflow route; here the [`workflow`] module is a real DAG orchestrator
//!   with dependency gating, context propagation, per-step failure policies,
//!   cycle detection, and an ergonomic [`WorkflowBuilder`] DSL.
//! * **Testability is a first-class design goal.** The engine is written
//!   against the [`ports`] (`JobStore`/`MessageQueue`) so the entire system —
//!   including retries and workflow orchestration — runs against the
//!   deterministic [`adapters::memory`] adapters with **no database**.
//! * **Durable retries.** Retries use the queue's native delayed redelivery
//!   ([`MessageQueue::send_delayed`]) instead of the Go version's detached timer
//!   goroutine, so they survive process restarts.
//! * **Typed everything.** Statuses, backoff strategies, and failure policies
//!   are exhaustive enums; durations are plain seconds for clean SDKs.
//!
//! ## Quick start (no database required)
//!
//! ```
//! use std::sync::Arc;
//! use queueflow_core::*;
//! use serde_json::json;
//!
//! # async fn run() -> Result<(), EngineError> {
//! let clock = Arc::new(SystemClock);
//! let store = Arc::new(InMemoryJobStore::new());
//! let queue = Arc::new(InMemoryMessageQueue::new(clock.clone()));
//!
//! let engine = Engine::builder(store, queue, clock)
//!     .register("echo", queueflow_core::task::builtin::echo())
//!     .build();
//!
//! let id = engine.enqueue("echo", json!({"hi": true}).as_object().unwrap().clone().into_iter().collect(), Default::default()).await?;
//! engine.process_once("default").await?;
//! assert_eq!(engine.get_job(&id).await?.status, JobStatus::Completed);
//! # Ok(()) }
//! ```

pub mod adapters;
pub mod api;
pub mod domain;
pub mod engine;
pub mod error;
pub mod ports;
pub mod stats;
pub mod task;
pub mod workflow;

// ---- Curated public surface -------------------------------------------------

pub use adapters::clock::{SystemClock, TestClock};
pub use adapters::memory::{InMemoryJobStore, InMemoryMessageQueue};
pub use api::JobApi;
pub use domain::{
    BackoffStrategy, CreateWorkflowRequest, Job, JobConfig, JobStatus, Json, Map, OnFailure,
    OnSuccess, StepStatus, Workflow, WorkflowStatus, WorkflowStep, CONTEXT_KEY,
};
pub use engine::{Engine, EngineBuilder, EnqueueOptions};
pub use error::{EngineError, HandlerError};
pub use ports::{
    Clock, JobStore, ListFilter, MessageQueue, QueueError, QueueMessage, ReadMessage, StepRecord,
    StorageError,
};
pub use stats::{EngineStats, StatsSnapshot};
pub use task::{handler_fn, FnHandler, TaskHandler};
pub use workflow::{CycleError, DependencyGraph, StepBuilder, WorkflowBuilder, WorkflowScheduler};

#[cfg(feature = "postgres")]
pub use adapters::postgres::{connect, migrate, PostgresJobStore, PostgresMessageQueue};
