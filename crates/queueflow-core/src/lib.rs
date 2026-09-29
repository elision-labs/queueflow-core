//! # queueflow-core
//!
//! The core engine for **QueueFlow**, a PostgreSQL-native distributed job
//! queue and workflow engine. The jobs table *is* the queue: workers claim
//! due rows with `FOR UPDATE SKIP LOCKED` and own them through lease tokens,
//! so any plain Postgres (RDS, Cloud SQL, Azure) works — no extensions.
//!
//! ## Design highlights
//!
//! * **Workflows as a real DAG orchestrator.** The [`workflow`] module provides
//!   dependency gating, context propagation, per-step failure policies,
//!   cycle detection, and an ergonomic [`WorkflowBuilder`] DSL.
//! * **Testability is a first-class design goal.** The engine is written
//!   against the [`ports`] (`JobStore`) so the entire system — including
//!   retries and workflow orchestration — runs against the deterministic
//!   [`adapters::memory`] adapter with **no database**.
//! * **Durable retries and scheduling.** A retry (or a `run_at` job) is just a
//!   row whose `scheduled_at` lies in the future, so delays survive restarts.
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
//! let store = Arc::new(InMemoryJobStore::new(clock.clone()));
//!
//! let engine = Engine::builder(store, clock)
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
pub mod cron;
pub mod domain;
pub mod engine;
pub mod error;
pub mod ports;
pub mod stats;
pub mod task;
pub mod workflow;

// ---- Curated public surface -------------------------------------------------

pub use adapters::clock::{SystemClock, TestClock};
pub use adapters::memory::InMemoryJobStore;
pub use api::JobApi;
pub use domain::{
    limits, BackoffStrategy, CreateCronRequest, CreateWorkflowRequest, CronSchedule, DeadLetter,
    Job, JobConfig, JobStatus, Json, LeasedJob, Map, OnFailure, OnSuccess, StepStatus, Workflow,
    WorkflowStatus, WorkflowStep, CONTEXT_KEY,
};
pub use engine::janitor::JanitorSweepReport;
pub use engine::{BatchItem, Engine, EngineBuilder, EnqueueOptions, JanitorConfig};
pub use error::{EngineError, HandlerError};
pub use ports::{
    Claimed, Clock, FinishedJob, JobStore, ListFilter, Page, PageCursor, StepRecord, StorageError,
};
pub use stats::{EngineStats, StatsSnapshot};
pub use task::{handler_fn, FnHandler, TaskHandler};
pub use workflow::{CycleError, DependencyGraph, StepBuilder, WorkflowBuilder, WorkflowScheduler};

#[cfg(feature = "postgres")]
pub use adapters::postgres::{connect, migrate, PostgresJobStore};
