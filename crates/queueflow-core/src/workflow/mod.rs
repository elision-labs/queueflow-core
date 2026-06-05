//! Workflow orchestration: DAG validation, an ergonomic builder DSL, and the
//! runtime scheduler that gates steps on their dependencies.
//!
//! This is the headline feature the Go reference never implemented (every
//! workflow endpoint there returns `501 Not Implemented`).

pub mod builder;
pub mod dag;
pub mod scheduler;

pub use builder::{StepBuilder, WorkflowBuilder};
pub use dag::{CycleError, DependencyGraph};
pub use scheduler::WorkflowScheduler;
