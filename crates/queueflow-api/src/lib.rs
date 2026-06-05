//! # queueflow-api
//!
//! The HTTP surface for QueueFlow: an axum router plus the utoipa-generated
//! OpenAPI document. It depends only on the object-safe [`queueflow_core::JobApi`]
//! facade, so it is agnostic to whether the engine is backed by Postgres or the
//! in-memory adapters — which is exactly what lets the API be tested in-process
//! with no database (see this crate's `tests/`).

pub mod auth;
pub mod dto;
pub mod error;
pub mod handlers;
pub mod openapi;
pub mod router;

use std::sync::Arc;

use queueflow_core::JobApi;

/// Shared application state injected into every handler.
#[derive(Clone)]
pub struct ApiState {
    pub engine: Arc<dyn JobApi>,
}

impl ApiState {
    pub fn new(engine: Arc<dyn JobApi>) -> Self {
        Self { engine }
    }
}

pub use openapi::ApiDoc;
pub use router::build_router;
