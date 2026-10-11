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

/// Default cap on one batch enqueue; see [`ApiState::with_max_batch`].
pub const DEFAULT_MAX_BATCH: usize = 1000;

/// Shared application state injected into every handler.
#[derive(Clone)]
pub struct ApiState {
    pub engine: Arc<dyn JobApi>,
    /// How callers authenticate (tenant JWTs / API keys, worker token),
    /// prepared for per-request use (the JWT decoding key is built once).
    /// The default fails closed (no credentials configured, nothing
    /// authenticates); see [`auth::AuthConfig`] and
    /// [`auth::AuthConfig::development`].
    pub auth: Arc<auth::AuthState>,
    /// Origins allowed by CORS. Empty = permissive (the development
    /// default); set via [`ApiState::with_cors_origins`] to restrict.
    pub cors_origins: Arc<Vec<String>>,
    /// Maximum jobs accepted by one `POST /api/v1/jobs/batch` (default 1000).
    pub max_batch: usize,
}

impl ApiState {
    pub fn new(engine: Arc<dyn JobApi>) -> Self {
        Self {
            engine,
            auth: Arc::new(auth::AuthConfig::default().into()),
            cors_origins: Arc::new(Vec::new()),
            max_batch: DEFAULT_MAX_BATCH,
        }
    }

    /// Cap the batch-enqueue size (default [`DEFAULT_MAX_BATCH`]).
    pub fn with_max_batch(mut self, n: usize) -> Self {
        self.max_batch = n.max(1);
        self
    }

    /// Replace the whole authentication configuration.
    pub fn with_auth(mut self, auth: auth::AuthConfig) -> Self {
        self.auth = Arc::new(auth.into());
        self
    }

    /// Require `token` on the worker-protocol routes (keeps the rest of the
    /// auth configuration unchanged).
    pub fn with_worker_token(mut self, token: Option<String>) -> Self {
        let mut config = self.auth.config.clone();
        config.worker_token = token;
        self.auth = Arc::new(config.into());
        self
    }

    /// Restrict CORS to these origins. An empty list keeps the permissive
    /// development default.
    pub fn with_cors_origins(mut self, origins: Vec<String>) -> Self {
        self.cors_origins = Arc::new(origins);
        self
    }
}

pub use openapi::ApiDoc;
pub use router::build_router;
