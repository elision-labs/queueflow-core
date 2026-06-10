//! Error types for the engine surface.

use crate::ports::StorageError;
use crate::workflow::dag::CycleError;

/// Top-level error returned by engine and [`crate::api::JobApi`] operations.
///
/// The HTTP layer maps these to status codes (see `queueflow-api`):
/// `NotFound` → 404, `Validation`/`Workflow` → 400, `Unauthorized` → 401,
/// `Forbidden` → 403, `Conflict` → 409, everything else → 500.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error("invalid workflow: {0}")]
    Workflow(#[from] CycleError),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("unauthorized")]
    Unauthorized,

    #[error("forbidden")]
    Forbidden,
}

impl EngineError {
    /// True when the error represents a missing resource (404).
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            EngineError::Storage(
                StorageError::JobNotFound(_)
                    | StorageError::WorkflowNotFound(_)
                    | StorageError::StepNotFound { .. }
            )
        )
    }

    /// True when the error is the caller's fault (400).
    pub fn is_bad_request(&self) -> bool {
        matches!(self, EngineError::Validation(_) | EngineError::Workflow(_))
    }
}

/// Error returned by a [`crate::task::TaskHandler`].
///
/// `retryable` controls whether the engine schedules a retry (subject to
/// `max_retries`) or sends the job straight to the dead-letter queue. A handler
/// can signal a permanent, non-retryable failure (e.g. bad input) explicitly.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct HandlerError {
    pub message: String,
    pub retryable: bool,
}

impl HandlerError {
    /// A transient failure: the engine will retry up to `max_retries`.
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    /// A permanent failure: the engine will not retry; the job is dead-lettered.
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }
}

impl From<&str> for HandlerError {
    fn from(s: &str) -> Self {
        HandlerError::retryable(s)
    }
}

impl From<String> for HandlerError {
    fn from(s: String) -> Self {
        HandlerError::retryable(s)
    }
}
