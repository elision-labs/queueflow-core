//! Maps engine errors to HTTP responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use queueflow_core::EngineError;

use crate::dto::ErrorBody;

/// Wraps an [`EngineError`] so handlers can `?` and get a proper HTTP response.
pub struct ApiError(pub EngineError);

impl From<EngineError> for ApiError {
    fn from(e: EngineError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self.0 {
            e if e.is_not_found() => (StatusCode::NOT_FOUND, e.to_string()),
            e if e.is_bad_request() => (StatusCode::BAD_REQUEST, e.to_string()),
            EngineError::Conflict(_) => (StatusCode::CONFLICT, self.0.to_string()),
            EngineError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_string()),
            EngineError::Forbidden => (StatusCode::FORBIDDEN, "access denied".to_string()),
            other => {
                // Don't leak internals; log the detail, return a generic message.
                tracing::error!(error = %other, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };
        (status, Json(ErrorBody::new(message))).into_response()
    }
}
