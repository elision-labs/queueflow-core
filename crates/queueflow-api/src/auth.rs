//! Bearer-token authentication middleware.
//!
//! Mirrors the Go reference: a non-empty `Authorization: Bearer <token>` is
//! accepted and mapped to a tenant. Real JWT validation is a documented TODO
//! (the Go version is identical in this regard) — the seam is here in
//! [`validate_token`].

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use queueflow_core::EngineError;

use crate::error::ApiError;

/// The authenticated tenant, injected into request extensions.
#[derive(Clone, Debug)]
pub struct Tenant(pub String);

/// Reject requests without a valid bearer token; attach the [`Tenant`].
pub async fn bearer_auth(mut req: Request, next: Next) -> Result<Response, ApiError> {
    let header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let token = match header {
        Some(h) => h.strip_prefix("Bearer ").map(str::trim),
        None => None,
    };

    let Some(token) = token.filter(|t| !t.is_empty()) else {
        return Err(ApiError(EngineError::Unauthorized));
    };

    let tenant = validate_token(token).ok_or(ApiError(EngineError::Unauthorized))?;
    req.extensions_mut().insert(Tenant(tenant));
    Ok(next.run(req).await)
}

/// Validate a token and return its tenant id.
///
/// Placeholder: any non-empty token authenticates as `tenant1`. Replace with
/// JWT signature + claims validation, or an API-key lookup, in production.
fn validate_token(token: &str) -> Option<String> {
    if token.is_empty() {
        None
    } else {
        Some("tenant1".to_string())
    }
}
