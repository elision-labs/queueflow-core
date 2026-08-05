//! Bearer-token authentication middleware.
//!
//! Two credential classes share the `Authorization: Bearer` header but are
//! deliberately not interchangeable:
//!
//! * **Tenant credentials** authenticate producers (create/list/cancel jobs
//!   and workflows, DLQ admin). Two real mechanisms are supported, together
//!   or alone:
//!   - **JWTs** (HS256): configure `--jwt-secret`; the token's `sub` claim is
//!     the tenant id and `exp` is enforced.
//!   - **Static API keys**: configure `--api-keys "token:tenant,..."`; the
//!     token maps to its tenant.
//!
//!   With neither configured the server runs in **development mode**: any
//!   non-empty token authenticates as tenant `tenant1` (and `serve` warns).
//! * **The worker token** authenticates the worker protocol (lease,
//!   heartbeat, complete, fail). Workers are deployment infrastructure: they
//!   execute arbitrary tenants' jobs and see their payloads, so a tenant
//!   credential must never lease work. Configure it with `--worker-token`;
//!   when unset the worker routes fall back to accepting any authenticated
//!   caller (development mode, again with a loud warning).

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use queueflow_core::EngineError;

use crate::error::ApiError;
use crate::ApiState;

/// How the API authenticates callers. Attached to [`ApiState`].
#[derive(Clone, Debug, Default)]
pub struct AuthConfig {
    /// Credential required by the worker-protocol routes. `None` = any
    /// authenticated caller may act as a worker (development mode).
    pub worker_token: Option<String>,
    /// HS256 secret for validating tenant JWTs (`sub` = tenant id, `exp`
    /// enforced).
    pub jwt_secret: Option<String>,
    /// Static API keys as `(token, tenant)` pairs.
    pub api_keys: Vec<(String, String)>,
}

impl AuthConfig {
    /// True when real tenant authentication is configured; false means the
    /// development-mode placeholder is in effect.
    pub fn strict(&self) -> bool {
        self.jwt_secret.is_some() || !self.api_keys.is_empty()
    }
}

/// Parse `--api-keys` input: comma-separated `token:tenant` pairs.
pub fn parse_api_keys(raw: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    for pair in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let Some((token, tenant)) = pair.split_once(':') else {
            return Err(format!("api key entry '{pair}' is not 'token:tenant'"));
        };
        let (token, tenant) = (token.trim(), tenant.trim());
        if token.is_empty() || tenant.is_empty() {
            return Err(format!(
                "api key entry '{pair}' has an empty token or tenant"
            ));
        }
        out.push((token.to_string(), tenant.to_string()));
    }
    Ok(out)
}

/// The authenticated tenant, injected into request extensions.
#[derive(Clone, Debug)]
pub struct Tenant(pub String);

/// The bearer token on a request, if any.
fn bearer_token(req: &Request) -> Option<&str> {
    req.headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
}

/// Constant-time string comparison, so tokens cannot be recovered
/// byte-by-byte through response timing. (Length is not hidden; tokens of the
/// wrong length fail fast, which reveals nothing useful.)
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Tenant endpoints: reject requests without a valid tenant credential;
/// attach the [`Tenant`]. The worker token, when configured, is explicitly
/// refused here — infrastructure credentials do not get a tenant identity.
pub async fn bearer_auth(
    State(state): State<ApiState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(token) = bearer_token(&req) else {
        return Err(ApiError(EngineError::Unauthorized));
    };
    if let Some(worker_token) = &state.auth.worker_token {
        if ct_eq(token, worker_token) {
            return Err(ApiError(EngineError::Forbidden));
        }
    }
    let tenant = validate_token(&state.auth, token).ok_or(ApiError(EngineError::Unauthorized))?;
    req.extensions_mut().insert(Tenant(tenant));
    Ok(next.run(req).await)
}

/// Worker-protocol endpoints: require the configured worker token. Without
/// one configured, any authenticated caller is accepted (development mode).
pub async fn worker_auth(
    State(state): State<ApiState>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(token) = bearer_token(&req) else {
        return Err(ApiError(EngineError::Unauthorized));
    };
    match &state.auth.worker_token {
        Some(expected) if ct_eq(token, expected) => Ok(next.run(req).await),
        Some(_) => {
            // Authenticated as something else (e.g. a tenant): forbidden, not
            // unauthorized — the caller holds real credentials of the wrong
            // class.
            if validate_token(&state.auth, token).is_some() {
                Err(ApiError(EngineError::Forbidden))
            } else {
                Err(ApiError(EngineError::Unauthorized))
            }
        }
        None => {
            if validate_token(&state.auth, token).is_some() {
                Ok(next.run(req).await)
            } else {
                Err(ApiError(EngineError::Unauthorized))
            }
        }
    }
}

/// The `sub` claim carries the tenant id; `exp` is enforced by
/// [`jsonwebtoken::Validation`]'s defaults.
#[derive(serde::Deserialize)]
struct Claims {
    sub: String,
}

fn validate_jwt(secret: &str, token: &str) -> Option<String> {
    let key = jsonwebtoken::DecodingKey::from_secret(secret.as_bytes());
    let validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    match jsonwebtoken::decode::<Claims>(token, &key, &validation) {
        Ok(data) => Some(data.claims.sub),
        Err(e) => {
            tracing::debug!(error = %e, "jwt rejected");
            None
        }
    }
}

/// Validate a tenant credential and return its tenant id.
fn validate_token(cfg: &AuthConfig, token: &str) -> Option<String> {
    // Static API keys first: exact, constant-time comparison per key.
    for (key, tenant) in &cfg.api_keys {
        if ct_eq(token, key) {
            return Some(tenant.clone());
        }
    }
    if let Some(secret) = &cfg.jwt_secret {
        if let Some(sub) = validate_jwt(secret, token) {
            return Some(sub);
        }
    }
    if cfg.strict() {
        // Real auth is configured and nothing matched.
        None
    } else if token.is_empty() {
        None
    } else {
        // Development mode: any non-empty token maps to a fixed tenant.
        Some("tenant1".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{ct_eq, parse_api_keys, validate_token, AuthConfig};

    #[test]
    fn ct_eq_matches_equal_strings_only() {
        assert!(ct_eq("secret", "secret"));
        assert!(!ct_eq("secret", "secreT"));
        assert!(!ct_eq("secret", "secre"));
        assert!(!ct_eq("", "x"));
        assert!(ct_eq("", ""));
    }

    #[test]
    fn api_keys_parse_and_reject_malformed() {
        assert_eq!(
            parse_api_keys("a:t1, b:t2").unwrap(),
            vec![
                ("a".to_string(), "t1".to_string()),
                ("b".to_string(), "t2".to_string())
            ]
        );
        assert!(parse_api_keys("missing-tenant").is_err());
        assert!(parse_api_keys(":t").is_err());
        assert!(parse_api_keys("t:").is_err());
        assert_eq!(parse_api_keys("").unwrap(), vec![]);
    }

    #[test]
    fn strict_mode_disables_the_placeholder() {
        let dev = AuthConfig::default();
        assert_eq!(validate_token(&dev, "anything"), Some("tenant1".into()));

        let strict = AuthConfig {
            api_keys: vec![("k1".into(), "acme".into())],
            ..Default::default()
        };
        assert_eq!(validate_token(&strict, "k1"), Some("acme".into()));
        assert_eq!(validate_token(&strict, "anything"), None);
    }

    #[test]
    fn jwt_subject_becomes_the_tenant() {
        let cfg = AuthConfig {
            jwt_secret: Some("s3cret".into()),
            ..Default::default()
        };
        let exp = (chrono::Utc::now().timestamp() + 3600) as usize;
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(), // HS256
            &serde_json::json!({ "sub": "acme", "exp": exp }),
            &jsonwebtoken::EncodingKey::from_secret(b"s3cret"),
        )
        .unwrap();
        assert_eq!(validate_token(&cfg, &token), Some("acme".into()));

        // Wrong signature and garbage are both rejected outright.
        let forged = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &serde_json::json!({ "sub": "acme", "exp": exp }),
            &jsonwebtoken::EncodingKey::from_secret(b"other"),
        )
        .unwrap();
        assert_eq!(validate_token(&cfg, &forged), None);
        assert_eq!(validate_token(&cfg, "not-a-jwt"), None);
    }
}
