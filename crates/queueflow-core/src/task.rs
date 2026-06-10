//! Task handlers: the user-supplied functions that actually process jobs.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::Map;
use crate::error::HandlerError;

/// Processes a job's payload and returns a result object.
///
/// Implement this trait for stateful handlers, or use [`handler_fn`] to wrap an
/// async closure.
///
/// **Idempotency.** Delivery is at-least-once: if a worker crashes after the
/// handler finished but before the terminal write landed, the janitor reaps
/// the expired lease and the job is retried — so the handler runs again. A
/// job that already reached a terminal state is never re-run, but handlers
/// with external side effects should still be written to be idempotent.
#[async_trait]
pub trait TaskHandler: Send + Sync {
    async fn handle(&self, payload: Map) -> Result<Map, HandlerError>;
}

/// Adapts an `async` closure into a [`TaskHandler`].
pub struct FnHandler<F> {
    f: F,
}

#[async_trait]
impl<F, Fut> TaskHandler for FnHandler<F>
where
    F: Fn(Map) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Map, HandlerError>> + Send,
{
    async fn handle(&self, payload: Map) -> Result<Map, HandlerError> {
        (self.f)(payload).await
    }
}

/// Wrap an async closure `Fn(Map) -> impl Future<Output = Result<Map, HandlerError>>`
/// as a shareable [`TaskHandler`].
///
/// ```
/// use queueflow_core::{handler_fn, Map};
/// let h = handler_fn(|payload: Map| async move { Ok(payload) });
/// ```
pub fn handler_fn<F, Fut>(f: F) -> Arc<dyn TaskHandler>
where
    F: Fn(Map) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Map, HandlerError>> + Send + 'static,
{
    Arc::new(FnHandler { f })
}

/// Built-in `log`/`sleep`/`echo` handlers, plus a `fail` handler that is handy
/// in tests and demos.
pub mod builtin {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::json;

    use super::{handler_fn, TaskHandler};
    use crate::domain::Map;
    use crate::error::HandlerError;

    /// Returns the payload unchanged plus a `timestamp` field.
    pub fn echo() -> Arc<dyn TaskHandler> {
        handler_fn(|mut payload: Map| async move {
            payload.insert("echoed".into(), json!(true));
            Ok(payload)
        })
    }

    /// Logs the payload at info level and returns an empty result.
    pub fn log() -> Arc<dyn TaskHandler> {
        handler_fn(|payload: Map| async move {
            tracing::info!(?payload, "log task");
            Ok(Map::new())
        })
    }

    /// Sleeps for `payload["duration"]` seconds (default 0), respecting the
    /// engine's per-attempt timeout (the future is simply dropped on timeout).
    pub fn sleep() -> Arc<dyn TaskHandler> {
        handler_fn(|payload: Map| async move {
            let secs = payload
                .get("duration")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
                .max(0.0);
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            Ok(Map::from_iter([("slept_for".into(), json!(secs))]))
        })
    }

    /// Always fails. `payload["permanent"] == true` makes it non-retryable.
    pub fn fail() -> Arc<dyn TaskHandler> {
        handler_fn(|payload: Map| async move {
            let permanent = payload
                .get("permanent")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let msg = payload
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("intentional failure")
                .to_string();
            Err(if permanent {
                HandlerError::permanent(msg)
            } else {
                HandlerError::retryable(msg)
            })
        })
    }
}
