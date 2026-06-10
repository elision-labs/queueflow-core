//! PostgreSQL adapter (feature `postgres`). Plain Postgres — no extensions:
//! the jobs table doubles as the claim queue, and LISTEN/NOTIFY powers the
//! long-poll wakeups.
//!
//! Uses sqlx's **runtime** query API (string queries, no `query!` macros),
//! so the crate compiles with no `DATABASE_URL` and no `.sqlx` cache. SQL
//! correctness is verified by the opt-in integration tests gated on
//! `TEST_DATABASE_URL`.

mod job_store;
mod listener;

pub use job_store::PostgresJobStore;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Open a connection pool tuned for queue workloads.
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections.max(1))
        .min_connections(1)
        .connect(database_url)
        .await
}

/// Apply the embedded migrations (idempotent).
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}

/// Serialize any value to a JSONB-compatible `serde_json::Value`.
pub(crate) fn to_jsonb<T: serde::Serialize>(
    v: &T,
) -> Result<serde_json::Value, crate::ports::StorageError> {
    Ok(serde_json::to_value(v)?)
}

/// Parse a snake_case enum (e.g. a status string) back into its Rust type.
pub(crate) fn enum_from_str<T: serde::de::DeserializeOwned>(
    s: &str,
) -> Result<T, crate::ports::StorageError> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|e| crate::ports::StorageError::Database(format!("invalid enum '{s}': {e}")))
}
