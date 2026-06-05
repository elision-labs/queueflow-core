//! PGMQ-backed [`MessageQueue`].
//!
//! PGMQ gives us durable, at-least-once delivery with visibility timeouts and
//! native delayed sends — the latter is what makes retries survive restarts.
//!
//! Note: PGMQ is FIFO and has no native priority. Unlike the Go reference (which
//! incorrectly passed a job's priority as PGMQ's *delay* argument), this adapter
//! does not misuse the delay slot; `priority` is recorded on the job row but
//! dequeue order is FIFO. Use separate queues for priority classes.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use crate::ports::*;

/// Sends and reads messages via PGMQ.
#[derive(Clone)]
pub struct PostgresMessageQueue {
    pool: PgPool,
}

impl PostgresMessageQueue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a PGMQ queue if it does not already exist (idempotent).
    pub async fn ensure_queue(&self, queue: &str) -> Result<(), QueueError> {
        // pgmq.create raises if the queue exists on some versions; tolerate it.
        if let Err(e) = sqlx::query("SELECT pgmq.create($1)")
            .bind(queue)
            .execute(&self.pool)
            .await
        {
            tracing::debug!(queue, error = %e, "pgmq.create (likely already exists)");
        }
        Ok(())
    }
}

fn dq<E: std::fmt::Display>(e: E) -> QueueError {
    QueueError::Delivery(e.to_string())
}

#[async_trait]
impl MessageQueue for PostgresMessageQueue {
    async fn send(
        &self,
        queue: &str,
        msg: &QueueMessage,
        _priority: i32,
    ) -> Result<i64, QueueError> {
        let body = serde_json::to_value(msg)?;
        let row = sqlx::query("SELECT pgmq.send($1, $2::jsonb) AS msg_id")
            .bind(queue)
            .bind(body)
            .fetch_one(&self.pool)
            .await
            .map_err(dq)?;
        row.try_get::<i64, _>("msg_id").map_err(dq)
    }

    async fn send_delayed(
        &self,
        queue: &str,
        msg: &QueueMessage,
        _priority: i32,
        delay_secs: u64,
    ) -> Result<i64, QueueError> {
        let body = serde_json::to_value(msg)?;
        let row = sqlx::query("SELECT pgmq.send($1, $2::jsonb, $3::int) AS msg_id")
            .bind(queue)
            .bind(body)
            .bind(delay_secs.min(i32::MAX as u64) as i32)
            .fetch_one(&self.pool)
            .await
            .map_err(dq)?;
        row.try_get::<i64, _>("msg_id").map_err(dq)
    }

    async fn read(
        &self,
        queue: &str,
        vt_secs: u32,
        count: usize,
    ) -> Result<Vec<ReadMessage>, QueueError> {
        let rows = sqlx::query(
            "SELECT msg_id, read_ct, enqueued_at, message FROM pgmq.read($1, $2::int, $3::int)",
        )
        .bind(queue)
        .bind(vt_secs as i32)
        .bind(count as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(dq)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let msg_id: i64 = row.try_get("msg_id").map_err(dq)?;
            let read_ct: i32 = row.try_get("read_ct").map_err(dq)?;
            let enqueued_at: DateTime<Utc> = row.try_get("enqueued_at").map_err(dq)?;
            let body: serde_json::Value = row.try_get("message").map_err(dq)?;
            let message: QueueMessage = serde_json::from_value(body)?;
            out.push(ReadMessage {
                msg_id,
                message,
                read_count: read_ct.max(0) as u32,
                enqueued_at,
            });
        }
        Ok(out)
    }

    async fn delete(&self, queue: &str, msg_id: i64) -> Result<(), QueueError> {
        sqlx::query("SELECT pgmq.delete($1, $2::bigint)")
            .bind(queue)
            .bind(msg_id)
            .execute(&self.pool)
            .await
            .map_err(dq)?;
        Ok(())
    }

    async fn purge(&self, queue: &str) -> Result<u64, QueueError> {
        let row = sqlx::query("SELECT pgmq.purge_queue($1) AS purged")
            .bind(queue)
            .fetch_one(&self.pool)
            .await
            .map_err(dq)?;
        let purged: i64 = row.try_get("purged").map_err(dq)?;
        Ok(purged.max(0) as u64)
    }

    async fn queue_depth(&self, queue: &str) -> Result<u64, QueueError> {
        let row = sqlx::query("SELECT queue_length FROM pgmq.metrics($1)")
            .bind(queue)
            .fetch_optional(&self.pool)
            .await
            .map_err(dq)?;
        match row {
            Some(r) => {
                let len: i64 = r.try_get("queue_length").map_err(dq)?;
                Ok(len.max(0) as u64)
            }
            None => Ok(0),
        }
    }
}
