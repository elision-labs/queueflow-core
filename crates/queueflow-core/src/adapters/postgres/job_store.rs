//! PostgreSQL-backed [`JobStore`].
//!
//! The jobs table is also the queue: claims are `FOR UPDATE SKIP LOCKED`
//! against the partial `idx_jobs_claim` index, and lease ownership rides on a
//! random `lease_token` UUID stamped by every claim.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

use super::listener::WorkHub;
use super::{enum_from_str, to_jsonb};
use crate::domain::*;
use crate::ports::*;
use crate::stats::{QueueStats, StatsSnapshot};

/// Advisory lock key for the retention sweep ("qflow_rt" as big-endian bytes):
/// concurrent janitors must not race the same bulk delete.
const RETENTION_LOCK_KEY: i64 = 0x7166_6c6f_775f_7274;

/// Stores jobs and workflows in PostgreSQL.
#[derive(Clone)]
pub struct PostgresJobStore {
    pool: PgPool,
    hub: Arc<WorkHub>,
}

impl PostgresJobStore {
    pub fn new(pool: PgPool) -> Self {
        let hub = WorkHub::new(pool.clone());
        Self { pool, hub }
    }

    /// Delete every job on `queue` regardless of status; returns the number
    /// removed. Admin/test helper — the engine never calls this.
    pub async fn purge_queue(&self, queue: &str) -> Result<u64, StorageError> {
        let affected = sqlx::query("DELETE FROM queueflow.jobs WHERE queue_name = $1")
            .bind(queue)
            .execute(&self.pool)
            .await
            .map_err(db)?
            .rows_affected();
        Ok(affected)
    }
}

fn db<E: std::fmt::Display>(e: E) -> StorageError {
    StorageError::Database(e.to_string())
}

fn job_from_row(row: &sqlx::postgres::PgRow) -> Result<Job, StorageError> {
    let status: String = row.try_get("status").map_err(db)?;
    let payload: Json = row.try_get("payload").map_err(db)?;
    let config: Json = row.try_get("config").map_err(db)?;
    let metadata: Json = row.try_get("metadata").map_err(db)?;
    let result: Option<Json> = row.try_get("result").map_err(db)?;
    let retry_count: i32 = row.try_get("retry_count").map_err(db)?;
    let delivery_count: i32 = row.try_get("delivery_count").map_err(db)?;

    Ok(Job {
        id: row.try_get("id").map_err(db)?,
        queue_name: row.try_get("queue_name").map_err(db)?,
        task_name: row.try_get("task_name").map_err(db)?,
        payload: serde_json::from_value(payload)?,
        config: serde_json::from_value(config)?,
        status: enum_from_str(&status)?,
        created_at: row.try_get("created_at").map_err(db)?,
        scheduled_at: row.try_get("scheduled_at").map_err(db)?,
        started_at: row.try_get("started_at").map_err(db)?,
        completed_at: row.try_get("completed_at").map_err(db)?,
        delivery_count: delivery_count.max(0) as u32,
        error_message: row.try_get("error_message").map_err(db)?,
        retry_count: retry_count.max(0) as u32,
        next_retry_at: row.try_get("next_retry_at").map_err(db)?,
        workflow_id: row.try_get("workflow_id").map_err(db)?,
        workflow_step_id: row.try_get("workflow_step_id").map_err(db)?,
        result,
        metadata: serde_json::from_value(metadata)?,
        tenant_id: row.try_get("tenant_id").map_err(db)?,
        idempotency_key: row.try_get("idempotency_key").map_err(db)?,
    })
}

const JOB_COLUMNS: &str = "id, queue_name, task_name, payload, config, status, created_at, \
     scheduled_at, started_at, completed_at, delivery_count, error_message, retry_count, \
     next_retry_at, workflow_id, workflow_step_id, result, metadata, tenant_id, idempotency_key";

/// `JOB_COLUMNS` qualified with a table alias, for queries where an
/// unqualified name would be ambiguous (e.g. `UPDATE ... FROM ... RETURNING`).
fn job_columns_prefixed(alias: &str) -> String {
    JOB_COLUMNS
        .split(',')
        .map(|c| format!("{alias}.{}", c.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The claim statement, built once: it runs on every worker poll, so the
/// column-list formatting should not be repaid per call. SKIP LOCKED makes
/// concurrent claimers disjoint without blocking; the CTE-then-UPDATE shape
/// is the standard Postgres claim pattern. `$4` (cover_timeout) stretches
/// the lease to the job's configured timeout plus grace, so local workers
/// need no separate extension round trip.
static CLAIM_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "WITH c AS (\
             SELECT id FROM queueflow.jobs \
             WHERE queue_name = $1 AND status IN ('pending','retrying') \
               AND scheduled_at <= now() \
             ORDER BY priority DESC, scheduled_at, created_at \
             FOR UPDATE SKIP LOCKED \
             LIMIT $2\
         ) \
         UPDATE queueflow.jobs j \
         SET status = 'running', \
             started_at = COALESCE(j.started_at, now()), \
             locked_until = now() + make_interval(secs => CASE WHEN $4 THEN \
                 GREATEST($3, COALESCE((j.config->>'timeout_secs')::float8, 0) + {grace}) \
                 ELSE $3 END), \
             lease_token = gen_random_uuid(), \
             delivery_count = j.delivery_count + 1 \
         FROM c WHERE j.id = c.id \
         RETURNING {cols}, j.lease_token::text AS lease_token",
        grace = LEASE_GRACE_SECS,
        cols = job_columns_prefixed("j")
    )
});

/// The janitor's expired-lease reclaim, built once for the same reason.
static RECLAIM_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "WITH c AS (\
             SELECT id FROM queueflow.jobs \
             WHERE status = 'running' AND locked_until < now() \
             ORDER BY locked_until \
             FOR UPDATE SKIP LOCKED \
             LIMIT $1\
         ) \
         UPDATE queueflow.jobs j \
         SET lease_token = gen_random_uuid(), \
             locked_until = now() + make_interval(secs => $2) \
         FROM c WHERE j.id = c.id \
         RETURNING {cols}, j.lease_token::text AS lease_token",
        cols = job_columns_prefixed("j")
    )
});

/// Rows deleted per retention transaction. Bounding each round keeps the
/// delete's locks and WAL burst small even when a huge backlog expires at
/// once (e.g. the first sweep after enabling retention).
const PURGE_BATCH: i64 = 5_000;

/// Keyset-cursor predicate for text-id tables: continue strictly past
/// `(created_at, id)` in the filter's sort order. Applied to page queries
/// only — a requested total still counts the whole filtered set.
fn push_cursor_text(qb: &mut QueryBuilder<Postgres>, filter: &ListFilter) {
    if let Some(c) = &filter.after {
        qb.push(if filter.order_desc {
            " AND (created_at, id) < ("
        } else {
            " AND (created_at, id) > ("
        });
        qb.push_bind(c.created_at);
        qb.push(", ");
        qb.push_bind(c.id.clone());
        qb.push(")");
    }
}

/// The effective OFFSET: ignored when a cursor positions the page instead.
fn effective_offset(filter: &ListFilter) -> i64 {
    if filter.after.is_some() {
        0
    } else {
        filter.offset.max(0)
    }
}

/// Insert one job row. `ON CONFLICT DO NOTHING` absorbs an idempotency-key
/// collision (the partial unique index from migration 0002); returns whether a
/// row was actually inserted. Generic over the executor so it can run both
/// standalone and inside the `create_step_job` transaction.
async fn insert_job<'e, E>(executor: E, job: &Job) -> Result<bool, StorageError>
where
    E: sqlx::PgExecutor<'e>,
{
    let affected = sqlx::query(
        "INSERT INTO queueflow.jobs (id, queue_name, task_name, payload, config, status, priority, \
         created_at, scheduled_at, started_at, completed_at, error_message, retry_count, next_retry_at, \
         workflow_id, workflow_step_id, result, metadata, tenant_id, idempotency_key) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) \
         ON CONFLICT DO NOTHING",
    )
    .bind(&job.id)
    .bind(&job.queue_name)
    .bind(&job.task_name)
    .bind(to_jsonb(&job.payload)?)
    .bind(to_jsonb(&job.config)?)
    .bind(job.status.as_str())
    .bind(job.config.priority)
    .bind(job.created_at)
    .bind(job.scheduled_at)
    .bind(job.started_at)
    .bind(job.completed_at)
    .bind(job.error_message.as_deref())
    .bind(job.retry_count as i32)
    .bind(job.next_retry_at)
    .bind(job.workflow_id.as_deref())
    .bind(job.workflow_step_id.as_deref())
    .bind(job.result.clone())
    .bind(to_jsonb(&job.metadata)?)
    .bind(job.tenant_id.as_deref())
    .bind(job.idempotency_key.as_deref())
    .execute(executor)
    .await
    .map_err(db)?
    .rows_affected();
    Ok(affected > 0)
}

#[async_trait]
impl JobStore for PostgresJobStore {
    async fn create_job(&self, job: &Job) -> Result<bool, StorageError> {
        insert_job(&self.pool, job).await
    }

    async fn find_job_by_idempotency_key(
        &self,
        tenant_id: Option<&str>,
        key: &str,
    ) -> Result<Option<Job>, StorageError> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {JOB_COLUMNS} FROM queueflow.jobs \
             WHERE COALESCE(tenant_id, '') = COALESCE($1, '') AND idempotency_key = $2"
        )))
        .bind(tenant_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        row.as_ref().map(job_from_row).transpose()
    }

    /// One multi-row INSERT via UNNEST: a 1000-job batch is a single round
    /// trip instead of 1000 — and a single NOTIFY per queue, thanks to
    /// per-transaction dedup.
    async fn batch_create_jobs(&self, jobs: &[Job]) -> Result<Vec<String>, StorageError> {
        if jobs.is_empty() {
            return Ok(vec![]);
        }
        let n = jobs.len();
        let mut ids = Vec::with_capacity(n);
        let mut queues = Vec::with_capacity(n);
        let mut tasks = Vec::with_capacity(n);
        let mut payloads = Vec::with_capacity(n);
        let mut configs = Vec::with_capacity(n);
        let mut priorities = Vec::with_capacity(n);
        let mut created = Vec::with_capacity(n);
        let mut scheduled = Vec::with_capacity(n);
        let mut metadatas = Vec::with_capacity(n);
        let mut tenants: Vec<Option<String>> = Vec::with_capacity(n);
        for job in jobs {
            ids.push(job.id.clone());
            queues.push(job.queue_name.clone());
            tasks.push(job.task_name.clone());
            payloads.push(to_jsonb(&job.payload)?);
            configs.push(to_jsonb(&job.config)?);
            priorities.push(job.config.priority);
            created.push(job.created_at);
            scheduled.push(job.scheduled_at);
            metadatas.push(to_jsonb(&job.metadata)?);
            tenants.push(job.tenant_id.clone());
        }
        sqlx::query(
            "INSERT INTO queueflow.jobs (id, queue_name, task_name, payload, config, status, \
             priority, created_at, scheduled_at, retry_count, metadata, tenant_id) \
             SELECT u.id, u.queue_name, u.task_name, u.payload, u.config, 'pending', \
             u.priority, u.created_at, u.scheduled_at, 0, u.metadata, u.tenant_id \
             FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[], $5::jsonb[], \
             $6::int[], $7::timestamptz[], $8::timestamptz[], $9::jsonb[], $10::text[]) \
             AS u(id, queue_name, task_name, payload, config, priority, created_at, scheduled_at, metadata, tenant_id)",
        )
        .bind(&ids)
        .bind(&queues)
        .bind(&tasks)
        .bind(&payloads)
        .bind(&configs)
        .bind(&priorities)
        .bind(&created)
        .bind(&scheduled)
        .bind(&metadatas)
        .bind(&tenants)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(ids)
    }

    async fn get_job(&self, id: &str) -> Result<Job, StorageError> {
        // Only interpolates the JOB_COLUMNS constant; no user input.
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {JOB_COLUMNS} FROM queueflow.jobs WHERE id = $1"
        )))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?
        .ok_or_else(|| StorageError::JobNotFound(id.to_string()))?;
        job_from_row(&row)
    }

    async fn list_jobs(&self, filter: &ListFilter) -> Result<Page<Job>, StorageError> {
        // Exact totals are opt-in: a COUNT(*) over the filtered set is the
        // most expensive part of listing on large tables.
        let total = if filter.include_total {
            let mut cb: QueryBuilder<Postgres> =
                QueryBuilder::new("SELECT COUNT(*) FROM queueflow.jobs WHERE 1=1");
            push_job_filters(&mut cb, filter);
            Some(
                cb.build()
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db)?
                    .try_get(0)
                    .map_err(db)?,
            )
        } else {
            None
        };

        // Page: fetch limit+1 rows so has_more needs no count query.
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(format!(
            "SELECT {JOB_COLUMNS} FROM queueflow.jobs WHERE 1=1"
        ));
        push_job_filters(&mut qb, filter);
        push_cursor_text(&mut qb, filter);
        qb.push(if filter.order_desc {
            " ORDER BY created_at DESC, id DESC"
        } else {
            " ORDER BY created_at ASC, id ASC"
        });
        let limit = if filter.limit <= 0 { 50 } else { filter.limit };
        qb.push(" LIMIT ").push_bind(limit + 1);
        qb.push(" OFFSET ").push_bind(effective_offset(filter));

        let rows = qb.build().fetch_all(&self.pool).await.map_err(db)?;
        let mut jobs = rows
            .iter()
            .map(job_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = jobs.len() as i64 > limit;
        jobs.truncate(limit as usize);
        Ok(Page {
            items: jobs,
            has_more,
            total,
        })
    }

    // ---- Claim / lease ------------------------------------------------------

    async fn claim_jobs(
        &self,
        queue: &str,
        count: usize,
        lease_secs: u32,
        cover_timeout: bool,
    ) -> Result<Claimed, StorageError> {
        // Snapshot the queue's epoch before the claim runs: a NOTIFY that
        // races this query either made its job visible to the claim or
        // advanced the epoch, so the caller's await_work cannot lose it.
        let epoch = self.hub.epoch(queue);
        let rows = sqlx::query(CLAIM_SQL.as_str())
            .bind(queue)
            .bind(count as i64)
            .bind(lease_secs as f64)
            .bind(cover_timeout)
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in &rows {
            jobs.push(LeasedJob {
                job: job_from_row(row)?,
                lease_token: row.try_get("lease_token").map_err(db)?,
            });
        }

        // Only when empty-handed: when is the next delayed job due?
        // idx_jobs_next_due (queue_name, scheduled_at, partial) serves this
        // as a cheap first-row index probe however deep the backlog.
        let next_due = if jobs.is_empty() {
            sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                "SELECT MIN(scheduled_at) FROM queueflow.jobs \
                 WHERE queue_name = $1 AND status IN ('pending','retrying') \
                   AND scheduled_at > now()",
            )
            .bind(queue)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?
        } else {
            None
        };

        Ok(Claimed {
            jobs,
            next_due,
            epoch,
        })
    }

    async fn extend_lease(
        &self,
        job_id: &str,
        token: &str,
        lease_secs: u32,
    ) -> Result<Option<JobStatus>, StorageError> {
        let affected = sqlx::query(
            "UPDATE queueflow.jobs \
             SET locked_until = now() + make_interval(secs => $3) \
             WHERE id = $1 AND status = 'running' AND lease_token::text = $2",
        )
        .bind(job_id)
        .bind(token)
        .bind(lease_secs as f64)
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        if affected > 0 {
            return Ok(Some(JobStatus::Running));
        }
        // Not extended: missing, finished/cancelled, or reclaimed. Load the
        // status so the caller (heartbeat) can tell the worker what happened.
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM queueflow.jobs WHERE id = $1")
                .bind(job_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db)?;
        match status {
            None => Err(StorageError::JobNotFound(job_id.to_string())),
            Some(s) => {
                let status: JobStatus = enum_from_str(&s)?;
                if status == JobStatus::Running {
                    Ok(None) // still running, but under someone else's token
                } else {
                    Ok(Some(status))
                }
            }
        }
    }

    async fn finish_if_leased(
        &self,
        job_id: &str,
        token: &str,
        status: JobStatus,
        error: Option<&str>,
        result: Option<&Json>,
    ) -> Result<Option<FinishedJob>, StorageError> {
        debug_assert!(status.is_terminal());
        // RETURNING the workflow linkage saves callers advancing a workflow
        // (the remote-worker complete path) a re-read of the row.
        let row = sqlx::query(
            "UPDATE queueflow.jobs \
             SET status = $3, \
                 error_message = COALESCE($4, error_message), \
                 result = COALESCE($5, result), \
                 completed_at = now(), \
                 locked_until = NULL, lease_token = NULL \
             WHERE id = $1 AND status = 'running' AND lease_token::text = $2 \
             RETURNING workflow_id, workflow_step_id",
        )
        .bind(job_id)
        .bind(token)
        .bind(status.as_str())
        .bind(error)
        .bind(result.cloned())
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        row.map(|r| {
            Ok(FinishedJob {
                workflow_id: r.try_get("workflow_id").map_err(db)?,
                workflow_step_id: r.try_get("workflow_step_id").map_err(db)?,
            })
        })
        .transpose()
    }

    async fn await_work(
        &self,
        queue: &str,
        since_epoch: u64,
        max_wait: Duration,
    ) -> Result<(), StorageError> {
        self.hub.await_work(queue, since_epoch, max_wait).await;
        Ok(())
    }

    async fn await_job_change(&self, job_id: &str, max_wait: Duration) -> Result<(), StorageError> {
        self.hub.await_job_change(job_id, max_wait).await;
        Ok(())
    }

    async fn cancel_job_if_active(&self, id: &str, reason: &str) -> Result<bool, StorageError> {
        // Check-and-cancel in one statement so a concurrently completing
        // worker can never have its terminal status overwritten. The lease is
        // cleared but the status flip alone already blocks finish_if_leased.
        let affected = sqlx::query(
            "UPDATE queueflow.jobs SET status = 'cancelled', error_message = $2, \
             completed_at = now(), locked_until = NULL, lease_token = NULL \
             WHERE id = $1 AND status NOT IN ('completed','failed','cancelled')",
        )
        .bind(id)
        .bind(reason)
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(affected > 0)
    }

    async fn mark_retrying(
        &self,
        id: &str,
        token: &str,
        retry_count: u32,
        next_retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<bool, StorageError> {
        // One row update is the whole durable retry: the backoff delay lives
        // in scheduled_at, and releasing the lease happens in the same write.
        // Lease-guarded so a retry can never resurrect a cancelled job.
        let affected = sqlx::query(
            "UPDATE queueflow.jobs SET status = 'retrying', retry_count = $3, \
             next_retry_at = $4, scheduled_at = $4, error_message = $5, \
             locked_until = NULL, lease_token = NULL \
             WHERE id = $1 AND status = 'running' AND lease_token::text = $2",
        )
        .bind(id)
        .bind(token)
        .bind(retry_count as i32)
        .bind(next_retry_at)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(affected > 0)
    }

    async fn move_to_dlq(&self, id: &str, reason: &str, error: &str) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO queueflow.dead_letters (job_id, queue_name, task_name, reason, error_message, tenant_id) \
             SELECT id, queue_name, task_name, $2, $3, tenant_id FROM queueflow.jobs WHERE id = $1",
        )
        .bind(id)
        .bind(reason)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(())
    }

    async fn count_dead_letters(&self) -> Result<i64, StorageError> {
        let row = sqlx::query("SELECT COUNT(*) FROM queueflow.dead_letters")
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        row.try_get(0).map_err(db)
    }

    async fn queue_stats(
        &self,
        tenant_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<Vec<QueueStats>, StorageError> {
        // Non-terminal rows only, so the status index bounds the scan however
        // large the history grows.
        let rows = sqlx::query(
            "SELECT queue_name, \
                    COUNT(*) FILTER (WHERE status IN ('pending','retrying') AND scheduled_at <= $2) AS pending, \
                    COUNT(*) FILTER (WHERE status IN ('pending','retrying') AND scheduled_at >  $2) AS scheduled, \
                    COUNT(*) FILTER (WHERE status = 'running') AS running, \
                    MIN(scheduled_at) FILTER (WHERE status IN ('pending','retrying') AND scheduled_at <= $2) AS oldest \
             FROM queueflow.jobs \
             WHERE status IN ('pending','retrying','running') \
               AND ($1::text IS NULL OR tenant_id = $1) \
             GROUP BY queue_name ORDER BY queue_name",
        )
        .bind(tenant_id)
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter()
            .map(|r| {
                let n = |col: &str| -> Result<u64, StorageError> {
                    r.try_get::<i64, _>(col)
                        .map(|v| v.max(0) as u64)
                        .map_err(db)
                };
                let oldest: Option<DateTime<Utc>> = r.try_get("oldest").map_err(db)?;
                Ok(QueueStats {
                    queue: r.try_get("queue_name").map_err(db)?,
                    pending: n("pending")?,
                    scheduled: n("scheduled")?,
                    running: n("running")?,
                    oldest_pending_age_secs: oldest.map(|o| (now - o).num_seconds().max(0) as u64),
                })
            })
            .collect()
    }

    async fn count_stats(&self, tenant_id: Option<&str>) -> Result<StatsSnapshot, StorageError> {
        // `$1 IS NULL OR tenant_id = $1` keeps one statement for both the
        // scoped and the unscoped read; the scoped case uses idx_jobs_tenant.
        let jobs = sqlx::query(
            "SELECT COUNT(*) AS created, \
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed, \
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed, \
                    COALESCE(SUM(retry_count), 0)::BIGINT AS retried \
             FROM queueflow.jobs WHERE ($1::text IS NULL OR tenant_id = $1)",
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;
        let workflows = sqlx::query(
            "SELECT COUNT(*) AS created, \
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed, \
                    COUNT(*) FILTER (WHERE status IN ('failed', 'partially_failed')) AS failed \
             FROM queueflow.workflows WHERE ($1::text IS NULL OR tenant_id = $1)",
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;
        let dead: i64 = sqlx::query(
            "SELECT COUNT(*) FROM queueflow.dead_letters WHERE ($1::text IS NULL OR tenant_id = $1)",
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db)?
        .try_get(0)
        .map_err(db)?;
        let n = |row: &sqlx::postgres::PgRow, col: &str| -> Result<u64, StorageError> {
            row.try_get::<i64, _>(col)
                .map(|v| v.max(0) as u64)
                .map_err(db)
        };
        Ok(StatsSnapshot {
            jobs_created: n(&jobs, "created")?,
            jobs_completed: n(&jobs, "completed")?,
            jobs_failed: n(&jobs, "failed")?,
            jobs_retried: n(&jobs, "retried")?,
            jobs_dead_lettered: dead.max(0) as u64,
            workflows_created: n(&workflows, "created")?,
            workflows_completed: n(&workflows, "completed")?,
            workflows_failed: n(&workflows, "failed")?,
        })
    }

    async fn list_dead_letters(
        &self,
        filter: &ListFilter,
    ) -> Result<Page<DeadLetter>, StorageError> {
        let total = if filter.include_total {
            let mut cb: QueryBuilder<Postgres> =
                QueryBuilder::new("SELECT COUNT(*) FROM queueflow.dead_letters WHERE 1=1");
            push_dlq_filters(&mut cb, filter);
            Some(
                cb.build()
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db)?
                    .try_get(0)
                    .map_err(db)?,
            )
        } else {
            None
        };

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(format!(
            "SELECT {DLQ_COLUMNS} FROM queueflow.dead_letters WHERE 1=1"
        ));
        push_dlq_filters(&mut qb, filter);
        // Dead-letter ids are BIGSERIAL; the cursor carries them in decimal.
        if let Some(c) = &filter.after {
            if let Ok(id) = c.id.parse::<i64>() {
                qb.push(if filter.order_desc {
                    " AND (created_at, id) < ("
                } else {
                    " AND (created_at, id) > ("
                });
                qb.push_bind(c.created_at);
                qb.push(", ");
                qb.push_bind(id);
                qb.push(")");
            }
        }
        qb.push(if filter.order_desc {
            " ORDER BY created_at DESC, id DESC"
        } else {
            " ORDER BY created_at ASC, id ASC"
        });
        let limit = if filter.limit <= 0 { 50 } else { filter.limit };
        qb.push(" LIMIT ").push_bind(limit + 1);
        qb.push(" OFFSET ").push_bind(effective_offset(filter));

        let rows = qb.build().fetch_all(&self.pool).await.map_err(db)?;
        let mut items = rows
            .iter()
            .map(dead_letter_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = items.len() as i64 > limit;
        items.truncate(limit as usize);
        Ok(Page {
            items,
            has_more,
            total,
        })
    }

    async fn get_dead_letter(&self, id: i64) -> Result<DeadLetter, StorageError> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {DLQ_COLUMNS} FROM queueflow.dead_letters WHERE id = $1"
        )))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?
        .ok_or(StorageError::DeadLetterNotFound(id))?;
        dead_letter_from_row(&row)
    }

    // ---- Cron schedules -----------------------------------------------------

    async fn create_cron(&self, cron: &CronSchedule) -> Result<bool, StorageError> {
        // ON CONFLICT DO NOTHING absorbs a (tenant, name) collision via the
        // partial unique index from migration 0004.
        let affected = sqlx::query(
            "INSERT INTO queueflow.cron_schedules \
             (id, name, cron_expr, task_name, payload, config, queue_name, tenant_id, enabled, \
              next_run_at, last_enqueued_at, created_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
             ON CONFLICT DO NOTHING",
        )
        .bind(&cron.id)
        .bind(&cron.name)
        .bind(&cron.cron_expr)
        .bind(&cron.task_name)
        .bind(to_jsonb(&cron.payload)?)
        .bind(cron.config.as_ref().map(to_jsonb).transpose()?)
        .bind(cron.queue_name.as_deref())
        .bind(cron.tenant_id.as_deref())
        .bind(cron.enabled)
        .bind(cron.next_run_at)
        .bind(cron.last_enqueued_at)
        .bind(cron.created_at)
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(affected > 0)
    }

    async fn get_cron(&self, id: &str) -> Result<CronSchedule, StorageError> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {CRON_COLUMNS} FROM queueflow.cron_schedules WHERE id = $1"
        )))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?
        .ok_or_else(|| StorageError::CronNotFound(id.to_string()))?;
        cron_from_row(&row)
    }

    async fn list_crons(&self, filter: &ListFilter) -> Result<Page<CronSchedule>, StorageError> {
        let total = if filter.include_total {
            let mut cb: QueryBuilder<Postgres> =
                QueryBuilder::new("SELECT COUNT(*) FROM queueflow.cron_schedules WHERE 1=1");
            push_cron_filters(&mut cb, filter);
            Some(
                cb.build()
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db)?
                    .try_get(0)
                    .map_err(db)?,
            )
        } else {
            None
        };

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(format!(
            "SELECT {CRON_COLUMNS} FROM queueflow.cron_schedules WHERE 1=1"
        ));
        push_cron_filters(&mut qb, filter);
        push_cursor_text(&mut qb, filter);
        qb.push(if filter.order_desc {
            " ORDER BY created_at DESC, id DESC"
        } else {
            " ORDER BY created_at ASC, id ASC"
        });
        let limit = if filter.limit <= 0 { 50 } else { filter.limit };
        qb.push(" LIMIT ").push_bind(limit + 1);
        qb.push(" OFFSET ").push_bind(effective_offset(filter));

        let rows = qb.build().fetch_all(&self.pool).await.map_err(db)?;
        let mut items = rows
            .iter()
            .map(cron_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = items.len() as i64 > limit;
        items.truncate(limit as usize);
        Ok(Page {
            items,
            has_more,
            total,
        })
    }

    async fn delete_cron(&self, id: &str) -> Result<bool, StorageError> {
        let affected = sqlx::query("DELETE FROM queueflow.cron_schedules WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(db)?
            .rows_affected();
        Ok(affected > 0)
    }

    async fn set_cron_enabled(
        &self,
        id: &str,
        enabled: bool,
        next_run_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let affected = sqlx::query(
            "UPDATE queueflow.cron_schedules SET enabled = $2, next_run_at = $3 WHERE id = $1",
        )
        .bind(id)
        .bind(enabled)
        .bind(next_run_at)
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(affected > 0)
    }

    async fn due_crons(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {CRON_COLUMNS} FROM queueflow.cron_schedules \
             WHERE enabled AND next_run_at <= $1 \
             ORDER BY next_run_at \
             LIMIT $2"
        )))
        .bind(now)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(cron_from_row).collect()
    }

    async fn advance_cron(
        &self,
        id: &str,
        fired_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE queueflow.cron_schedules \
             SET last_enqueued_at = $2, next_run_at = $3 WHERE id = $1",
        )
        .bind(id)
        .bind(fired_at)
        .bind(next_run_at)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(())
    }

    async fn replay_dead_letter(&self, id: i64, replacement: &Job) -> Result<bool, StorageError> {
        // Claim (`replayed_at IS NULL`) and insert commit together, so a dead
        // letter can never spawn two replays and the replacement is only ever
        // claimable once the claim won.
        let mut tx = self.pool.begin().await.map_err(db)?;
        let claimed = sqlx::query(
            "UPDATE queueflow.dead_letters \
             SET replayed_at = now(), replay_job_id = $2 \
             WHERE id = $1 AND replayed_at IS NULL",
        )
        .bind(id)
        .bind(&replacement.id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if claimed == 0 {
            tx.rollback().await.map_err(db)?;
            return Ok(false);
        }
        insert_job(&mut *tx, replacement).await?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    async fn ping(&self) -> Result<(), StorageError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(db)?;
        Ok(())
    }

    // ---- Janitor sweeps -----------------------------------------------------

    async fn claim_expired_leases(
        &self,
        limit: usize,
        lease_secs: u32,
    ) -> Result<Vec<LeasedJob>, StorageError> {
        let rows = sqlx::query(RECLAIM_SQL.as_str())
            .bind(limit as i64)
            .bind(lease_secs as f64)
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(LeasedJob {
                job: job_from_row(row)?,
                lease_token: row.try_get("lease_token").map_err(db)?,
            });
        }
        Ok(out)
    }

    async fn stalled_step_jobs(&self, limit: usize) -> Result<Vec<Job>, StorageError> {
        // Driven from the (few) non-terminal steps: the positive status list
        // is exhaustive and keeps idx_workflow_steps_status usable, so this
        // sweep stays cheap however much terminal history accumulates.
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {} FROM queueflow.workflow_steps s \
             JOIN queueflow.jobs j ON j.id = s.job_id \
             WHERE s.status IN ('pending','running') \
               AND j.status IN ('completed','failed','cancelled') \
             LIMIT $1",
            job_columns_prefixed("j")
        )))
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(job_from_row).collect()
    }

    async fn stalled_workflow_ids(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        // Positive status lists (exhaustive per the enums) keep the status
        // indexes usable; NOT IN forms would force scans as history grows.
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT w.id FROM queueflow.workflows w \
             WHERE w.status IN ('created','running') \
               AND NOT EXISTS (\
                   SELECT 1 FROM queueflow.workflow_steps s \
                   JOIN queueflow.jobs j ON j.id = s.job_id \
                   WHERE s.workflow_id = w.id \
                     AND j.status IN ('pending','running','retrying')\
               ) \
             LIMIT $1",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        Ok(rows)
    }

    async fn purge_terminal(&self, older_than: DateTime<Utc>) -> Result<u64, StorageError> {
        // Deleting in bounded rounds keeps each transaction's locks and WAL
        // burst small even when an enormous backlog expires at once. Each
        // round re-takes the xact-scoped advisory lock (released on
        // commit/rollback, so a crashed janitor can never wedge retention);
        // losing it mid-way means another janitor took over.
        let mut purged = 0u64;
        loop {
            let mut tx = self.pool.begin().await.map_err(db)?;
            let won: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
                .bind(RETENTION_LOCK_KEY)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
            if !won {
                tx.rollback().await.map_err(db)?;
                return Ok(purged);
            }
            let jobs = sqlx::query(
                "DELETE FROM queueflow.jobs WHERE id IN (\
                     SELECT id FROM queueflow.jobs \
                     WHERE status IN ('completed','failed','cancelled') \
                       AND COALESCE(completed_at, created_at) < $1 \
                     LIMIT $2)",
            )
            .bind(older_than)
            .bind(PURGE_BATCH)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
            // Steps go with their workflow via ON DELETE CASCADE.
            let workflows = sqlx::query(
                "DELETE FROM queueflow.workflows WHERE id IN (\
                     SELECT id FROM queueflow.workflows \
                     WHERE status IN ('completed','failed','partially_failed','cancelled') \
                       AND COALESCE(completed_at, created_at) < $1 \
                     LIMIT $2)",
            )
            .bind(older_than)
            .bind(PURGE_BATCH)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
            let dead_letters = sqlx::query(
                "DELETE FROM queueflow.dead_letters WHERE id IN (\
                     SELECT id FROM queueflow.dead_letters \
                     WHERE created_at < $1 \
                     LIMIT $2)",
            )
            .bind(older_than)
            .bind(PURGE_BATCH)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
            tx.commit().await.map_err(db)?;

            purged += jobs;
            let batch = PURGE_BATCH as u64;
            if jobs < batch && workflows < batch && dead_letters < batch {
                return Ok(purged);
            }
        }
    }

    // ---- Workflows ----------------------------------------------------------

    async fn create_workflow(&self, wf: &Workflow) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query(
            "INSERT INTO queueflow.workflows (id, name, status, created_at, context, metadata, tenant_id) \
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(&wf.id)
        .bind(&wf.name)
        .bind(wf.status.as_str())
        .bind(wf.created_at)
        .bind(to_jsonb(&wf.context)?)
        .bind(to_jsonb(&wf.metadata)?)
        .bind(wf.tenant_id.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(db)?;

        for (idx, step) in wf.steps.iter().enumerate() {
            sqlx::query(
                "INSERT INTO queueflow.workflow_steps (workflow_id, name, idx, task_name, payload, \
                 depends_on, config, on_success, on_failure, status, metadata) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',$10)",
            )
            .bind(&wf.id)
            .bind(&step.name)
            .bind(idx as i32)
            .bind(&step.task_name)
            .bind(to_jsonb(&step.payload)?)
            .bind(to_jsonb(&step.depends_on)?)
            .bind(step.config.as_ref().map(to_jsonb).transpose()?)
            .bind(on_success_str(step.on_success))
            .bind(on_failure_str(step.on_failure))
            .bind(to_jsonb(&step.metadata)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    async fn get_workflow_status(&self, id: &str) -> Result<WorkflowStatus, StorageError> {
        let status: String =
            sqlx::query_scalar("SELECT status FROM queueflow.workflows WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db)?
                .ok_or_else(|| StorageError::WorkflowNotFound(id.to_string()))?;
        enum_from_str(&status)
    }

    async fn get_workflow_context(&self, id: &str) -> Result<Map, StorageError> {
        let context: Json =
            sqlx::query_scalar("SELECT context FROM queueflow.workflows WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db)?
                .ok_or_else(|| StorageError::WorkflowNotFound(id.to_string()))?;
        Ok(serde_json::from_value(context)?)
    }

    async fn get_workflow(&self, id: &str) -> Result<Workflow, StorageError> {
        let row = sqlx::query(
            "SELECT id, name, status, created_at, started_at, completed_at, context, metadata, tenant_id \
             FROM queueflow.workflows WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?
        .ok_or_else(|| StorageError::WorkflowNotFound(id.to_string()))?;

        let step_rows = sqlx::query(
            "SELECT name, task_name, payload, depends_on, config, on_success, on_failure, metadata \
             FROM queueflow.workflow_steps WHERE workflow_id = $1 ORDER BY idx ASC",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;

        let steps = step_rows
            .iter()
            .map(step_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        workflow_from_row(&row, steps)
    }

    /// Two queries total (page of workflows + all their steps via `ANY`),
    /// regardless of page size — not one per workflow.
    async fn list_workflows(&self, filter: &ListFilter) -> Result<Page<Workflow>, StorageError> {
        let total = if filter.include_total {
            let mut cb: QueryBuilder<Postgres> =
                QueryBuilder::new("SELECT COUNT(*) FROM queueflow.workflows WHERE 1=1");
            push_workflow_filters(&mut cb, filter);
            Some(
                cb.build()
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db)?
                    .try_get(0)
                    .map_err(db)?,
            )
        } else {
            None
        };

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "SELECT id, name, status, created_at, started_at, completed_at, context, metadata, tenant_id \
             FROM queueflow.workflows WHERE 1=1",
        );
        push_workflow_filters(&mut qb, filter);
        push_cursor_text(&mut qb, filter);
        qb.push(if filter.order_desc {
            " ORDER BY created_at DESC, id DESC"
        } else {
            " ORDER BY created_at ASC, id ASC"
        });
        let limit = if filter.limit <= 0 { 50 } else { filter.limit };
        qb.push(" LIMIT ").push_bind(limit + 1);
        qb.push(" OFFSET ").push_bind(effective_offset(filter));

        let mut rows = qb.build().fetch_all(&self.pool).await.map_err(db)?;
        let has_more = rows.len() as i64 > limit;
        rows.truncate(limit as usize);

        let ids: Vec<String> = rows
            .iter()
            .map(|r| r.try_get::<String, _>("id").map_err(db))
            .collect::<Result<_, _>>()?;

        // All steps for the whole page in one query, grouped per workflow.
        let mut steps_by_wf: std::collections::HashMap<String, Vec<WorkflowStep>> =
            std::collections::HashMap::new();
        if !ids.is_empty() {
            let step_rows = sqlx::query(
                "SELECT workflow_id, name, task_name, payload, depends_on, config, on_success, \
                 on_failure, metadata \
                 FROM queueflow.workflow_steps WHERE workflow_id = ANY($1) \
                 ORDER BY workflow_id, idx ASC",
            )
            .bind(&ids)
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;
            for r in &step_rows {
                let wf_id: String = r.try_get("workflow_id").map_err(db)?;
                steps_by_wf
                    .entry(wf_id)
                    .or_default()
                    .push(step_from_row(r)?);
            }
        }

        let mut workflows = Vec::with_capacity(rows.len());
        for row in &rows {
            let id: String = row.try_get("id").map_err(db)?;
            let steps = steps_by_wf.remove(&id).unwrap_or_default();
            workflows.push(workflow_from_row(row, steps)?);
        }
        Ok(Page {
            items: workflows,
            has_more,
            total,
        })
    }

    async fn workflow_step_statuses(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StepRecord>, StorageError> {
        let rows = sqlx::query(
            "SELECT name, status, job_id FROM queueflow.workflow_steps \
             WHERE workflow_id = $1 ORDER BY idx ASC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;

        rows.iter()
            .map(|r| {
                let status: String = r.try_get("status").map_err(db)?;
                Ok(StepRecord {
                    name: r.try_get("name").map_err(db)?,
                    status: enum_from_str(&status)?,
                    job_id: r.try_get("job_id").map_err(db)?,
                })
            })
            .collect()
    }

    async fn link_step_job(
        &self,
        workflow_id: &str,
        step_name: &str,
        job_id: &str,
    ) -> Result<bool, StorageError> {
        // `job_id IS NULL` makes this a claim: exactly one concurrent advance
        // can win, so a step is never enqueued twice.
        let affected = sqlx::query(
            "UPDATE queueflow.workflow_steps SET job_id = $3 \
             WHERE workflow_id = $1 AND name = $2 AND job_id IS NULL",
        )
        .bind(workflow_id)
        .bind(step_name)
        .bind(job_id)
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(affected > 0)
    }

    async fn create_step_job(&self, job: &Job) -> Result<bool, StorageError> {
        let wf_id = job.workflow_id.as_deref().unwrap_or_default();
        let step_name = job.workflow_step_id.as_deref().unwrap_or_default();
        // Claim and insert commit together: the job only ever exists linked
        // to the step it won, and a loser persists nothing.
        let mut tx = self.pool.begin().await.map_err(db)?;
        let claimed = sqlx::query(
            "UPDATE queueflow.workflow_steps SET job_id = $3 \
             WHERE workflow_id = $1 AND name = $2 AND job_id IS NULL",
        )
        .bind(wf_id)
        .bind(step_name)
        .bind(&job.id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if claimed == 0 {
            tx.rollback().await.map_err(db)?;
            return Ok(false);
        }
        insert_job(&mut *tx, job).await?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    async fn set_step_status(
        &self,
        workflow_id: &str,
        step_name: &str,
        status: StepStatus,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE queueflow.workflow_steps SET status = $3, \
             error_message = COALESCE($4, error_message) \
             WHERE workflow_id = $1 AND name = $2",
        )
        .bind(workflow_id)
        .bind(step_name)
        .bind(status.as_str())
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(())
    }

    async fn set_workflow_status(
        &self,
        workflow_id: &str,
        status: WorkflowStatus,
    ) -> Result<bool, StorageError> {
        // Terminal workflows are never overwritten, and a same-status write
        // reports false so concurrent aggregations count each transition once.
        let affected = sqlx::query(
            "UPDATE queueflow.workflows SET status = $2, \
             started_at = CASE WHEN $2 = 'running' AND started_at IS NULL THEN now() ELSE started_at END, \
             completed_at = CASE WHEN $2 IN ('completed','failed','partially_failed','cancelled') THEN now() ELSE completed_at END \
             WHERE id = $1 AND status <> $2 \
             AND status NOT IN ('completed','failed','partially_failed','cancelled')",
        )
        .bind(workflow_id)
        .bind(status.as_str())
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(affected > 0)
    }

    async fn merge_workflow_context(
        &self,
        workflow_id: &str,
        key: &str,
        value: &Json,
    ) -> Result<(), StorageError> {
        // jsonb_set to merge a single key into the context object.
        sqlx::query(
            "UPDATE queueflow.workflows \
             SET context = jsonb_set(COALESCE(context, '{}'::jsonb), ARRAY[$2], $3, true) \
             WHERE id = $1",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(())
    }
}

/// Map a `workflow_steps` row (without runtime status columns) to a step.
fn step_from_row(r: &sqlx::postgres::PgRow) -> Result<WorkflowStep, StorageError> {
    let payload: Json = r.try_get("payload").map_err(db)?;
    let depends_on: Json = r.try_get("depends_on").map_err(db)?;
    let config: Option<Json> = r.try_get("config").map_err(db)?;
    let metadata: Json = r.try_get("metadata").map_err(db)?;
    let on_success: String = r.try_get("on_success").map_err(db)?;
    let on_failure: String = r.try_get("on_failure").map_err(db)?;
    Ok(WorkflowStep {
        name: r.try_get("name").map_err(db)?,
        task_name: r.try_get("task_name").map_err(db)?,
        payload: serde_json::from_value(payload)?,
        depends_on: serde_json::from_value(depends_on)?,
        config: config.map(serde_json::from_value).transpose()?,
        on_success: enum_from_str(&on_success)?,
        on_failure: enum_from_str(&on_failure)?,
        metadata: serde_json::from_value(metadata)?,
    })
}

/// Map a `workflows` row plus its (pre-fetched) steps to a [`Workflow`].
fn workflow_from_row(
    row: &sqlx::postgres::PgRow,
    steps: Vec<WorkflowStep>,
) -> Result<Workflow, StorageError> {
    let status: String = row.try_get("status").map_err(db)?;
    let context: Json = row.try_get("context").map_err(db)?;
    let metadata: Json = row.try_get("metadata").map_err(db)?;
    Ok(Workflow {
        id: row.try_get("id").map_err(db)?,
        name: row.try_get("name").map_err(db)?,
        steps,
        status: enum_from_str(&status)?,
        created_at: row.try_get("created_at").map_err(db)?,
        started_at: row.try_get("started_at").map_err(db)?,
        completed_at: row.try_get("completed_at").map_err(db)?,
        context: serde_json::from_value(context)?,
        metadata: serde_json::from_value(metadata)?,
        tenant_id: row.try_get("tenant_id").map_err(db)?,
    })
}

const DLQ_COLUMNS: &str = "id, job_id, queue_name, task_name, reason, error_message, tenant_id, \
     created_at, replayed_at, replay_job_id";

const CRON_COLUMNS: &str = "id, name, cron_expr, task_name, payload, config, queue_name, \
     tenant_id, enabled, next_run_at, last_enqueued_at, created_at";

fn cron_from_row(row: &sqlx::postgres::PgRow) -> Result<CronSchedule, StorageError> {
    let payload: Json = row.try_get("payload").map_err(db)?;
    let config: Option<Json> = row.try_get("config").map_err(db)?;
    Ok(CronSchedule {
        id: row.try_get("id").map_err(db)?,
        name: row.try_get("name").map_err(db)?,
        cron_expr: row.try_get("cron_expr").map_err(db)?,
        task_name: row.try_get("task_name").map_err(db)?,
        payload: serde_json::from_value(payload)?,
        config: config.map(serde_json::from_value).transpose()?,
        queue_name: row.try_get("queue_name").map_err(db)?,
        tenant_id: row.try_get("tenant_id").map_err(db)?,
        enabled: row.try_get("enabled").map_err(db)?,
        next_run_at: row.try_get("next_run_at").map_err(db)?,
        last_enqueued_at: row.try_get("last_enqueued_at").map_err(db)?,
        created_at: row.try_get("created_at").map_err(db)?,
    })
}

/// Time-range predicates: `[created_after, created_before)`. A true filter,
/// so it applies to both the COUNT and the page query (unlike the cursor).
fn push_time_range(qb: &mut QueryBuilder<Postgres>, filter: &ListFilter) {
    if let Some(t) = filter.created_after {
        qb.push(" AND created_at >= ").push_bind(t);
    }
    if let Some(t) = filter.created_before {
        qb.push(" AND created_at < ").push_bind(t);
    }
}

fn push_cron_filters(qb: &mut QueryBuilder<Postgres>, filter: &ListFilter) {
    push_time_range(qb, filter);
    if let Some(t) = &filter.tenant_id {
        qb.push(" AND tenant_id = ").push_bind(t.clone());
    }
}

fn dead_letter_from_row(row: &sqlx::postgres::PgRow) -> Result<DeadLetter, StorageError> {
    Ok(DeadLetter {
        id: row.try_get("id").map_err(db)?,
        job_id: row.try_get("job_id").map_err(db)?,
        queue_name: row.try_get("queue_name").map_err(db)?,
        task_name: row.try_get("task_name").map_err(db)?,
        reason: row.try_get("reason").map_err(db)?,
        error_message: row.try_get("error_message").map_err(db)?,
        tenant_id: row.try_get("tenant_id").map_err(db)?,
        created_at: row.try_get("created_at").map_err(db)?,
        replayed_at: row.try_get("replayed_at").map_err(db)?,
        replay_job_id: row.try_get("replay_job_id").map_err(db)?,
    })
}

fn push_dlq_filters(qb: &mut QueryBuilder<Postgres>, filter: &ListFilter) {
    push_time_range(qb, filter);
    if let Some(t) = &filter.tenant_id {
        qb.push(" AND tenant_id = ").push_bind(t.clone());
    }
    if let Some(q) = &filter.queue {
        qb.push(" AND queue_name = ").push_bind(q.clone());
    }
}

fn push_job_filters(qb: &mut QueryBuilder<Postgres>, filter: &ListFilter) {
    push_time_range(qb, filter);
    if let Some(t) = &filter.tenant_id {
        qb.push(" AND tenant_id = ").push_bind(t.clone());
    }
    if let Some(s) = &filter.status {
        qb.push(" AND status = ").push_bind(s.clone());
    }
    if let Some(q) = &filter.queue {
        qb.push(" AND queue_name = ").push_bind(q.clone());
    }
}

fn push_workflow_filters(qb: &mut QueryBuilder<Postgres>, filter: &ListFilter) {
    push_time_range(qb, filter);
    if let Some(t) = &filter.tenant_id {
        qb.push(" AND tenant_id = ").push_bind(t.clone());
    }
    if let Some(s) = &filter.status {
        qb.push(" AND status = ").push_bind(s.clone());
    }
}

fn on_failure_str(v: OnFailure) -> &'static str {
    match v {
        OnFailure::Halt => "halt",
        OnFailure::Skip => "skip",
        OnFailure::Continue => "continue",
    }
}

fn on_success_str(v: OnSuccess) -> &'static str {
    match v {
        OnSuccess::Continue => "continue",
    }
}
