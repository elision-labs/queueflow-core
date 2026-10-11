//! In-memory adapter: a complete, dependency-free implementation of the
//! [`JobStore`] port, including its claim/lease queue role.
//!
//! This is not a toy — it faithfully models the *observable* behaviour the
//! engine relies on: priority-ordered claims, `scheduled_at` visibility,
//! lease tokens and expiry, and the janitor sweeps. Combined with
//! [`TestClock`](crate::adapters::clock::TestClock), it makes the engine,
//! retry/backoff logic, and the whole workflow scheduler testable
//! deterministically with zero external services.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::domain::*;
use crate::ports::*;
use crate::stats::{QueueStats, StatsSnapshot};

#[derive(Clone)]
struct StepState {
    status: StepStatus,
    job_id: Option<String>,
    #[allow(dead_code)]
    error: Option<String>,
}

// Dead letters are stored as the public domain type; ids come from a local
// sequence, mirroring the BIGSERIAL column on Postgres.

/// A held lease on a `running` job.
#[derive(Clone)]
struct Lease {
    token: String,
    locked_until: DateTime<Utc>,
}

/// Per-queue wakeup channel: a monotonically increasing epoch (bumped on
/// every notification) plus the notifier itself. The epoch lets a claimer
/// detect a wakeup that raced its empty claim (see
/// [`JobStore::await_work`]).
#[derive(Default)]
struct QueueSignal {
    epoch: AtomicU64,
    notify: Notify,
}

/// In-memory [`JobStore`]. Cloneable; clones share the same backing maps.
#[derive(Clone)]
pub struct InMemoryJobStore {
    inner: Arc<Inner>,
    /// Headroom added to a job's timeout when a claim asks to cover it.
    lease_grace_secs: u64,
}

impl InMemoryJobStore {
    /// Override the lease grace (default [`LEASE_GRACE_SECS`]).
    pub fn with_lease_grace_secs(mut self, secs: u64) -> Self {
        self.lease_grace_secs = secs;
        self
    }
}

/// Lock order: when holding more than one of these mutexes, acquire them in
/// field-declaration order (jobs -> leases -> workflows -> steps ->
/// dead_letters -> waiters). Every multi-lock method follows it; deviating
/// can deadlock a janitor sweep racing a workflow advance.
struct Inner {
    clock: Arc<dyn Clock>,
    jobs: Mutex<HashMap<String, Job>>,
    /// job_id -> lease. Entries exist only while a job is leased (`running`).
    leases: Mutex<HashMap<String, Lease>>,
    workflows: Mutex<HashMap<String, Workflow>>,
    // keyed by (workflow_id, step_name)
    steps: Mutex<HashMap<(String, String), StepState>>,
    dead_letters: Mutex<Vec<DeadLetter>>,
    dlq_seq: AtomicI64,
    crons: Mutex<HashMap<String, CronSchedule>>,
    /// Per-queue wakeups for `await_work`.
    waiters: Mutex<HashMap<String, Arc<QueueSignal>>>,
    /// One broadcast for all job status changes: at in-memory (test) scale a
    /// spurious wakeup per watcher is cheaper than a per-job waiter map.
    job_events: Notify,
}

impl InMemoryJobStore {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            lease_grace_secs: LEASE_GRACE_SECS,
            inner: Arc::new(Inner {
                clock,
                jobs: Mutex::new(HashMap::new()),
                leases: Mutex::new(HashMap::new()),
                workflows: Mutex::new(HashMap::new()),
                steps: Mutex::new(HashMap::new()),
                dead_letters: Mutex::new(Vec::new()),
                dlq_seq: AtomicI64::new(0),
                crons: Mutex::new(HashMap::new()),
                waiters: Mutex::new(HashMap::new()),
                job_events: Notify::new(),
            }),
        }
    }

    fn now(&self) -> DateTime<Utc> {
        self.inner.clock.now()
    }

    fn waiter(&self, queue: &str) -> Arc<QueueSignal> {
        self.inner
            .waiters
            .lock()
            .unwrap()
            .entry(queue.to_string())
            .or_default()
            .clone()
    }

    /// Wake every `await_work` parked on `queue` (mirrors the NOTIFY
    /// trigger), advancing the epoch first so an in-flight claimer that
    /// misses the wakeup still sees the epoch move.
    fn notify_work(&self, queue: &str) {
        let signal = self.waiter(queue);
        signal.epoch.fetch_add(1, Ordering::SeqCst);
        signal.notify.notify_waiters();
    }

    /// Wake every `await_job_change` watcher (mirrors the status trigger).
    fn notify_job_change(&self) {
        self.inner.job_events.notify_waiters();
    }
}

/// Slice a sorted, fully-matched result set into a [`Page`] honouring the
/// filter's offset/limit and `include_total`. When a cursor is set, `offset`
/// is ignored (the cursor filter already positioned the set).
fn paginate<T>(matched: Vec<T>, filter: &ListFilter, total: i64) -> Page<T> {
    let start = if filter.after.is_some() {
        0
    } else {
        filter.offset.max(0) as usize
    };
    let lim = if filter.limit <= 0 {
        50
    } else {
        filter.limit as usize
    };
    let has_more = matched.len() > start + lim;
    let items = matched.into_iter().skip(start).take(lim).collect();
    Page {
        items,
        has_more,
        total: filter.include_total.then_some(total),
    }
}

/// Keyset-cursor predicate: keep rows strictly past the cursor in the
/// filter's sort order. Rows are compared by `(created_at, id)`.
fn past_cursor(created_at: DateTime<Utc>, id: &str, filter: &ListFilter) -> bool {
    match &filter.after {
        None => true,
        Some(c) => {
            let key = (created_at, id);
            let cursor = (c.created_at, c.id.as_str());
            if filter.order_desc {
                key < cursor
            } else {
                key > cursor
            }
        }
    }
}

/// Time-range predicate shared by the list methods: `[after, before)`.
fn in_time_range(created_at: DateTime<Utc>, f: &ListFilter) -> bool {
    f.created_after.is_none_or(|t| created_at >= t)
        && f.created_before.is_none_or(|t| created_at < t)
}

fn matches_filter(job: &Job, f: &ListFilter) -> bool {
    if let Some(t) = &f.tenant_id {
        if job.tenant_id.as_deref() != Some(t.as_str()) {
            return false;
        }
    }
    if let Some(s) = &f.status {
        if job.status.as_str() != s {
            return false;
        }
    }
    if let Some(q) = &f.queue {
        if &job.queue_name != q {
            return false;
        }
    }
    in_time_range(job.created_at, f)
}

#[async_trait]
impl JobStore for InMemoryJobStore {
    async fn create_job(&self, job: &Job) -> Result<bool, StorageError> {
        {
            let mut guard = self.inner.jobs.lock().unwrap();
            if let Some(key) = &job.idempotency_key {
                let duplicate = guard.values().any(|j| {
                    j.idempotency_key.as_deref() == Some(key.as_str())
                        && j.tenant_id == job.tenant_id
                });
                if duplicate {
                    return Ok(false);
                }
            }
            guard.insert(job.id.clone(), job.clone());
        }
        if job.status == JobStatus::Pending {
            self.notify_work(&job.queue_name);
        }
        Ok(true)
    }

    async fn find_job_by_idempotency_key(
        &self,
        tenant_id: Option<&str>,
        key: &str,
    ) -> Result<Option<Job>, StorageError> {
        Ok(self
            .inner
            .jobs
            .lock()
            .unwrap()
            .values()
            .find(|j| {
                j.idempotency_key.as_deref() == Some(key) && j.tenant_id.as_deref() == tenant_id
            })
            .cloned())
    }

    async fn batch_create_jobs(&self, jobs: &[Job]) -> Result<Vec<String>, StorageError> {
        let mut ids = Vec::with_capacity(jobs.len());
        {
            let mut guard = self.inner.jobs.lock().unwrap();
            for job in jobs {
                guard.insert(job.id.clone(), job.clone());
                ids.push(job.id.clone());
            }
        }
        // One wakeup per distinct queue, like the trigger's per-tx dedup.
        let mut queues: Vec<&str> = jobs.iter().map(|j| j.queue_name.as_str()).collect();
        queues.sort_unstable();
        queues.dedup();
        for q in queues {
            self.notify_work(q);
        }
        Ok(ids)
    }

    async fn get_job(&self, id: &str) -> Result<Job, StorageError> {
        self.inner
            .jobs
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| StorageError::JobNotFound(id.to_string()))
    }

    async fn list_jobs(&self, filter: &ListFilter) -> Result<Page<Job>, StorageError> {
        let guard = self.inner.jobs.lock().unwrap();
        let mut matched: Vec<Job> = guard
            .values()
            .filter(|j| matches_filter(j, filter))
            .cloned()
            .collect();
        let total = matched.len() as i64;
        matched.retain(|j| past_cursor(j.created_at, &j.id, filter));
        matched.sort_by(|a, b| {
            if filter.order_desc {
                b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id))
            } else {
                a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id))
            }
        });
        Ok(paginate(matched, filter, total))
    }

    // ---- Claim / lease ------------------------------------------------------

    async fn claim_jobs(
        &self,
        queue: &str,
        count: usize,
        lease_secs: u32,
        cover_timeout: bool,
    ) -> Result<Claimed, StorageError> {
        // Snapshot the epoch before scanning: a notification racing this
        // claim either makes its job visible to the scan or advances the
        // epoch, so the caller's subsequent await_work cannot lose it.
        let epoch = self.waiter(queue).epoch.load(Ordering::SeqCst);
        let now = self.now();
        let mut jobs = self.inner.jobs.lock().unwrap();
        let mut leases = self.inner.leases.lock().unwrap();

        let claimable = |j: &Job| {
            j.queue_name == queue && matches!(j.status, JobStatus::Pending | JobStatus::Retrying)
        };

        // Due jobs, ordered like the Postgres claim query: higher priority
        // first, then earlier scheduled_at, then created_at, then id (stable).
        let mut due: Vec<String> = jobs
            .values()
            .filter(|j| claimable(j) && j.scheduled_at <= now)
            .map(|j| j.id.clone())
            .collect();
        due.sort_by(|a, b| {
            let ja = &jobs[a];
            let jb = &jobs[b];
            jb.config
                .priority
                .cmp(&ja.config.priority)
                .then(ja.scheduled_at.cmp(&jb.scheduled_at))
                .then(ja.created_at.cmp(&jb.created_at))
                .then(a.cmp(b))
        });

        let mut out = Vec::new();
        for id in due.into_iter().take(count) {
            let job = jobs.get_mut(&id).unwrap();
            job.status = JobStatus::Running;
            job.started_at.get_or_insert(now);
            job.delivery_count += 1;
            let lease = if cover_timeout {
                lease_secs.max(
                    (job.config
                        .timeout_secs
                        .saturating_add(self.lease_grace_secs))
                    .min(u32::MAX as u64) as u32,
                )
            } else {
                lease_secs
            };
            let token = Uuid::new_v4().to_string();
            leases.insert(
                id.clone(),
                Lease {
                    token: token.clone(),
                    locked_until: now + Duration::seconds(lease as i64),
                },
            );
            out.push(LeasedJob {
                job: job.clone(),
                lease_token: token,
            });
        }

        let next_due = if out.is_empty() {
            jobs.values()
                .filter(|j| claimable(j) && j.scheduled_at > now)
                .map(|j| j.scheduled_at)
                .min()
        } else {
            None
        };
        drop(leases);
        drop(jobs);

        if !out.is_empty() {
            self.notify_job_change(); // pending -> running transitions
        }
        Ok(Claimed {
            jobs: out,
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
        let now = self.now();
        let jobs = self.inner.jobs.lock().unwrap();
        let job = jobs
            .get(job_id)
            .ok_or_else(|| StorageError::JobNotFound(job_id.to_string()))?;
        if job.status != JobStatus::Running {
            // Finished or cancelled mid-run: report it, extend nothing.
            return Ok(Some(job.status));
        }
        let mut leases = self.inner.leases.lock().unwrap();
        match leases.get_mut(job_id) {
            Some(lease) if lease.token == token => {
                lease.locked_until = now + Duration::seconds(lease_secs as i64);
                Ok(Some(JobStatus::Running))
            }
            _ => Ok(None), // reclaimed under a different token
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
        let now = self.now();
        let finished = {
            let mut jobs = self.inner.jobs.lock().unwrap();
            let mut leases = self.inner.leases.lock().unwrap();
            let Some(job) = jobs.get_mut(job_id) else {
                return Ok(None);
            };
            let owned = job.status == JobStatus::Running
                && leases.get(job_id).is_some_and(|l| l.token == token);
            if !owned {
                return Ok(None);
            }
            job.status = status;
            job.completed_at = Some(now);
            if let Some(e) = error {
                job.error_message = Some(e.to_string());
            }
            if let Some(r) = result {
                job.result = Some(r.clone());
            }
            leases.remove(job_id);
            FinishedJob {
                workflow_id: job.workflow_id.clone(),
                workflow_step_id: job.workflow_step_id.clone(),
            }
        };
        self.notify_job_change();
        Ok(Some(finished))
    }

    async fn await_work(
        &self,
        queue: &str,
        since_epoch: u64,
        max_wait: StdDuration,
    ) -> Result<(), StorageError> {
        let signal = self.waiter(queue);
        // Register the waiter *before* re-checking the epoch: a wakeup that
        // fired between the caller's empty claim and this call advanced the
        // epoch (return immediately); one firing after registration lands in
        // the notify. No ordering loses a wakeup.
        let notified = signal.notify.notified();
        if signal.epoch.load(Ordering::SeqCst) != since_epoch {
            return Ok(());
        }
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(max_wait) => {}
        }
        Ok(())
    }

    async fn await_job_change(
        &self,
        _job_id: &str,
        max_wait: StdDuration,
    ) -> Result<(), StorageError> {
        // One broadcast for all jobs: spurious wakeups are within contract,
        // and callers re-read the job and loop.
        tokio::select! {
            _ = self.inner.job_events.notified() => {}
            _ = tokio::time::sleep(max_wait) => {}
        }
        Ok(())
    }

    async fn cancel_job_if_active(&self, id: &str, reason: &str) -> Result<bool, StorageError> {
        {
            let mut guard = self.inner.jobs.lock().unwrap();
            let Some(job) = guard.get_mut(id) else {
                return Ok(false);
            };
            if job.status.is_terminal() {
                return Ok(false);
            }
            job.status = JobStatus::Cancelled;
            job.error_message = Some(reason.to_string());
            job.completed_at = Some(self.now());
            self.inner.leases.lock().unwrap().remove(id);
        }
        self.notify_job_change();
        Ok(true)
    }

    async fn mark_retrying(
        &self,
        id: &str,
        token: &str,
        retry_count: u32,
        next_retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<bool, StorageError> {
        {
            let mut guard = self.inner.jobs.lock().unwrap();
            let mut leases = self.inner.leases.lock().unwrap();
            let Some(job) = guard.get_mut(id) else {
                return Ok(false);
            };
            let owned = job.status == JobStatus::Running
                && leases.get(id).is_some_and(|l| l.token == token);
            if !owned {
                return Ok(false);
            }
            job.status = JobStatus::Retrying;
            job.retry_count = retry_count;
            job.next_retry_at = Some(next_retry_at);
            // The durable delay: the row itself is invisible to claims until then.
            job.scheduled_at = next_retry_at;
            job.error_message = Some(error.to_string());
            leases.remove(id);
        }
        self.notify_job_change();
        Ok(true)
    }

    async fn move_to_dlq(&self, id: &str, reason: &str, error: &str) -> Result<(), StorageError> {
        let (queue_name, task_name, tenant_id) = {
            let jobs = self.inner.jobs.lock().unwrap();
            let job = jobs.get(id);
            (
                job.map(|j| j.queue_name.clone()),
                job.map(|j| j.task_name.clone()),
                job.and_then(|j| j.tenant_id.clone()),
            )
        };
        self.inner.dead_letters.lock().unwrap().push(DeadLetter {
            id: self.inner.dlq_seq.fetch_add(1, Ordering::Relaxed) + 1,
            job_id: id.to_string(),
            queue_name,
            task_name,
            reason: reason.to_string(),
            error_message: Some(error.to_string()),
            tenant_id,
            created_at: self.now(),
            replayed_at: None,
            replay_job_id: None,
        });
        Ok(())
    }

    async fn count_dead_letters(&self) -> Result<i64, StorageError> {
        Ok(self.inner.dead_letters.lock().unwrap().len() as i64)
    }

    async fn queue_stats(
        &self,
        tenant_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<Vec<QueueStats>, StorageError> {
        let owned = |t: &Option<String>| tenant_id.is_none_or(|want| t.as_deref() == Some(want));
        let mut by_queue: std::collections::BTreeMap<String, (QueueStats, Option<DateTime<Utc>>)> =
            Default::default();
        let jobs = self.inner.jobs.lock().unwrap();
        for j in jobs.values().filter(|j| owned(&j.tenant_id)) {
            let entry = by_queue.entry(j.queue_name.clone()).or_insert_with(|| {
                (
                    QueueStats {
                        queue: j.queue_name.clone(),
                        ..Default::default()
                    },
                    None,
                )
            });
            match j.status {
                JobStatus::Pending | JobStatus::Retrying => {
                    if j.scheduled_at <= now {
                        entry.0.pending += 1;
                        entry.1 = Some(entry.1.map_or(j.scheduled_at, |o| o.min(j.scheduled_at)));
                    } else {
                        entry.0.scheduled += 1;
                    }
                }
                JobStatus::Running => entry.0.running += 1,
                _ => {}
            }
        }
        Ok(by_queue
            .into_values()
            .filter(|(q, _)| q.pending + q.scheduled + q.running > 0)
            .map(|(mut q, oldest)| {
                q.oldest_pending_age_secs = oldest.map(|o| (now - o).num_seconds().max(0) as u64);
                q
            })
            .collect())
    }

    async fn count_stats(&self, tenant_id: Option<&str>) -> Result<StatsSnapshot, StorageError> {
        let owned = |t: &Option<String>| tenant_id.is_none_or(|want| t.as_deref() == Some(want));
        let mut snap = StatsSnapshot::default();
        {
            let jobs = self.inner.jobs.lock().unwrap();
            for j in jobs.values().filter(|j| owned(&j.tenant_id)) {
                snap.jobs_created += 1;
                snap.jobs_retried += u64::from(j.retry_count);
                match j.status {
                    JobStatus::Completed => snap.jobs_completed += 1,
                    JobStatus::Failed => snap.jobs_failed += 1,
                    _ => {}
                }
            }
        }
        {
            let wfs = self.inner.workflows.lock().unwrap();
            for w in wfs.values().filter(|w| owned(&w.tenant_id)) {
                snap.workflows_created += 1;
                match w.status {
                    WorkflowStatus::Completed => snap.workflows_completed += 1,
                    WorkflowStatus::Failed | WorkflowStatus::PartiallyFailed => {
                        snap.workflows_failed += 1
                    }
                    _ => {}
                }
            }
        }
        snap.jobs_dead_lettered = self
            .inner
            .dead_letters
            .lock()
            .unwrap()
            .iter()
            .filter(|d| owned(&d.tenant_id))
            .count() as u64;
        Ok(snap)
    }

    async fn list_dead_letters(
        &self,
        filter: &ListFilter,
    ) -> Result<Page<DeadLetter>, StorageError> {
        let guard = self.inner.dead_letters.lock().unwrap();
        let mut matched: Vec<DeadLetter> = guard
            .iter()
            .filter(|d| {
                filter
                    .tenant_id
                    .as_ref()
                    .map(|t| d.tenant_id.as_deref() == Some(t.as_str()))
                    .unwrap_or(true)
                    && filter
                        .queue
                        .as_ref()
                        .map(|q| d.queue_name.as_deref() == Some(q.as_str()))
                        .unwrap_or(true)
                    && in_time_range(d.created_at, filter)
            })
            .cloned()
            .collect();
        let total = matched.len() as i64;
        // The dead-letter cursor id is the BIGSERIAL rendered in decimal.
        if let Some(cursor_id) = filter.after.as_ref().and_then(|c| c.id.parse::<i64>().ok()) {
            let c = filter.after.as_ref().unwrap();
            matched.retain(|d| {
                let key = (d.created_at, d.id);
                let cursor = (c.created_at, cursor_id);
                if filter.order_desc {
                    key < cursor
                } else {
                    key > cursor
                }
            });
        }
        matched.sort_by(|a, b| {
            if filter.order_desc {
                b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id))
            } else {
                a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id))
            }
        });
        Ok(paginate(matched, filter, total))
    }

    async fn get_dead_letter(&self, id: i64) -> Result<DeadLetter, StorageError> {
        self.inner
            .dead_letters
            .lock()
            .unwrap()
            .iter()
            .find(|d| d.id == id)
            .cloned()
            .ok_or(StorageError::DeadLetterNotFound(id))
    }

    // ---- Cron schedules -----------------------------------------------------

    async fn create_cron(&self, cron: &CronSchedule) -> Result<bool, StorageError> {
        let mut crons = self.inner.crons.lock().unwrap();
        let taken = crons
            .values()
            .any(|c| c.name == cron.name && c.tenant_id == cron.tenant_id);
        if taken {
            return Ok(false);
        }
        crons.insert(cron.id.clone(), cron.clone());
        Ok(true)
    }

    async fn get_cron(&self, id: &str) -> Result<CronSchedule, StorageError> {
        self.inner
            .crons
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| StorageError::CronNotFound(id.to_string()))
    }

    async fn list_crons(&self, filter: &ListFilter) -> Result<Page<CronSchedule>, StorageError> {
        let guard = self.inner.crons.lock().unwrap();
        let mut matched: Vec<CronSchedule> = guard
            .values()
            .filter(|c| {
                filter
                    .tenant_id
                    .as_ref()
                    .map(|t| c.tenant_id.as_deref() == Some(t.as_str()))
                    .unwrap_or(true)
                    && in_time_range(c.created_at, filter)
            })
            .cloned()
            .collect();
        let total = matched.len() as i64;
        matched.retain(|c| past_cursor(c.created_at, &c.id, filter));
        matched.sort_by(|a, b| {
            if filter.order_desc {
                b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id))
            } else {
                a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id))
            }
        });
        Ok(paginate(matched, filter, total))
    }

    async fn delete_cron(&self, id: &str) -> Result<bool, StorageError> {
        Ok(self.inner.crons.lock().unwrap().remove(id).is_some())
    }

    async fn set_cron_enabled(
        &self,
        id: &str,
        enabled: bool,
        next_run_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let mut crons = self.inner.crons.lock().unwrap();
        let Some(cron) = crons.get_mut(id) else {
            return Ok(false);
        };
        cron.enabled = enabled;
        cron.next_run_at = next_run_at;
        Ok(true)
    }

    async fn due_crons(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        let guard = self.inner.crons.lock().unwrap();
        let mut due: Vec<CronSchedule> = guard
            .values()
            .filter(|c| c.enabled && c.next_run_at <= now)
            .cloned()
            .collect();
        due.sort_by_key(|c| c.next_run_at);
        due.truncate(limit);
        Ok(due)
    }

    async fn advance_cron(
        &self,
        id: &str,
        fired_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let mut crons = self.inner.crons.lock().unwrap();
        if let Some(cron) = crons.get_mut(id) {
            cron.last_enqueued_at = Some(fired_at);
            cron.next_run_at = next_run_at;
        }
        Ok(())
    }

    async fn replay_dead_letter(&self, id: i64, replacement: &Job) -> Result<bool, StorageError> {
        // One critical section covers the claim and the insert (lock order:
        // jobs before dead_letters), so a dead letter never spawns two
        // replays and the replacement is only visible once the claim won.
        {
            let mut jobs = self.inner.jobs.lock().unwrap();
            let mut dead = self.inner.dead_letters.lock().unwrap();
            let Some(entry) = dead.iter_mut().find(|d| d.id == id) else {
                return Err(StorageError::DeadLetterNotFound(id));
            };
            if entry.replayed_at.is_some() {
                return Ok(false);
            }
            entry.replayed_at = Some(self.now());
            entry.replay_job_id = Some(replacement.id.clone());
            jobs.insert(replacement.id.clone(), replacement.clone());
        }
        if replacement.status == JobStatus::Pending {
            self.notify_work(&replacement.queue_name);
        }
        Ok(true)
    }

    async fn ping(&self) -> Result<(), StorageError> {
        Ok(())
    }

    // ---- Janitor sweeps -----------------------------------------------------

    async fn claim_expired_leases(
        &self,
        limit: usize,
        lease_secs: u32,
    ) -> Result<Vec<LeasedJob>, StorageError> {
        let now = self.now();
        let jobs = self.inner.jobs.lock().unwrap();
        let mut leases = self.inner.leases.lock().unwrap();

        let expired: Vec<String> = leases
            .iter()
            .filter(|(id, l)| {
                l.locked_until < now && jobs.get(*id).map(|j| j.status) == Some(JobStatus::Running)
            })
            .map(|(id, _)| id.clone())
            .take(limit)
            .collect();

        let mut out = Vec::with_capacity(expired.len());
        for id in expired {
            let token = Uuid::new_v4().to_string();
            leases.insert(
                id.clone(),
                Lease {
                    token: token.clone(),
                    locked_until: now + Duration::seconds(lease_secs as i64),
                },
            );
            out.push(LeasedJob {
                job: jobs[&id].clone(),
                lease_token: token,
            });
        }
        Ok(out)
    }

    async fn stalled_step_jobs(&self, limit: usize) -> Result<Vec<Job>, StorageError> {
        let jobs = self.inner.jobs.lock().unwrap();
        let steps = self.inner.steps.lock().unwrap();
        Ok(steps
            .values()
            .filter(|st| !st.status.is_terminal())
            .filter_map(|st| st.job_id.as_ref().and_then(|id| jobs.get(id)))
            .filter(|j| j.status.is_terminal())
            .take(limit)
            .cloned()
            .collect())
    }

    async fn stalled_workflow_ids(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        let jobs = self.inner.jobs.lock().unwrap();
        let workflows = self.inner.workflows.lock().unwrap();
        let steps = self.inner.steps.lock().unwrap();
        Ok(workflows
            .values()
            .filter(|wf| !wf.status.is_terminal())
            .filter(|wf| {
                // No live (non-terminal) linked job anywhere in the workflow.
                !steps.iter().any(|((wf_id, _), st)| {
                    wf_id == &wf.id
                        && st
                            .job_id
                            .as_ref()
                            .and_then(|id| jobs.get(id))
                            .is_some_and(|j| !j.status.is_terminal())
                })
            })
            .map(|wf| wf.id.clone())
            .take(limit)
            .collect())
    }

    async fn purge_terminal(&self, older_than: DateTime<Utc>) -> Result<u64, StorageError> {
        let mut jobs = self.inner.jobs.lock().unwrap();
        let before = jobs.len();
        jobs.retain(|_, j| {
            !(j.status.is_terminal() && j.completed_at.unwrap_or(j.created_at) < older_than)
        });
        let purged = (before - jobs.len()) as u64;

        let mut workflows = self.inner.workflows.lock().unwrap();
        let dead_wfs: Vec<String> = workflows
            .values()
            .filter(|w| {
                w.status.is_terminal() && w.completed_at.unwrap_or(w.created_at) < older_than
            })
            .map(|w| w.id.clone())
            .collect();
        for id in &dead_wfs {
            workflows.remove(id);
        }
        self.inner
            .steps
            .lock()
            .unwrap()
            .retain(|(wf_id, _), _| !dead_wfs.contains(wf_id));
        self.inner
            .dead_letters
            .lock()
            .unwrap()
            .retain(|d| d.created_at >= older_than);
        Ok(purged)
    }

    // ---- Workflows ----------------------------------------------------------

    async fn create_workflow(&self, wf: &Workflow) -> Result<(), StorageError> {
        self.inner
            .workflows
            .lock()
            .unwrap()
            .insert(wf.id.clone(), wf.clone());
        let mut steps = self.inner.steps.lock().unwrap();
        for step in &wf.steps {
            steps.insert(
                (wf.id.clone(), step.name.clone()),
                StepState {
                    status: StepStatus::Pending,
                    job_id: None,
                    error: None,
                },
            );
        }
        Ok(())
    }

    async fn get_workflow(&self, id: &str) -> Result<Workflow, StorageError> {
        self.inner
            .workflows
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| StorageError::WorkflowNotFound(id.to_string()))
    }

    async fn list_workflows(&self, filter: &ListFilter) -> Result<Page<Workflow>, StorageError> {
        let guard = self.inner.workflows.lock().unwrap();
        let mut matched: Vec<Workflow> = guard
            .values()
            .filter(|w| {
                filter
                    .status
                    .as_ref()
                    .map(|s| w.status.as_str() == s)
                    .unwrap_or(true)
                    && filter
                        .tenant_id
                        .as_ref()
                        .map(|t| w.tenant_id.as_deref() == Some(t.as_str()))
                        .unwrap_or(true)
                    && in_time_range(w.created_at, filter)
            })
            .cloned()
            .collect();
        let total = matched.len() as i64;
        matched.retain(|w| past_cursor(w.created_at, &w.id, filter));
        matched.sort_by(|a, b| {
            if filter.order_desc {
                b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id))
            } else {
                a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id))
            }
        });
        Ok(paginate(matched, filter, total))
    }

    async fn workflow_step_statuses(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StepRecord>, StorageError> {
        // Preserve the declared step order for stable, readable results.
        let wf = self.get_workflow(workflow_id).await?;
        let steps = self.inner.steps.lock().unwrap();
        let mut out = Vec::with_capacity(wf.steps.len());
        for step in &wf.steps {
            if let Some(st) = steps.get(&(workflow_id.to_string(), step.name.clone())) {
                out.push(StepRecord {
                    name: step.name.clone(),
                    status: st.status,
                    job_id: st.job_id.clone(),
                });
            }
        }
        Ok(out)
    }

    async fn link_step_job(
        &self,
        workflow_id: &str,
        step_name: &str,
        job_id: &str,
    ) -> Result<bool, StorageError> {
        let mut steps = self.inner.steps.lock().unwrap();
        let st = steps
            .get_mut(&(workflow_id.to_string(), step_name.to_string()))
            .ok_or_else(|| StorageError::StepNotFound {
                workflow: workflow_id.to_string(),
                step: step_name.to_string(),
            })?;
        if st.job_id.is_some() {
            return Ok(false);
        }
        st.job_id = Some(job_id.to_string());
        Ok(true)
    }

    async fn create_step_job(&self, job: &Job) -> Result<bool, StorageError> {
        let wf_id = job.workflow_id.clone().unwrap_or_default();
        let step_name = job.workflow_step_id.clone().unwrap_or_default();
        {
            // One critical section covers the claim and the insert, so the
            // job only ever exists linked to the step it won.
            let mut jobs = self.inner.jobs.lock().unwrap();
            let mut steps = self.inner.steps.lock().unwrap();
            let st = steps.get_mut(&(wf_id.clone(), step_name.clone())).ok_or(
                StorageError::StepNotFound {
                    workflow: wf_id,
                    step: step_name,
                },
            )?;
            if st.job_id.is_some() {
                return Ok(false);
            }
            st.job_id = Some(job.id.clone());
            jobs.insert(job.id.clone(), job.clone());
        }
        if job.status == JobStatus::Pending {
            self.notify_work(&job.queue_name);
        }
        Ok(true)
    }

    async fn set_step_status(
        &self,
        workflow_id: &str,
        step_name: &str,
        status: StepStatus,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        let mut steps = self.inner.steps.lock().unwrap();
        let st = steps
            .get_mut(&(workflow_id.to_string(), step_name.to_string()))
            .ok_or_else(|| StorageError::StepNotFound {
                workflow: workflow_id.to_string(),
                step: step_name.to_string(),
            })?;
        st.status = status;
        if let Some(e) = error {
            st.error = Some(e.to_string());
        }
        Ok(())
    }

    async fn set_workflow_status(
        &self,
        workflow_id: &str,
        status: WorkflowStatus,
    ) -> Result<bool, StorageError> {
        let mut guard = self.inner.workflows.lock().unwrap();
        let wf = guard
            .get_mut(workflow_id)
            .ok_or_else(|| StorageError::WorkflowNotFound(workflow_id.to_string()))?;
        // A terminal workflow is never overwritten.
        if wf.status.is_terminal() || wf.status == status {
            return Ok(false);
        }
        wf.status = status;
        let now = self.now();
        if status == WorkflowStatus::Running && wf.started_at.is_none() {
            wf.started_at = Some(now);
        }
        if status.is_terminal() {
            wf.completed_at = Some(now);
        }
        Ok(true)
    }

    async fn merge_workflow_context(
        &self,
        workflow_id: &str,
        key: &str,
        value: &Json,
    ) -> Result<(), StorageError> {
        let mut guard = self.inner.workflows.lock().unwrap();
        let wf = guard
            .get_mut(workflow_id)
            .ok_or_else(|| StorageError::WorkflowNotFound(workflow_id.to_string()))?;
        wf.context.insert(key.to_string(), value.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::clock::TestClock;

    fn job(id: &str, queue: &str, priority: i32) -> Job {
        let epoch = TestClock::epoch().now();
        Job {
            id: id.to_string(),
            queue_name: queue.to_string(),
            task_name: "t".into(),
            payload: Map::new(),
            config: JobConfig {
                priority,
                ..JobConfig::default()
            },
            status: JobStatus::Pending,
            created_at: epoch,
            scheduled_at: epoch,
            started_at: None,
            completed_at: None,
            delivery_count: 0,
            error_message: None,
            retry_count: 0,
            next_retry_at: None,
            workflow_id: None,
            workflow_step_id: None,
            result: None,
            metadata: Map::new(),
            tenant_id: None,
            idempotency_key: None,
        }
    }

    fn store() -> (InMemoryJobStore, Arc<TestClock>) {
        let clock = Arc::new(TestClock::epoch());
        (InMemoryJobStore::new(clock.clone()), clock)
    }

    #[tokio::test]
    async fn claimed_job_is_hidden_until_finished_or_expired() {
        let (s, clock) = store();
        s.create_job(&job("a", "default", 0)).await.unwrap();

        let first = s.claim_jobs("default", 1, 30, false).await.unwrap();
        assert_eq!(first.jobs.len(), 1);
        assert_eq!(first.jobs[0].job.delivery_count, 1);
        assert_eq!(first.jobs[0].job.status, JobStatus::Running);

        // Hidden while leased — and unlike a visibility timeout, a lease does
        // NOT make the job claimable again on expiry; recovery is the
        // janitor's expired-lease sweep.
        assert!(s
            .claim_jobs("default", 1, 30, false)
            .await
            .unwrap()
            .jobs
            .is_empty());
        clock.advance_secs(31);
        assert!(s
            .claim_jobs("default", 1, 30, false)
            .await
            .unwrap()
            .jobs
            .is_empty());

        let expired = s.claim_expired_leases(10, 30).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_ne!(expired[0].lease_token, first.jobs[0].lease_token);
    }

    #[tokio::test]
    async fn future_scheduled_job_is_invisible_and_reports_next_due() {
        let (s, clock) = store();
        let mut j = job("a", "default", 0);
        j.scheduled_at = clock.now() + Duration::seconds(60);
        s.create_job(&j).await.unwrap();

        let c = s.claim_jobs("default", 1, 30, false).await.unwrap();
        assert!(c.jobs.is_empty());
        assert_eq!(c.next_due, Some(j.scheduled_at));

        clock.advance_secs(61);
        assert_eq!(
            s.claim_jobs("default", 1, 30, false)
                .await
                .unwrap()
                .jobs
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn higher_priority_is_claimed_first() {
        let (s, _) = store();
        s.create_job(&job("low", "default", 0)).await.unwrap();
        s.create_job(&job("high", "default", 10)).await.unwrap();
        let c = s.claim_jobs("default", 1, 30, false).await.unwrap();
        assert_eq!(c.jobs[0].job.id, "high");
    }

    #[tokio::test]
    async fn stale_token_cannot_finish_or_extend() {
        let (s, clock) = store();
        s.create_job(&job("a", "default", 0)).await.unwrap();
        let stale = s.claim_jobs("default", 1, 30, false).await.unwrap().jobs[0]
            .lease_token
            .clone();

        // Lease expires; the janitor reclaims under a fresh token.
        clock.advance_secs(31);
        let fresh = s.claim_expired_leases(10, 30).await.unwrap()[0]
            .lease_token
            .clone();

        assert_eq!(s.extend_lease("a", &stale, 30).await.unwrap(), None);
        assert!(s
            .finish_if_leased("a", &stale, JobStatus::Completed, None, None)
            .await
            .unwrap()
            .is_none());
        assert!(s
            .finish_if_leased("a", &fresh, JobStatus::Completed, None, None)
            .await
            .unwrap()
            .is_some());
        // A replay of the fresh token after finishing is also rejected.
        assert!(s
            .finish_if_leased("a", &fresh, JobStatus::Completed, None, None)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn heartbeat_observes_mid_run_cancellation() {
        let (s, _) = store();
        s.create_job(&job("a", "default", 0)).await.unwrap();
        let token = s.claim_jobs("default", 1, 30, false).await.unwrap().jobs[0]
            .lease_token
            .clone();
        assert_eq!(
            s.extend_lease("a", &token, 60).await.unwrap(),
            Some(JobStatus::Running)
        );

        assert!(s.cancel_job_if_active("a", "user").await.unwrap());
        assert_eq!(
            s.extend_lease("a", &token, 60).await.unwrap(),
            Some(JobStatus::Cancelled)
        );
        // And the cancelled job can no longer be completed under the token.
        assert!(s
            .finish_if_leased("a", &token, JobStatus::Completed, None, None)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn mark_retrying_reschedules_and_releases_the_lease() {
        let (s, clock) = store();
        s.create_job(&job("a", "default", 0)).await.unwrap();
        let token = s.claim_jobs("default", 1, 30, false).await.unwrap().jobs[0]
            .lease_token
            .clone();

        let next = clock.now() + Duration::seconds(60);
        // A bogus token must not reschedule; the real one must.
        assert!(!s
            .mark_retrying("a", "bogus", 1, next, "boom")
            .await
            .unwrap());
        assert!(s.mark_retrying("a", &token, 1, next, "boom").await.unwrap());

        let c = s.claim_jobs("default", 1, 30, false).await.unwrap();
        assert!(c.jobs.is_empty());
        assert_eq!(c.next_due, Some(next));

        clock.advance_secs(61);
        let again = s.claim_jobs("default", 1, 30, false).await.unwrap();
        assert_eq!(again.jobs[0].job.retry_count, 1);
        assert_eq!(again.jobs[0].job.delivery_count, 2);
    }

    #[tokio::test]
    async fn await_work_wakes_on_new_job() {
        let (s, _) = store();
        let epoch = s.claim_jobs("default", 1, 30, false).await.unwrap().epoch;
        let s2 = s.clone();
        tokio::spawn(async move {
            tokio::time::sleep(StdDuration::from_millis(50)).await;
            s2.create_job(&job("late", "default", 0)).await.unwrap();
        });
        // Wakes well before the 5s cap once the job lands.
        let start = std::time::Instant::now();
        s.await_work("default", epoch, StdDuration::from_secs(5))
            .await
            .unwrap();
        assert!(start.elapsed() < StdDuration::from_secs(4));
        assert_eq!(
            s.claim_jobs("default", 1, 30, false)
                .await
                .unwrap()
                .jobs
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn await_work_returns_immediately_when_the_epoch_moved() {
        // A job enqueued between an empty claim and the park must not cost
        // the full wait: the epoch snapshot detects it.
        let (s, _) = store();
        let epoch = s.claim_jobs("default", 1, 30, false).await.unwrap().epoch;
        s.create_job(&job("racy", "default", 0)).await.unwrap();
        let start = std::time::Instant::now();
        s.await_work("default", epoch, StdDuration::from_secs(5))
            .await
            .unwrap();
        assert!(start.elapsed() < StdDuration::from_secs(1));
    }

    #[tokio::test]
    async fn cover_timeout_stretches_the_lease_to_the_job_timeout() {
        let (s, clock) = store();
        let mut j = job("a", "default", 0);
        j.config.timeout_secs = 120;
        s.create_job(&j).await.unwrap();
        assert_eq!(
            s.claim_jobs("default", 1, 30, true)
                .await
                .unwrap()
                .jobs
                .len(),
            1
        );

        // Well past the raw 30s lease but inside timeout + grace: not reaped.
        clock.advance_secs(120);
        assert!(s.claim_expired_leases(10, 30).await.unwrap().is_empty());
        // Past timeout + grace: reaped.
        clock.advance_secs(31);
        assert_eq!(s.claim_expired_leases(10, 30).await.unwrap().len(), 1);
    }
}
