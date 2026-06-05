//! In-memory adapters: a complete, dependency-free implementation of the
//! [`JobStore`] and [`MessageQueue`] ports.
//!
//! These are not toys — they faithfully model the *observable* behaviour the
//! engine relies on, in particular PGMQ's visibility-timeout and delayed
//! delivery. Combined with [`TestClock`](crate::adapters::clock::TestClock),
//! they make the engine, retry/backoff logic, and the whole workflow scheduler
//! testable deterministically with zero external services.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::*;
use crate::ports::*;

// ---------------------------------------------------------------------------
// Job store
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct StepState {
    status: StepStatus,
    job_id: Option<String>,
    #[allow(dead_code)]
    error: Option<String>,
}

#[derive(Clone)]
struct DeadLetter {
    #[allow(dead_code)]
    job_id: String,
    #[allow(dead_code)]
    reason: String,
    #[allow(dead_code)]
    error: String,
}

/// In-memory [`JobStore`]. Cloneable; clones share the same backing maps.
#[derive(Clone, Default)]
pub struct InMemoryJobStore {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    jobs: Mutex<HashMap<String, Job>>,
    workflows: Mutex<HashMap<String, Workflow>>,
    // keyed by (workflow_id, step_name)
    steps: Mutex<HashMap<(String, String), StepState>>,
    dead_letters: Mutex<Vec<DeadLetter>>,
}

impl InMemoryJobStore {
    pub fn new() -> Self {
        Self::default()
    }
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
    true
}

#[async_trait]
impl JobStore for InMemoryJobStore {
    async fn create_job(&self, job: &Job) -> Result<(), StorageError> {
        self.inner
            .jobs
            .lock()
            .unwrap()
            .insert(job.id.clone(), job.clone());
        Ok(())
    }

    async fn batch_create_jobs(&self, jobs: &[Job]) -> Result<Vec<String>, StorageError> {
        let mut guard = self.inner.jobs.lock().unwrap();
        let mut ids = Vec::with_capacity(jobs.len());
        for job in jobs {
            guard.insert(job.id.clone(), job.clone());
            ids.push(job.id.clone());
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

    async fn list_jobs(&self, filter: &ListFilter) -> Result<(Vec<Job>, i64), StorageError> {
        let guard = self.inner.jobs.lock().unwrap();
        let mut matched: Vec<Job> = guard
            .values()
            .filter(|j| matches_filter(j, filter))
            .cloned()
            .collect();
        let total = matched.len() as i64;
        matched.sort_by(|a, b| {
            if filter.order_desc {
                b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id))
            } else {
                a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id))
            }
        });
        let start = filter.offset.max(0) as usize;
        let lim = if filter.limit <= 0 {
            50
        } else {
            filter.limit as usize
        };
        let page = matched.into_iter().skip(start).take(lim).collect();
        Ok((page, total))
    }

    async fn update_status(
        &self,
        id: &str,
        status: JobStatus,
        error: Option<&str>,
        result: Option<&Json>,
    ) -> Result<(), StorageError> {
        let mut guard = self.inner.jobs.lock().unwrap();
        let job = guard
            .get_mut(id)
            .ok_or_else(|| StorageError::JobNotFound(id.to_string()))?;
        job.status = status;
        let now = Utc::now();
        if status == JobStatus::Running && job.started_at.is_none() {
            job.started_at = Some(now);
        }
        if status.is_terminal() {
            job.completed_at = Some(now);
        }
        if let Some(e) = error {
            job.error_message = Some(e.to_string());
        }
        if let Some(r) = result {
            job.result = Some(r.clone());
        }
        Ok(())
    }

    async fn mark_retrying(
        &self,
        id: &str,
        retry_count: u32,
        next_retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<(), StorageError> {
        let mut guard = self.inner.jobs.lock().unwrap();
        let job = guard
            .get_mut(id)
            .ok_or_else(|| StorageError::JobNotFound(id.to_string()))?;
        job.status = JobStatus::Retrying;
        job.retry_count = retry_count;
        job.next_retry_at = Some(next_retry_at);
        job.error_message = Some(error.to_string());
        Ok(())
    }

    async fn move_to_dlq(&self, id: &str, reason: &str, error: &str) -> Result<(), StorageError> {
        self.inner.dead_letters.lock().unwrap().push(DeadLetter {
            job_id: id.to_string(),
            reason: reason.to_string(),
            error: error.to_string(),
        });
        Ok(())
    }

    async fn count_dead_letters(&self) -> Result<i64, StorageError> {
        Ok(self.inner.dead_letters.lock().unwrap().len() as i64)
    }

    async fn ping(&self) -> Result<(), StorageError> {
        Ok(())
    }

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

    async fn list_workflows(
        &self,
        filter: &ListFilter,
    ) -> Result<(Vec<Workflow>, i64), StorageError> {
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
            })
            .cloned()
            .collect();
        let total = matched.len() as i64;
        matched.sort_by(|a, b| {
            if filter.order_desc {
                b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id))
            } else {
                a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id))
            }
        });
        let start = filter.offset.max(0) as usize;
        let lim = if filter.limit <= 0 {
            50
        } else {
            filter.limit as usize
        };
        let page = matched.into_iter().skip(start).take(lim).collect();
        Ok((page, total))
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
    ) -> Result<(), StorageError> {
        let mut steps = self.inner.steps.lock().unwrap();
        let st = steps
            .get_mut(&(workflow_id.to_string(), step_name.to_string()))
            .ok_or_else(|| StorageError::StepNotFound {
                workflow: workflow_id.to_string(),
                step: step_name.to_string(),
            })?;
        st.job_id = Some(job_id.to_string());
        Ok(())
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
    ) -> Result<(), StorageError> {
        let mut guard = self.inner.workflows.lock().unwrap();
        let wf = guard
            .get_mut(workflow_id)
            .ok_or_else(|| StorageError::WorkflowNotFound(workflow_id.to_string()))?;
        wf.status = status;
        let now = Utc::now();
        if status == WorkflowStatus::Running && wf.started_at.is_none() {
            wf.started_at = Some(now);
        }
        if status.is_terminal() {
            wf.completed_at = Some(now);
        }
        Ok(())
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

// ---------------------------------------------------------------------------
// Message queue
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MemMsg {
    message: QueueMessage,
    priority: i32,
    enqueued_at: DateTime<Utc>,
    visible_at: DateTime<Utc>,
    read_count: u32,
}

/// In-memory [`MessageQueue`] that simulates PGMQ visibility timeouts and
/// delayed delivery against an injected [`Clock`].
#[derive(Clone)]
pub struct InMemoryMessageQueue {
    clock: Arc<dyn Clock>,
    queues: Arc<Mutex<HashMap<String, HashMap<i64, MemMsg>>>>,
    next_id: Arc<AtomicI64>,
}

impl InMemoryMessageQueue {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            queues: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicI64::new(1)),
        }
    }

    fn push(&self, queue: &str, msg: &QueueMessage, priority: i32, delay_secs: u64) -> i64 {
        let now = self.clock.now();
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut guard = self.queues.lock().unwrap();
        let q = guard.entry(queue.to_string()).or_default();
        q.insert(
            id,
            MemMsg {
                message: msg.clone(),
                priority,
                enqueued_at: now,
                visible_at: now + Duration::seconds(delay_secs as i64),
                read_count: 0,
            },
        );
        id
    }
}

#[async_trait]
impl MessageQueue for InMemoryMessageQueue {
    async fn send(
        &self,
        queue: &str,
        msg: &QueueMessage,
        priority: i32,
    ) -> Result<i64, QueueError> {
        Ok(self.push(queue, msg, priority, 0))
    }

    async fn send_delayed(
        &self,
        queue: &str,
        msg: &QueueMessage,
        priority: i32,
        delay_secs: u64,
    ) -> Result<i64, QueueError> {
        Ok(self.push(queue, msg, priority, delay_secs))
    }

    async fn read(
        &self,
        queue: &str,
        vt_secs: u32,
        count: usize,
    ) -> Result<Vec<ReadMessage>, QueueError> {
        let now = self.clock.now();
        let mut guard = self.queues.lock().unwrap();
        let Some(q) = guard.get_mut(queue) else {
            return Ok(vec![]);
        };
        // Visible messages, ordered like PGMQ-with-priority: higher priority
        // first, then oldest first, then stable by msg_id.
        let mut visible: Vec<i64> = q
            .iter()
            .filter(|(_, m)| m.visible_at <= now)
            .map(|(id, _)| *id)
            .collect();
        visible.sort_by(|a, b| {
            let ma = &q[a];
            let mb = &q[b];
            mb.priority
                .cmp(&ma.priority)
                .then(ma.enqueued_at.cmp(&mb.enqueued_at))
                .then(a.cmp(b))
        });

        let mut out = Vec::new();
        for id in visible.into_iter().take(count) {
            let m = q.get_mut(&id).unwrap();
            m.visible_at = now + Duration::seconds(vt_secs as i64);
            m.read_count += 1;
            out.push(ReadMessage {
                msg_id: id,
                message: m.message.clone(),
                read_count: m.read_count,
                enqueued_at: m.enqueued_at,
            });
        }
        Ok(out)
    }

    async fn delete(&self, queue: &str, msg_id: i64) -> Result<(), QueueError> {
        if let Some(q) = self.queues.lock().unwrap().get_mut(queue) {
            q.remove(&msg_id);
        }
        Ok(())
    }

    async fn purge(&self, queue: &str) -> Result<u64, QueueError> {
        if let Some(q) = self.queues.lock().unwrap().get_mut(queue) {
            let n = q.len() as u64;
            q.clear();
            Ok(n)
        } else {
            Ok(0)
        }
    }

    async fn queue_depth(&self, queue: &str) -> Result<u64, QueueError> {
        let now = self.clock.now();
        Ok(self
            .queues
            .lock()
            .unwrap()
            .get(queue)
            .map(|q| q.values().filter(|m| m.visible_at <= now).count() as u64)
            .unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::clock::TestClock;

    fn msg(id: &str) -> QueueMessage {
        QueueMessage {
            job_id: id.to_string(),
            task_name: "t".into(),
            payload: Map::new(),
            config: JobConfig::default(),
        }
    }

    #[tokio::test]
    async fn read_hides_message_until_visibility_timeout_elapses() {
        let clock = Arc::new(TestClock::epoch());
        let q = InMemoryMessageQueue::new(clock.clone());
        q.send("default", &msg("a"), 0).await.unwrap();

        let first = q.read("default", 30, 1).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].read_count, 1);

        // Hidden while leased.
        assert!(q.read("default", 30, 1).await.unwrap().is_empty());

        // Reappears after the visibility timeout, with an incremented read count.
        clock.advance_secs(31);
        let again = q.read("default", 30, 1).await.unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].read_count, 2);
    }

    #[tokio::test]
    async fn delayed_send_is_invisible_until_delay_passes() {
        let clock = Arc::new(TestClock::epoch());
        let q = InMemoryMessageQueue::new(clock.clone());
        q.send_delayed("default", &msg("a"), 0, 60).await.unwrap();

        assert!(q.read("default", 30, 1).await.unwrap().is_empty());
        clock.advance_secs(61);
        assert_eq!(q.read("default", 30, 1).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn higher_priority_is_read_first() {
        let clock = Arc::new(TestClock::epoch());
        let q = InMemoryMessageQueue::new(clock.clone());
        q.send("default", &msg("low"), 0).await.unwrap();
        q.send("default", &msg("high"), 10).await.unwrap();
        let read = q.read("default", 30, 1).await.unwrap();
        assert_eq!(read[0].message.job_id, "high");
    }

    #[tokio::test]
    async fn delete_acknowledges_and_purge_clears() {
        let clock = Arc::new(TestClock::epoch());
        let q = InMemoryMessageQueue::new(clock.clone());
        let id = q.send("default", &msg("a"), 0).await.unwrap();
        q.send("default", &msg("b"), 0).await.unwrap();
        q.delete("default", id).await.unwrap();
        assert_eq!(q.queue_depth("default").await.unwrap(), 1);
        assert_eq!(q.purge("default").await.unwrap(), 1);
        assert_eq!(q.queue_depth("default").await.unwrap(), 0);
    }
}
