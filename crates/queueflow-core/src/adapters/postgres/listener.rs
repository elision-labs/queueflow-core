//! LISTEN/NOTIFY fan-out for [`JobStore::await_work`] and
//! [`JobStore::await_job_change`].
//!
//! One shared listening connection per process, subscribed to two channels:
//! `queueflow_work` (the `jobs_notify` trigger sends the queue name, waking
//! parked workers) and `queueflow_job_events` (the `jobs_status_notify`
//! trigger sends the job id, waking per-job watchers such as the SSE
//! stream). Idle waiters therefore hold zero database connections, however
//! many are parked.
//!
//! Each queue also carries a monotonically increasing *epoch*, bumped on
//! every notification. A claimer snapshots the epoch before its claim query
//! and hands it back to `await_work`; a notification that landed in between
//! advanced the epoch, so the wait returns immediately instead of sleeping
//! out the poll cap — no wakeup is ever lost to that race.
//!
//! Degradation: if LISTEN is unavailable (e.g. a transaction-pooling
//! pgbouncer) the hub keeps retrying in the background and waiters simply
//! sleep out their bounded `max_wait` — work is still picked up, just on the
//! poll cadence instead of instantly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::sync::Notify;

/// The NOTIFY channel the `jobs_notify` trigger publishes to (payload: the
/// queue name).
const WORK_CHANNEL: &str = "queueflow_work";

/// The NOTIFY channel the `jobs_status_notify` trigger publishes to
/// (payload: the job id).
const JOB_EVENTS_CHANNEL: &str = "queueflow_job_events";

/// Per-queue wakeup channel: epoch + notifier (see the module docs).
#[derive(Default)]
struct QueueSignal {
    epoch: AtomicU64,
    notify: Notify,
}

pub(crate) struct WorkHub {
    pool: PgPool,
    waiters: Mutex<HashMap<String, Arc<QueueSignal>>>,
    /// job_id -> (watcher count, notifier). Entries are reference-counted by
    /// [`JobWatch`] guards and removed when the last watcher unsubscribes,
    /// so the map is bounded by concurrently-watched jobs, not job history.
    job_waiters: Mutex<HashMap<String, (usize, Arc<Notify>)>>,
    listener_started: tokio::sync::OnceCell<()>,
}

/// Guard holding one subscription in [`WorkHub::job_waiters`]. Dropping it —
/// including a cancelled `await_job_change` future — releases the entry.
struct JobWatch {
    hub: Arc<WorkHub>,
    job_id: String,
}

impl Drop for JobWatch {
    fn drop(&mut self) {
        let mut map = self.hub.job_waiters.lock().unwrap();
        if let Some(entry) = map.get_mut(&self.job_id) {
            entry.0 -= 1;
            if entry.0 == 0 {
                map.remove(&self.job_id);
            }
        }
    }
}

impl WorkHub {
    pub(crate) fn new(pool: PgPool) -> Arc<Self> {
        Arc::new(Self {
            pool,
            waiters: Mutex::new(HashMap::new()),
            job_waiters: Mutex::new(HashMap::new()),
            listener_started: tokio::sync::OnceCell::new(),
        })
    }

    fn signal(&self, queue: &str) -> Arc<QueueSignal> {
        self.waiters
            .lock()
            .unwrap()
            .entry(queue.to_string())
            .or_default()
            .clone()
    }

    /// The queue's current work epoch. Snapshot it *before* a claim query.
    pub(crate) fn epoch(&self, queue: &str) -> u64 {
        self.signal(queue).epoch.load(Ordering::SeqCst)
    }

    /// Park until `queue` may have work or `max_wait` passes. Returns
    /// immediately when the epoch advanced past `since_epoch` (a wakeup
    /// raced the caller's empty claim). May wake spuriously; callers
    /// re-claim in a loop.
    pub(crate) async fn await_work(self: &Arc<Self>, queue: &str, since_epoch: u64, max_wait: Duration) {
        self.ensure_listener().await;
        let signal = self.signal(queue);
        // Register before the epoch re-check so no notification can slip
        // between the check and the park.
        let notified = signal.notify.notified();
        if signal.epoch.load(Ordering::SeqCst) != since_epoch {
            return;
        }
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(max_wait) => {}
        }
    }

    /// Park until `job_id` may have changed status or `max_wait` passes.
    /// May wake spuriously; callers re-read the job in a loop.
    pub(crate) async fn await_job_change(self: &Arc<Self>, job_id: &str, max_wait: Duration) {
        self.ensure_listener().await;
        let notify = {
            let mut map = self.job_waiters.lock().unwrap();
            let entry = map
                .entry(job_id.to_string())
                .or_insert_with(|| (0, Arc::new(Notify::new())));
            entry.0 += 1;
            entry.1.clone()
        };
        let _watch = JobWatch {
            hub: self.clone(),
            job_id: job_id.to_string(),
        };
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(max_wait) => {}
        }
    }

    fn notify_queue(&self, queue: &str) {
        let signal = self.signal(queue);
        signal.epoch.fetch_add(1, Ordering::SeqCst);
        signal.notify.notify_waiters();
    }

    fn notify_job(&self, job_id: &str) {
        if let Some((_, n)) = self.job_waiters.lock().unwrap().get(job_id) {
            n.notify_waiters();
        }
    }

    /// Wake everyone — used after a listener (re)connect, when notifications
    /// may have been missed.
    fn notify_all(&self) {
        for signal in self.waiters.lock().unwrap().values() {
            signal.epoch.fetch_add(1, Ordering::SeqCst);
            signal.notify.notify_waiters();
        }
        for (_, n) in self.job_waiters.lock().unwrap().values() {
            n.notify_waiters();
        }
    }

    async fn ensure_listener(self: &Arc<Self>) {
        let hub = self.clone();
        self.listener_started
            .get_or_init(|| async move {
                tokio::spawn(async move { hub.listen_loop().await });
            })
            .await;
    }

    async fn listen_loop(self: Arc<Self>) {
        loop {
            let mut listener = match PgListener::connect_with(&self.pool).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!(error = %e, "LISTEN connection failed; waiters fall back to bounded polling");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            if let Err(e) = listener.listen_all([WORK_CHANNEL, JOB_EVENTS_CHANNEL]).await {
                tracing::warn!(error = %e, "LISTEN failed; waiters fall back to bounded polling");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            // Anything enqueued while we were not listening was missed.
            self.notify_all();

            loop {
                // try_recv reconnects under the hood and returns Ok(None) when
                // the connection had dropped — i.e. notifications may have
                // been lost — so that case wakes every waiter.
                match listener.try_recv().await {
                    Ok(Some(n)) if n.channel() == JOB_EVENTS_CHANNEL => {
                        self.notify_job(n.payload())
                    }
                    Ok(Some(n)) => self.notify_queue(n.payload()),
                    Ok(None) => self.notify_all(),
                    Err(e) => {
                        tracing::warn!(error = %e, "LISTEN receive failed; reconnecting");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        break;
                    }
                }
            }
        }
    }
}
