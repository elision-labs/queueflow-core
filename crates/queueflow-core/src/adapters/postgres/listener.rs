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

/// The NOTIFY channel the workflow triggers publish to (payload: the
/// workflow id) on step or workflow state changes.
const WORKFLOW_EVENTS_CHANNEL: &str = "queueflow_workflow_events";

/// Reference-counted per-key notifiers: entries exist only while something
/// is watching the key, so the map is bounded by concurrent watchers.
#[derive(Default)]
struct WatchMap {
    inner: Mutex<HashMap<String, (usize, Arc<Notify>)>>,
}

impl WatchMap {
    fn subscribe(&self, key: &str) -> Arc<Notify> {
        let mut map = self.inner.lock().unwrap();
        let entry = map
            .entry(key.to_string())
            .or_insert_with(|| (0, Arc::new(Notify::new())));
        entry.0 += 1;
        entry.1.clone()
    }

    fn unsubscribe(&self, key: &str) {
        let mut map = self.inner.lock().unwrap();
        if let Some(entry) = map.get_mut(key) {
            entry.0 -= 1;
            if entry.0 == 0 {
                map.remove(key);
            }
        }
    }

    fn notify(&self, key: &str) {
        if let Some((_, n)) = self.inner.lock().unwrap().get(key) {
            n.notify_waiters();
        }
    }

    fn notify_all(&self) {
        for (_, n) in self.inner.lock().unwrap().values() {
            n.notify_waiters();
        }
    }
}

/// Per-queue wakeup channel: epoch + notifier (see the module docs).
#[derive(Default)]
struct QueueSignal {
    epoch: AtomicU64,
    notify: Notify,
}

pub(crate) struct WorkHub {
    pool: PgPool,
    waiters: Mutex<HashMap<String, Arc<QueueSignal>>>,
    /// Per-job watchers (the job SSE stream), released by [`Watch`] guards.
    job_waiters: WatchMap,
    /// Per-workflow watchers (the workflow SSE stream).
    workflow_waiters: WatchMap,
    listener_started: tokio::sync::OnceCell<()>,
}

/// Guard holding one subscription in a [`WatchMap`]. Dropping it, including
/// a cancelled await future, releases the entry.
struct Watch<'a> {
    map: &'a WatchMap,
    key: String,
}

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        self.map.unsubscribe(&self.key);
    }
}

impl WorkHub {
    pub(crate) fn new(pool: PgPool) -> Arc<Self> {
        Arc::new(Self {
            pool,
            waiters: Mutex::new(HashMap::new()),
            job_waiters: WatchMap::default(),
            workflow_waiters: WatchMap::default(),
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
    pub(crate) async fn await_work(
        self: &Arc<Self>,
        queue: &str,
        since_epoch: u64,
        max_wait: Duration,
    ) {
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
        Self::await_key(&self.job_waiters, job_id, max_wait).await;
    }

    /// Park until `workflow_id` (or one of its steps) may have changed, or
    /// `max_wait` passes. May wake spuriously.
    pub(crate) async fn await_workflow_change(
        self: &Arc<Self>,
        workflow_id: &str,
        max_wait: Duration,
    ) {
        self.ensure_listener().await;
        Self::await_key(&self.workflow_waiters, workflow_id, max_wait).await;
    }

    async fn await_key(map: &WatchMap, key: &str, max_wait: Duration) {
        let notify = map.subscribe(key);
        let _watch = Watch {
            map,
            key: key.to_string(),
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

    /// Wake everyone — used after a listener (re)connect, when notifications
    /// may have been missed.
    fn notify_all(&self) {
        for signal in self.waiters.lock().unwrap().values() {
            signal.epoch.fetch_add(1, Ordering::SeqCst);
            signal.notify.notify_waiters();
        }
        self.job_waiters.notify_all();
        self.workflow_waiters.notify_all();
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
            if let Err(e) = listener
                .listen_all([WORK_CHANNEL, JOB_EVENTS_CHANNEL, WORKFLOW_EVENTS_CHANNEL])
                .await
            {
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
                        self.job_waiters.notify(n.payload())
                    }
                    Ok(Some(n)) if n.channel() == WORKFLOW_EVENTS_CHANNEL => {
                        self.workflow_waiters.notify(n.payload())
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
