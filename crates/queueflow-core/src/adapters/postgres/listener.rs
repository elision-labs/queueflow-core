//! LISTEN/NOTIFY fan-out for [`JobStore::await_work`].
//!
//! One shared `LISTEN queueflow_work` connection per process; the
//! `jobs_notify` trigger sends the queue name as the payload, and the hub
//! wakes every in-process waiter parked on that queue. Idle workers therefore
//! hold zero database connections, however many are parked.
//!
//! Degradation: if LISTEN is unavailable (e.g. a transaction-pooling
//! pgbouncer) the hub keeps retrying in the background and waiters simply
//! sleep out their bounded `max_wait` — work is still picked up, just on the
//! poll cadence instead of instantly.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::sync::Notify;

/// The NOTIFY channel the `jobs_notify` trigger publishes to.
const CHANNEL: &str = "queueflow_work";

pub(crate) struct WorkHub {
    pool: PgPool,
    waiters: Mutex<HashMap<String, Arc<Notify>>>,
    listener_started: tokio::sync::OnceCell<()>,
}

impl WorkHub {
    pub(crate) fn new(pool: PgPool) -> Arc<Self> {
        Arc::new(Self {
            pool,
            waiters: Mutex::new(HashMap::new()),
            listener_started: tokio::sync::OnceCell::new(),
        })
    }

    /// Park until `queue` may have work or `max_wait` passes. May wake
    /// spuriously; callers re-claim in a loop.
    pub(crate) async fn await_work(self: &Arc<Self>, queue: &str, max_wait: Duration) {
        self.ensure_listener().await;
        let notify = self.waiter(queue);
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(max_wait) => {}
        }
    }

    fn waiter(&self, queue: &str) -> Arc<Notify> {
        self.waiters
            .lock()
            .unwrap()
            .entry(queue.to_string())
            .or_default()
            .clone()
    }

    fn notify_queue(&self, queue: &str) {
        if let Some(n) = self.waiters.lock().unwrap().get(queue) {
            n.notify_waiters();
        }
    }

    /// Wake everyone — used after a listener (re)connect, when notifications
    /// may have been missed.
    fn notify_all(&self) {
        for n in self.waiters.lock().unwrap().values() {
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
                    tracing::warn!(error = %e, "LISTEN connection failed; workers fall back to bounded polling");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            if let Err(e) = listener.listen(CHANNEL).await {
                tracing::warn!(error = %e, "LISTEN {CHANNEL} failed; workers fall back to bounded polling");
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
