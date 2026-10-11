//! Cheap, always-on engine counters.
//!
//! The core crate stays free of any metrics backend: it just bumps atomics.
//! The server binary reads [`EngineStats::snapshot`] and exposes the numbers as
//! Prometheus metrics. This keeps the hot path dependency-free and the metrics
//! representation a deployment concern.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free counters incremented by the engine.
#[derive(Debug, Default)]
pub struct EngineStats {
    pub jobs_created: AtomicU64,
    pub jobs_completed: AtomicU64,
    pub jobs_failed: AtomicU64,
    pub jobs_retried: AtomicU64,
    pub jobs_dead_lettered: AtomicU64,
    pub workflows_created: AtomicU64,
    pub workflows_completed: AtomicU64,
    pub workflows_failed: AtomicU64,
    /// Wall time handlers spend running (in-process workers only).
    pub handler_duration: Histogram,
    /// Time from a job becoming due (`scheduled_at`) to a worker claiming it:
    /// the backlog latency operators actually feel.
    pub queue_wait: Histogram,
}

impl EngineStats {
    #[inline]
    pub(crate) fn incr(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    /// A consistent-enough point-in-time copy for scraping.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            jobs_created: self.jobs_created.load(Ordering::Relaxed),
            jobs_completed: self.jobs_completed.load(Ordering::Relaxed),
            jobs_failed: self.jobs_failed.load(Ordering::Relaxed),
            jobs_retried: self.jobs_retried.load(Ordering::Relaxed),
            jobs_dead_lettered: self.jobs_dead_lettered.load(Ordering::Relaxed),
            workflows_created: self.workflows_created.load(Ordering::Relaxed),
            workflows_completed: self.workflows_completed.load(Ordering::Relaxed),
            workflows_failed: self.workflows_failed.load(Ordering::Relaxed),
        }
    }

    /// Point-in-time copy of the latency histograms (for the metrics port).
    pub fn latency(&self) -> LatencySnapshot {
        LatencySnapshot {
            handler_duration: self.handler_duration.snapshot(),
            queue_wait: self.queue_wait.snapshot(),
        }
    }
}

/// Upper bounds, in seconds, of the fixed latency buckets (Prometheus `le`).
/// Spans sub-10ms handlers to multi-minute batch jobs.
pub const LATENCY_BUCKETS_SECS: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
];

/// Lock-free fixed-bucket histogram. Bucket counts are cumulative, as
/// Prometheus expects; `sum` is kept in microseconds so it stays an integer.
#[derive(Debug, Default)]
pub struct Histogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_SECS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    pub fn observe(&self, secs: f64) {
        let secs = if secs.is_finite() { secs.max(0.0) } else { 0.0 };
        for (i, upper) in LATENCY_BUCKETS_SECS.iter().enumerate() {
            if secs <= *upper {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add((secs * 1_000_000.0) as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets: LATENCY_BUCKETS_SECS
                .iter()
                .zip(self.buckets.iter())
                .map(|(le, c)| (*le, c.load(Ordering::Relaxed)))
                .collect(),
            count: self.count.load(Ordering::Relaxed),
            sum_secs: self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        }
    }
}

/// Plain copy of a [`Histogram`]: `(upper_bound_secs, cumulative_count)`
/// per bucket, plus the total count and sum. The implicit `+Inf` bucket
/// equals `count`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HistogramSnapshot {
    pub buckets: Vec<(f64, u64)>,
    pub count: u64,
    pub sum_secs: f64,
}

/// The engine's latency histograms, for the metrics exporter.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LatencySnapshot {
    pub handler_duration: HistogramSnapshot,
    pub queue_wait: HistogramSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative() {
        let h = Histogram::default();
        h.observe(0.001);
        h.observe(0.2);
        h.observe(45.0);
        h.observe(10_000.0); // beyond the last bucket: only in +Inf
        let s = h.snapshot();
        assert_eq!(s.count, 4);
        let at = |le: f64| s.buckets.iter().find(|(b, _)| *b == le).unwrap().1;
        assert_eq!(at(0.005), 1);
        assert_eq!(at(0.25), 2);
        assert_eq!(at(60.0), 3);
        assert_eq!(at(300.0), 3);
        assert!((s.sum_secs - 10_045.201).abs() < 0.001);
    }
}

/// Plain snapshot of [`EngineStats`].
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
pub struct StatsSnapshot {
    pub jobs_created: u64,
    pub jobs_completed: u64,
    pub jobs_failed: u64,
    pub jobs_retried: u64,
    pub jobs_dead_lettered: u64,
    pub workflows_created: u64,
    pub workflows_completed: u64,
    pub workflows_failed: u64,
}

/// Live, per-queue backlog figures read from the store: what an operator
/// watches to decide whether to add workers. Only non-terminal jobs are
/// counted, so the query stays cheap on a large history table. Durable
/// totals live in [`StatsSnapshot`] via `JobStore::count_stats`.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
pub struct QueueStats {
    pub queue: String,
    /// Claimable now: `pending` or `retrying` with `scheduled_at` in the past.
    pub pending: u64,
    /// Waiting for a future `scheduled_at` (a `run_at` job or a backoff retry).
    pub scheduled: u64,
    /// Currently leased by a worker.
    pub running: u64,
    /// Seconds the oldest claimable job has been waiting, `None` when nothing
    /// is claimable. The queue's primary health signal: it grows when workers
    /// cannot keep up and stays near zero when they can.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_pending_age_secs: Option<u64>,
}
