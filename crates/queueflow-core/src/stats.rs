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
}

impl EngineStats {
    #[inline]
    pub(crate) fn incr(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
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
