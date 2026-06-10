//! The janitor: the background loop that is the engine's self-healing
//! mechanism (the role message-queue redelivery plays in broker designs).
//!
//! Three sweeps, all safe to run concurrently on multiple servers:
//!
//! 1. **Expired leases** — `running` jobs whose `locked_until` passed are
//!    reclaimed (SKIP LOCKED) and routed through the normal failure policy,
//!    so a crashed worker consumes retry budget and eventually dead-letters
//!    instead of crash-looping forever.
//! 2. **Workflow self-heal** — two complementary queries: terminal jobs whose
//!    linked step never got its status write, and non-terminal workflows with
//!    no live jobs at all (a crash mid-`advance`). Both re-drives are
//!    idempotent and protected by the step-claim guard.
//! 3. **Retention** (opt-in) — terminal jobs/workflows/dead letters older
//!    than the window are deleted, behind an advisory lock on Postgres.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::Duration as ChronoDuration;

use super::Engine;
use crate::error::EngineError;
use crate::ports::JobStore;

/// Tuning for [`Engine::run_janitor`].
#[derive(Clone, Debug)]
pub struct JanitorConfig {
    /// Cadence of the expired-lease and workflow-self-heal sweeps.
    pub interval: StdDuration,
    /// Max rows handled per sweep per cycle.
    pub batch: usize,
    /// Lease taken on reclaimed jobs while they are routed through the
    /// failure policy.
    pub reclaim_lease_secs: u32,
    /// Delete terminal jobs/workflows/dead letters older than this. `None`
    /// (the default) keeps history forever — silently deleting history is the
    /// wrong default for a young project.
    pub retention: Option<StdDuration>,
    /// Cadence of the retention sweep (only consulted when `retention` is
    /// set).
    pub retention_interval: StdDuration,
}

impl Default for JanitorConfig {
    fn default() -> Self {
        Self {
            interval: StdDuration::from_secs(5),
            batch: 100,
            reclaim_lease_secs: 60,
            retention: None,
            retention_interval: StdDuration::from_secs(3600),
        }
    }
}

/// What one janitor cycle did (for logs and tests).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JanitorSweepReport {
    /// Expired leases reclaimed and routed through the failure policy.
    pub expired_leases: usize,
    /// Terminal jobs whose workflow step was re-driven.
    pub healed_steps: usize,
    /// Stalled workflows re-advanced.
    pub advanced_workflows: usize,
}

impl JanitorSweepReport {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

impl<JS> Engine<JS>
where
    JS: JobStore + 'static,
{
    /// Spawn the janitor loop. One per server is intended; concurrent
    /// janitors are safe (claims are SKIP LOCKED, heals are idempotent,
    /// retention is advisory-locked). Stops with [`Engine::shutdown`].
    pub fn run_janitor(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            tracing::info!(interval = ?engine.janitor.interval, "janitor started");
            let mut last_purge = tokio::time::Instant::now();
            loop {
                tokio::select! {
                    biased;
                    _ = engine.shutdown.cancelled() => break,
                    _ = tokio::time::sleep(engine.janitor.interval) => {}
                }
                let report = engine.janitor_sweep().await;
                if !report.is_empty() {
                    tracing::info!(
                        expired = report.expired_leases,
                        healed_steps = report.healed_steps,
                        advanced_workflows = report.advanced_workflows,
                        "janitor sweep"
                    );
                }
                if engine.janitor.retention.is_some()
                    && last_purge.elapsed() >= engine.janitor.retention_interval
                {
                    last_purge = tokio::time::Instant::now();
                    match engine.janitor_purge().await {
                        Ok(0) | Err(_) => {} // errors are logged inside
                        Ok(n) => tracing::info!(purged_jobs = n, "retention sweep"),
                    }
                }
            }
            tracing::info!("janitor stopped");
        })
    }

    /// Run the expired-lease and workflow-self-heal sweeps once. Public so
    /// tests (and operators) can drive healing deterministically; errors are
    /// logged, not returned, because each sweep is independent and the loop
    /// must outlive transient storage failures.
    pub async fn janitor_sweep(&self) -> JanitorSweepReport {
        let mut report = JanitorSweepReport::default();

        match self
            .store
            .claim_expired_leases(self.janitor.batch, self.janitor.reclaim_lease_secs)
            .await
        {
            Ok(leases) => {
                for lease in leases {
                    report.expired_leases += 1;
                    tracing::warn!(
                        job_id = %lease.job.id,
                        delivery_count = lease.job.delivery_count,
                        "lease expired; applying failure policy"
                    );
                    // A crash consumes retry budget: retry with backoff until
                    // max_retries, then dead-letter — exactly like a reported
                    // failure.
                    self.handle_failure(
                        &lease.job,
                        &lease.lease_token,
                        "lease expired: the worker crashed or stopped heartbeating",
                        true,
                    )
                    .await;
                }
            }
            Err(e) => tracing::warn!(error = %e, "expired-lease sweep failed"),
        }

        match self.store.stalled_step_jobs(self.janitor.batch).await {
            Ok(jobs) => {
                for job in jobs {
                    report.healed_steps += 1;
                    tracing::warn!(job_id = %job.id, "healing stalled workflow step");
                    self.resume_workflow(&job).await;
                }
            }
            Err(e) => tracing::warn!(error = %e, "stalled-step sweep failed"),
        }

        match self.store.stalled_workflow_ids(self.janitor.batch).await {
            Ok(ids) => {
                for id in ids {
                    report.advanced_workflows += 1;
                    if let Err(e) = self.scheduler.advance(&id).await {
                        tracing::warn!(workflow_id = %id, error = %e, "stalled-workflow advance failed");
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "stalled-workflow sweep failed"),
        }

        report
    }

    /// Run the retention sweep once (no-op unless `retention` is configured).
    /// Returns the number of jobs purged.
    pub async fn janitor_purge(&self) -> Result<u64, EngineError> {
        let Some(window) = self.janitor.retention else {
            return Ok(0);
        };
        let cutoff = self.clock.now()
            - ChronoDuration::from_std(window).unwrap_or_else(|_| ChronoDuration::seconds(0));
        match self.store.purge_terminal(cutoff).await {
            Ok(n) => Ok(n),
            Err(e) => {
                tracing::warn!(error = %e, "retention sweep failed");
                Err(e.into())
            }
        }
    }
}
