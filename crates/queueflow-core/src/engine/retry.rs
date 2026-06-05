//! Typed retry backoff.
//!
//! Replaces the Go reference's stringly-typed `retry_backoff` (and its fragile
//! detached-goroutine timer) with an exhaustive enum and a pure, testable delay
//! function. The *scheduling* of the retry is durable: the engine asks the
//! queue to redeliver after [`BackoffStrategy::next_retry_at`], so a retry
//! survives a process restart.

use chrono::{DateTime, Duration, Utc};

use crate::domain::{BackoffStrategy, JobConfig};

impl BackoffStrategy {
    /// Deterministic delay (in seconds) before retrying, given a zero-based
    /// `attempt` index. Capped by `retry_max_delay_secs`. No jitter — used for
    /// tests and inspection.
    pub fn delay_secs(self, cfg: &JobConfig, attempt: u32) -> u64 {
        let base = cfg.retry_delay_secs as f64;
        let raw = match self {
            BackoffStrategy::Fixed => base,
            BackoffStrategy::Linear => base * (attempt as f64 + 1.0),
            // 2^attempt, guarding against overflow for absurd attempt counts.
            BackoffStrategy::Exponential => base * 2f64.powi(attempt.min(62) as i32),
        };
        raw.min(cfg.retry_max_delay_secs as f64).max(0.0) as u64
    }

    /// The wall-clock instant at which a retry should next become visible,
    /// applying optional jitter from `cfg.jitter_factor`.
    pub fn next_retry_at(self, cfg: &JobConfig, attempt: u32, now: DateTime<Utc>) -> DateTime<Utc> {
        let base = self.delay_secs(cfg, attempt) as f64;
        let secs = match cfg.jitter_factor {
            Some(j) if j > 0.0 => {
                let f = j.clamp(0.0, 1.0);
                // Uniformly in [base*(1-f), base*(1+f)].
                let r: f64 = rand::random();
                base * (1.0 + (r - 0.5) * 2.0 * f)
            }
            _ => base,
        };
        now + Duration::seconds(secs.max(0.0).round() as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> JobConfig {
        JobConfig {
            retry_delay_secs: 60,
            retry_max_delay_secs: 3600,
            jitter_factor: None,
            ..Default::default()
        }
    }

    #[test]
    fn fixed_is_constant() {
        let c = JobConfig {
            retry_backoff: BackoffStrategy::Fixed,
            ..cfg()
        };
        assert_eq!(BackoffStrategy::Fixed.delay_secs(&c, 0), 60);
        assert_eq!(BackoffStrategy::Fixed.delay_secs(&c, 5), 60);
    }

    #[test]
    fn linear_grows_by_attempt() {
        assert_eq!(BackoffStrategy::Linear.delay_secs(&cfg(), 0), 60);
        assert_eq!(BackoffStrategy::Linear.delay_secs(&cfg(), 1), 120);
        assert_eq!(BackoffStrategy::Linear.delay_secs(&cfg(), 2), 180);
    }

    #[test]
    fn exponential_doubles_and_caps() {
        assert_eq!(BackoffStrategy::Exponential.delay_secs(&cfg(), 0), 60);
        assert_eq!(BackoffStrategy::Exponential.delay_secs(&cfg(), 1), 120);
        assert_eq!(BackoffStrategy::Exponential.delay_secs(&cfg(), 2), 240);
        // capped at retry_max_delay_secs
        assert_eq!(BackoffStrategy::Exponential.delay_secs(&cfg(), 20), 3600);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let c = JobConfig {
            jitter_factor: Some(0.1),
            ..cfg()
        };
        let now = Utc::now();
        for _ in 0..200 {
            let at = BackoffStrategy::Exponential.next_retry_at(&c, 1, now);
            let secs = (at - now).num_seconds();
            assert!((108..=132).contains(&secs), "out of bounds: {secs}");
        }
    }
}
