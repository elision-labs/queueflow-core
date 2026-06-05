//! Clock adapters.

use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};

use crate::ports::Clock;

/// Wall-clock time. Use in production.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A controllable clock for deterministic tests. Time only advances when the
/// test calls [`TestClock::advance`], which makes retry/backoff and
/// visibility-timeout behaviour reproducible without any real sleeping.
#[derive(Debug)]
pub struct TestClock {
    now: Mutex<DateTime<Utc>>,
}

impl TestClock {
    pub fn at(t: DateTime<Utc>) -> Self {
        Self { now: Mutex::new(t) }
    }

    /// A fixed, readable epoch (2021-01-01T00:00:00Z) to anchor tests.
    pub fn epoch() -> Self {
        Self::at(
            DateTime::parse_from_rfc3339("2021-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        )
    }

    /// Move time forward.
    pub fn advance(&self, by: Duration) {
        let mut g = self.now.lock().unwrap();
        *g += by;
    }

    /// Move time forward by whole seconds (ergonomic helper).
    pub fn advance_secs(&self, secs: i64) {
        self.advance(Duration::seconds(secs));
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_only_moves_when_advanced() {
        let c = TestClock::epoch();
        let t0 = c.now();
        assert_eq!(c.now(), t0);
        c.advance_secs(61);
        assert_eq!(c.now(), t0 + Duration::seconds(61));
    }
}
