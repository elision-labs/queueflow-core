//! Cron expression parsing and next-occurrence computation.
//!
//! Expressions are evaluated in **UTC**. Standard 5-field crontab
//! (`minute hour day-of-month month day-of-week`) is normalized to the
//! 6-field seconds-first form the parser expects; 6/7-field input passes
//! through unchanged.

use std::str::FromStr;

use chrono::{DateTime, Utc};

/// Prepend a `0` seconds field to a standard 5-field expression.
fn normalize(expr: &str) -> String {
    let trimmed = expr.trim();
    if trimmed.split_whitespace().count() == 5 {
        format!("0 {trimmed}")
    } else {
        trimmed.to_string()
    }
}

/// Parse `expr`, or explain why it is invalid.
pub fn validate_expr(expr: &str) -> Result<(), String> {
    cron::Schedule::from_str(&normalize(expr))
        .map(|_| ())
        .map_err(|e| format!("invalid cron expression '{expr}': {e}"))
}

/// The first occurrence strictly after `after` (UTC).
pub fn next_occurrence(expr: &str, after: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let schedule = cron::Schedule::from_str(&normalize(expr))
        .map_err(|e| format!("invalid cron expression '{expr}': {e}"))?;
    schedule
        .after(&after)
        .next()
        .ok_or_else(|| format!("cron expression '{expr}' has no future occurrence"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn five_field_crontab_is_accepted_and_utc() {
        // Every 5 minutes.
        let next = next_occurrence("*/5 * * * *", at("2021-01-01T00:00:01Z")).unwrap();
        assert_eq!(next, at("2021-01-01T00:05:00Z"));
        // Daily at 03:30 UTC.
        let next = next_occurrence("30 3 * * *", at("2021-01-01T04:00:00Z")).unwrap();
        assert_eq!(next, at("2021-01-02T03:30:00Z"));
    }

    #[test]
    fn six_field_with_seconds_passes_through() {
        let next = next_occurrence("30 * * * * *", at("2021-01-01T00:00:00Z")).unwrap();
        assert_eq!(next, at("2021-01-01T00:00:30Z"));
    }

    #[test]
    fn occurrences_are_strictly_after() {
        // Asking exactly at an occurrence returns the following one.
        let next = next_occurrence("*/5 * * * *", at("2021-01-01T00:05:00Z")).unwrap();
        assert_eq!(next, at("2021-01-01T00:10:00Z"));
    }

    #[test]
    fn garbage_is_rejected_with_context() {
        let err = validate_expr("not a cron").unwrap_err();
        assert!(err.contains("not a cron"));
        assert!(validate_expr("*/5 * * * *").is_ok());
    }
}
