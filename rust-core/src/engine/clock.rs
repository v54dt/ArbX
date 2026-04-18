//! Clock abstraction for deterministic replay + testability.
//!
//! `LiveClock` wraps `chrono::Utc::now()` for production.
//! `TestClock` lets callers set time explicitly — enables deterministic
//! fixture replay and TTL / staleness tests without wall-clock dependency.

use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicI64, Ordering};

pub trait Clock: Send + Sync {
    fn utc_now(&self) -> DateTime<Utc>;
}

pub struct LiveClock;

impl Clock for LiveClock {
    fn utc_now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Caller-controlled clock for tests and deterministic replay.
/// Thread-safe via atomic i64 (millisecond precision).
#[allow(dead_code)]
pub struct TestClock {
    millis: AtomicI64,
}

#[allow(dead_code)]
impl TestClock {
    pub fn new(initial: DateTime<Utc>) -> Self {
        Self {
            millis: AtomicI64::new(initial.timestamp_millis()),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        self.millis.store(t.timestamp_millis(), Ordering::Relaxed);
    }

    pub fn advance_ms(&self, ms: i64) {
        self.millis.fetch_add(ms, Ordering::Relaxed);
    }
}

impl Clock for TestClock {
    fn utc_now(&self) -> DateTime<Utc> {
        let ms = self.millis.load(Ordering::Relaxed);
        DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_clock_returns_current_time() {
        let before = Utc::now();
        let clock = LiveClock;
        let t = clock.utc_now();
        let after = Utc::now();
        assert!(t >= before && t <= after);
    }

    #[test]
    fn test_clock_returns_set_time() {
        let fixed = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let clock = TestClock::new(fixed);
        assert_eq!(clock.utc_now(), fixed);
    }

    #[test]
    fn test_clock_advance() {
        let start = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let clock = TestClock::new(start);
        clock.advance_ms(500);
        let expected = DateTime::from_timestamp_millis(1_700_000_000_500).unwrap();
        assert_eq!(clock.utc_now(), expected);
    }
}
