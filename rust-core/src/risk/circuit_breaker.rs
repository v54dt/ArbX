use rust_decimal::Decimal;
use std::collections::VecDeque;
use std::time::Instant;

pub struct CircuitBreaker {
    max_drawdown: Decimal,
    max_orders_per_minute: u32,
    recent_orders: VecDeque<Instant>,
    max_consecutive_failures: u32,
    consecutive_failures: u32,
    tripped: bool,
    trip_reason: Option<String>,
}

impl CircuitBreaker {
    pub fn new(
        max_drawdown: Decimal,
        max_orders_per_minute: u32,
        max_consecutive_failures: u32,
    ) -> Self {
        Self {
            max_drawdown,
            max_orders_per_minute,
            recent_orders: VecDeque::new(),
            max_consecutive_failures,
            consecutive_failures: 0,
            tripped: false,
            trip_reason: None,
        }
    }

    pub fn is_tripped(&self) -> bool {
        self.tripped
    }

    pub fn trip_reason(&self) -> Option<&str> {
        self.trip_reason.as_deref()
    }

    pub fn record_order(&mut self) {
        let now = Instant::now();
        self.recent_orders.push_back(now);
        while self
            .recent_orders
            .front()
            .is_some_and(|t| now.duration_since(*t).as_secs() > 60)
        {
            self.recent_orders.pop_front();
        }
        if self.recent_orders.len() as u32 > self.max_orders_per_minute {
            self.trip("order rate exceeded");
        }
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
    }

    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= self.max_consecutive_failures {
            self.trip("consecutive failures exceeded");
        }
    }

    pub fn check_drawdown(&mut self, realized_pnl: Decimal) {
        if realized_pnl < -self.max_drawdown {
            self.trip("max drawdown exceeded");
        }
    }

    fn trip(&mut self, reason: &str) {
        if !self.tripped {
            tracing::error!(reason, "CIRCUIT BREAKER TRIPPED");
            self.tripped = true;
            self.trip_reason = Some(reason.to_string());
        }
    }

    pub fn reset(&mut self) {
        self.tripped = false;
        self.trip_reason = None;
        self.consecutive_failures = 0;
        tracing::info!("circuit breaker reset");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn trips_on_drawdown() {
        let mut cb = CircuitBreaker::new(dec!(500), 100, 5);
        assert!(!cb.is_tripped());
        cb.check_drawdown(dec!(-501));
        assert!(cb.is_tripped());
        assert_eq!(cb.trip_reason(), Some("max drawdown exceeded"));
    }

    #[test]
    fn trips_on_order_rate() {
        let mut cb = CircuitBreaker::new(dec!(500), 100, 5);
        for _ in 0..101 {
            cb.record_order();
        }
        assert!(cb.is_tripped());
        assert_eq!(cb.trip_reason(), Some("order rate exceeded"));
    }

    #[test]
    fn trips_on_consecutive_failures() {
        let mut cb = CircuitBreaker::new(dec!(500), 100, 5);
        for _ in 0..5 {
            cb.record_failure();
        }
        assert!(cb.is_tripped());
        assert_eq!(cb.trip_reason(), Some("consecutive failures exceeded"));
    }

    #[test]
    fn success_resets_failure_count() {
        let mut cb = CircuitBreaker::new(dec!(500), 100, 5);
        for _ in 0..3 {
            cb.record_failure();
        }
        cb.record_success();
        for _ in 0..3 {
            cb.record_failure();
        }
        assert!(!cb.is_tripped());
    }

    #[test]
    fn reset_clears_trip() {
        let mut cb = CircuitBreaker::new(dec!(500), 100, 5);
        cb.check_drawdown(dec!(-600));
        assert!(cb.is_tripped());
        cb.reset();
        assert!(!cb.is_tripped());
        assert_eq!(cb.trip_reason(), None);
    }
}
