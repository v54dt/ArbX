//! Engine-internal broadcast event bus.
//!
//! `EngineEventBus` wraps a `tokio::sync::broadcast` channel carrying
//! `EngineEvent` variants. Subscribers that fall behind lose events (lossy
//! semantics — acceptable for observability; the TUI / replay / alerting
//! consumers are best-effort).
//!
//! Wire-up: engine publishes events at key state transitions; consumers
//! subscribe via `bus.subscribe()`. Current consumers: none yet (PR2 will
//! migrate TUI from Prometheus polling to bus subscription).

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tokio::sync::broadcast;

use crate::models::enums::Venue;

/// Capacity of the broadcast channel. Slow subscribers that fall behind by
/// this many events start dropping the oldest. 4096 covers ~1 s of 4 kHz
/// quote throughput with headroom.
const BUS_CAPACITY: usize = 4096;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum EngineEvent {
    QuoteReceived {
        venue: Venue,
        symbol: String,
        ts: DateTime<Utc>,
    },
    OpportunityDetected {
        strategy: String,
        id: String,
        net_profit_bps: Decimal,
    },
    OpportunityRejected {
        strategy: String,
        reason: String,
    },
    OrderSubmitted {
        strategy: String,
        venue: Venue,
        order_id: String,
    },
    OrderFilled {
        venue: Venue,
        order_id: String,
        price: Decimal,
        quantity: Decimal,
    },
    CircuitBreakerTripped {
        reason: String,
    },
    Reconciled {
        succeeded: usize,
        failed: usize,
    },
    /// Daily PnL counters were reset at the configured trading-day boundary.
    /// Operators monitoring `/recent-events` see this event mark the moment
    /// when capital was rearmed (review §2.6).
    DailyPnLReset {
        scope: String,
        prev_pnl: Decimal,
    },
    Paused,
    Resumed,
    Shutdown,
}

#[derive(Clone)]
pub struct EngineEventBus {
    tx: broadcast::Sender<EngineEvent>,
}

impl EngineEventBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BUS_CAPACITY);
        Self { tx }
    }

    pub fn publish(&self, event: EngineEvent) {
        // Ignore send error — means no active subscribers.
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.tx.subscribe()
    }
}

impl Default for EngineEventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn publish_and_receive() {
        let bus = EngineEventBus::new();
        let mut rx = bus.subscribe();
        bus.publish(EngineEvent::OpportunityDetected {
            strategy: "test".into(),
            id: "opp-1".into(),
            net_profit_bps: dec!(5),
        });
        let event = rx.recv().await.unwrap();
        assert!(matches!(event, EngineEvent::OpportunityDetected { .. }));
    }

    #[tokio::test]
    async fn no_subscriber_doesnt_panic() {
        let bus = EngineEventBus::new();
        bus.publish(EngineEvent::Shutdown);
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let bus = EngineEventBus::new();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        bus.publish(EngineEvent::Paused);
        assert!(matches!(rx1.recv().await.unwrap(), EngineEvent::Paused));
        assert!(matches!(rx2.recv().await.unwrap(), EngineEvent::Paused));
    }
}
