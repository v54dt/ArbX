use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::enums::{Side, Venue};
use super::instrument::Instrument;

/// Records one leg of an executed (or attempted) arbitrage trade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLeg {
    pub venue: Venue,
    pub instrument: Instrument,
    pub side: Side,
    pub intended_price: Decimal,
    pub intended_quantity: Decimal,
    pub order_id: Option<String>,
    pub submitted_at: DateTime<Utc>,
}

/// Outcome of a single arbitrage execution attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeOutcome {
    AllSubmitted,
    PartialFailure,
    RiskRejected,
}

/// A paired record of one arbitrage opportunity that was acted upon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLog {
    pub id: String,
    pub strategy_id: String,
    pub outcome: TradeOutcome,
    pub legs: SmallVec<[TradeLeg; 4]>,
    pub expected_gross_profit: Decimal,
    pub expected_fees: Decimal,
    pub expected_net_profit: Decimal,
    pub expected_net_profit_bps: Decimal,
    pub notional: Decimal,
    pub created_at: DateTime<Utc>,
}
