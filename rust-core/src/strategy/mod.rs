pub mod base;
pub mod cross_exchange;
pub mod cross_venue_funding;
pub mod ewma_spread;
pub mod funding_rate;
pub mod multi_pair_cross_exchange;
pub mod signal_momentum;
pub mod triangular_arb;
pub mod tw_etf_futures;

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use smallvec::SmallVec;

use crate::models::enums::{Side, Venue};
use crate::models::instrument::Instrument;

pub type OpportunityId = String;
pub type StrategyId = String;

#[derive(Debug, Clone)]
pub enum OpportunityKind {
    CrossExchange,
    Triangular,
    SpotFuturesBasis {
        basis_bps: Decimal,
        days_to_expiry: u16,
    },
    StatArb {
        z_score: f64,
        hedge_ratio: Decimal,
    },
    FundingArb {
        annualized_diff_bps: Decimal,
    },
    /// Directional bet driven by external/internal signal confluence.
    /// `composite` is the signed signal score in [-1, 1]; positive = bullish.
    SignalMomentum {
        composite: Decimal,
    },
}

#[derive(Debug, Clone)]
pub struct Leg {
    pub venue: Venue,
    pub instrument: Instrument,
    pub side: Side,
    pub quote_price: Decimal,
    pub order_price: Decimal,
    pub quantity: Decimal,
    pub fee_estimate: Decimal,
}

#[derive(Debug, Clone)]
pub struct Economics {
    pub gross_profit: Decimal,
    pub fees_total: Decimal,
    pub net_profit: Decimal,
    pub net_profit_bps: Decimal,
    pub notional: Decimal,
}

/// Detection timing and provenance for an opportunity.
#[derive(Debug, Clone)]
pub struct OpportunityMeta {
    pub detected_at: DateTime<Utc>,
    pub quote_ts_per_leg: SmallVec<[DateTime<Utc>; 4]>,
    pub ttl: Duration,
    pub strategy_id: StrategyId,
}

/// A short-lived arbitrage opportunity to act on immediately.
#[derive(Debug, Clone)]
pub struct Opportunity {
    pub id: OpportunityId,
    pub kind: OpportunityKind,
    pub legs: SmallVec<[Leg; 4]>,
    pub economics: Economics,
    pub meta: OpportunityMeta,
}
