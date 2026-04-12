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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use rust_decimal_macros::dec;
    use smallvec::smallvec;

    fn sample_trade_log() -> TradeLog {
        TradeLog {
            id: "t1".into(),
            strategy_id: "s1".into(),
            outcome: TradeOutcome::AllSubmitted,
            legs: smallvec![TradeLeg {
                venue: Venue::Binance,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type: InstrumentType::Spot,
                    base: "BTC".into(),
                    quote: "USDT".into(),
                    settle_currency: None,
                    expiry: None,
                    last_trade_time: None,
                    settlement_time: None,
                },
                side: Side::Buy,
                intended_price: dec!(50000),
                intended_quantity: dec!(1),
                order_id: Some("o1".into()),
                submitted_at: Utc::now(),
            }],
            expected_gross_profit: dec!(10),
            expected_fees: dec!(2),
            expected_net_profit: dec!(8),
            expected_net_profit_bps: dec!(5),
            notional: dec!(50000),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn trade_log_serialization_roundtrip() {
        let log = sample_trade_log();
        let json = serde_json::to_string(&log).unwrap();
        let de: TradeLog = serde_json::from_str(&json).unwrap();
        assert_eq!(de.id, log.id);
        assert_eq!(de.strategy_id, log.strategy_id);
        assert_eq!(de.outcome, log.outcome);
        assert_eq!(de.legs.len(), 1);
        assert_eq!(de.expected_net_profit, dec!(8));
        assert_eq!(de.notional, dec!(50000));
    }

    #[test]
    fn trade_outcome_variants() {
        for variant in [
            TradeOutcome::AllSubmitted,
            TradeOutcome::PartialFailure,
            TradeOutcome::RiskRejected,
        ] {
            let json = serde_json::to_value(variant).unwrap();
            let de: TradeOutcome = serde_json::from_value(json).unwrap();
            assert_eq!(de, variant);
        }
    }
}
