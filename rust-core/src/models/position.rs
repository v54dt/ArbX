use chrono::{NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::enums::Venue;
use super::instrument::Instrument;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub venue: Venue,
    pub instrument: Instrument,
    pub quantity: Decimal,
    pub average_cost: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    #[serde(default)]
    pub settlement_date: Option<NaiveDate>,
}

impl Position {
    // Compares against UTC date; TW settlement is in Asia/Taipei timezone
    pub fn is_settled(&self) -> bool {
        match self.settlement_date {
            Some(date) => Utc::now().date_naive() >= date,
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use chrono::Days;
    use rust_decimal_macros::dec;

    fn sample_position(settlement_date: Option<NaiveDate>) -> Position {
        Position {
            venue: Venue::Fubon,
            instrument: Instrument {
                asset_class: AssetClass::Equity,
                instrument_type: InstrumentType::Spot,
                base: "2330".into(),
                quote: "TWD".into(),
                settle_currency: Some("TWD".into()),
                expiry: None,
                last_trade_time: None,
                settlement_time: None,
            },
            quantity: dec!(1000),
            average_cost: dec!(600),
            unrealized_pnl: dec!(0),
            realized_pnl: dec!(0),
            settlement_date,
        }
    }

    #[test]
    fn position_is_settled_when_no_date() {
        let pos = sample_position(None);
        assert!(pos.is_settled());
    }

    #[test]
    fn position_is_settled_when_past_date() {
        let yesterday = Utc::now()
            .date_naive()
            .checked_sub_days(Days::new(1))
            .unwrap();
        let pos = sample_position(Some(yesterday));
        assert!(pos.is_settled());
    }

    #[test]
    fn position_not_settled_when_future_date() {
        let tomorrow = Utc::now()
            .date_naive()
            .checked_add_days(Days::new(1))
            .unwrap();
        let pos = sample_position(Some(tomorrow));
        assert!(!pos.is_settled());
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub venue: Venue,
    pub positions: Vec<Position>,
    pub total_equity: Decimal,
    pub available_balance: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
}
