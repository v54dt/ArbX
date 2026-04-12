use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::enums::Venue;
use super::instrument::Instrument;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub venue: Venue,
    pub instrument: Instrument,
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_size: Decimal,
    pub ask_size: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookLevel {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub venue: Venue,
    pub instrument: Instrument,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub timestamp: DateTime<Utc>,
    pub local_timestamp: DateTime<Utc>,
}

/// Canonical key for order book lookup. Used by engine and all strategies.
/// Format: `"venue:base-quote:instrument_type"` all lowercase.
pub fn book_key(venue: Venue, instrument: &Instrument) -> String {
    format!(
        "{:?}:{}-{}:{:?}",
        venue, instrument.base, instrument.quote, instrument.instrument_type
    )
    .to_lowercase()
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<&OrderBookLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&OrderBookLevel> {
        self.asks.first()
    }

    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / Decimal::TWO),
            _ => None,
        }
    }

    pub fn spread_bps(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) if bid.price > Decimal::ZERO => {
                let spread = ask.price - bid.price;
                Some(spread / bid.price * Decimal::from(10_000))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use rust_decimal_macros::dec;

    fn empty_book() -> OrderBook {
        OrderBook {
            venue: Venue::Binance,
            instrument: Instrument {
                asset_class: AssetClass::Crypto,
                instrument_type: InstrumentType::Spot,
                base: "BTC".into(),
                quote: "USDT".into(),
                settle_currency: None,
                expiry: None,
            },
            bids: vec![],
            asks: vec![],
            timestamp: Utc::now(),
            local_timestamp: Utc::now(),
        }
    }

    fn book_with_levels(bid: Decimal, ask: Decimal) -> OrderBook {
        let mut ob = empty_book();
        ob.bids = vec![OrderBookLevel {
            price: bid,
            size: dec!(1),
        }];
        ob.asks = vec![OrderBookLevel {
            price: ask,
            size: dec!(1),
        }];
        ob
    }

    #[test]
    fn mid_price_correct() {
        let ob = book_with_levels(dec!(100), dec!(102));
        assert_eq!(ob.mid_price(), Some(dec!(101)));
    }

    #[test]
    fn mid_price_empty_book_returns_none() {
        assert_eq!(empty_book().mid_price(), None);
    }

    #[test]
    fn spread_bps_correct() {
        let ob = book_with_levels(dec!(100), dec!(101));
        assert_eq!(ob.spread_bps(), Some(dec!(100)));
    }

    #[test]
    fn spread_bps_empty_book_returns_none() {
        assert_eq!(empty_book().spread_bps(), None);
    }

    #[test]
    fn best_bid_returns_first() {
        let ob = book_with_levels(dec!(100), dec!(102));
        assert_eq!(ob.best_bid().unwrap().price, dec!(100));
    }

    #[test]
    fn best_ask_returns_first() {
        let ob = book_with_levels(dec!(100), dec!(102));
        assert_eq!(ob.best_ask().unwrap().price, dec!(102));
    }

    #[test]
    fn best_bid_empty_returns_none() {
        assert!(empty_book().best_bid().is_none());
    }

    #[test]
    fn best_ask_empty_returns_none() {
        assert!(empty_book().best_ask().is_none());
    }
}
