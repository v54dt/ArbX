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
