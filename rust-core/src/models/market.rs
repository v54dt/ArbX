use arrayvec::ArrayString;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::enums::Venue;
use super::instrument::Instrument;

pub type BookKey = ArrayString<64>;
pub type BookMap = FxHashMap<BookKey, OrderBook>;

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
    pub bids: SmallVec<[OrderBookLevel; 20]>,
    pub asks: SmallVec<[OrderBookLevel; 20]>,
    pub timestamp: DateTime<Utc>,
    pub local_timestamp: DateTime<Utc>,
}

/// `"venue:base-quote:instrument_type"` lowercase, stack-allocated.
pub fn book_key(venue: Venue, instrument: &Instrument) -> BookKey {
    use std::fmt::Write as _;
    let mut key = BookKey::new();
    write!(
        key,
        "{:?}:{}-{}:{:?}",
        venue, instrument.base, instrument.quote, instrument.instrument_type
    )
    .expect("book key exceeds 64 bytes");
    key.as_mut_str().make_ascii_lowercase();
    key
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

    /// Update book in-place from a top-of-book quote, reusing existing allocation.
    pub fn update_from_quote(&mut self, q: &Quote) {
        self.bids.clear();
        self.bids.push(OrderBookLevel {
            price: q.bid,
            size: q.bid_size,
        });
        self.asks.clear();
        self.asks.push(OrderBookLevel {
            price: q.ask,
            size: q.ask_size,
        });
        self.timestamp = q.timestamp;
        self.local_timestamp = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use rust_decimal_macros::dec;
    use smallvec::smallvec;

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
                last_trade_time: None,
                settlement_time: None,
            },
            bids: SmallVec::new(),
            asks: SmallVec::new(),
            timestamp: Utc::now(),
            local_timestamp: Utc::now(),
        }
    }

    fn book_with_levels(bid: Decimal, ask: Decimal) -> OrderBook {
        let mut ob = empty_book();
        ob.bids = smallvec![OrderBookLevel {
            price: bid,
            size: dec!(1),
        }];
        ob.asks = smallvec![OrderBookLevel {
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

    #[test]
    fn book_key_format_is_consistent() {
        let inst = Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };
        let key = book_key(Venue::Binance, &inst);
        assert_eq!(key.as_str(), "binance:btc-usdt:spot");
    }

    #[test]
    fn book_key_different_venues_produce_different_keys() {
        let inst = Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };
        let k1 = book_key(Venue::Binance, &inst);
        let k2 = book_key(Venue::Okx, &inst);
        assert_ne!(k1, k2);
        assert!(k1.starts_with("binance:"));
        assert!(k2.starts_with("okx:"));
    }

    #[test]
    fn book_key_different_instrument_types_produce_different_keys() {
        let spot = Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };
        let mut swap = spot.clone();
        swap.instrument_type = InstrumentType::Swap;

        let k1 = book_key(Venue::Binance, &spot);
        let k2 = book_key(Venue::Binance, &swap);
        assert_ne!(k1, k2);
        assert!(k1.ends_with(":spot"));
        assert!(k2.ends_with(":swap"));
    }

    #[test]
    fn update_from_quote_reuses_book() {
        let mut book = book_with_levels(dec!(100), dec!(101));
        let q = Quote {
            venue: Venue::Binance,
            instrument: book.instrument.clone(),
            bid: dec!(200),
            ask: dec!(201),
            bid_size: dec!(5),
            ask_size: dec!(3),
            timestamp: Utc::now(),
        };
        book.update_from_quote(&q);
        assert_eq!(book.bids[0].price, dec!(200));
        assert_eq!(book.asks[0].price, dec!(201));
        assert_eq!(book.bids[0].size, dec!(5));
    }
}
