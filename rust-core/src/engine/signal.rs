use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::models::market::{OrderBook, Quote};

/// Unique identifier for a signal type.
pub type SignalId = &'static str;

// Well-known signal IDs
pub const SIGNAL_QUEUE_IMBALANCE: SignalId = "queue_imbalance";
pub const SIGNAL_MICROPRICE: SignalId = "microprice";
pub const SIGNAL_OFI: SignalId = "ofi";
pub const SIGNAL_VPIN: SignalId = "vpin";
pub const SIGNAL_FUNDING_Z: SignalId = "funding_z";
pub const SIGNAL_NEWS_SENTIMENT: SignalId = "news_sentiment";
pub const SIGNAL_WHALE_ALERT: SignalId = "whale_alert";
pub const SIGNAL_MACRO_EVENT: SignalId = "macro_event";
pub const SIGNAL_EXCHANGE_FLOW: SignalId = "exchange_flow";

/// A single signal data point.
#[derive(Debug, Clone)]
pub struct SignalValue {
    pub value: Decimal,
    pub confidence: Decimal,
    pub timestamp: DateTime<Utc>,
}

/// A signal that arrived over IPC (Aeron) before being routed into the
/// `SignalCache`. Carries the routing key alongside the value so the
/// engine can dispatch without re-parsing.
#[derive(Debug, Clone)]
pub struct ExternalSignal {
    pub instrument_key: String,
    pub signal_id: String,
    pub value: Decimal,
    pub confidence: Decimal,
    pub timestamp: DateTime<Utc>,
}

/// Cache of latest signal values, keyed by (instrument_key, signal_id).
/// Strategies can read this during evaluate() without subscribing to a stream.
#[derive(Debug, Clone, Default)]
pub struct SignalCache {
    entries: HashMap<(String, SignalId), SignalValue>,
}

impl SignalCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn update(&mut self, instrument_key: &str, signal_id: SignalId, value: SignalValue) {
        self.entries
            .insert((instrument_key.to_string(), signal_id), value);
    }

    pub fn get(&self, instrument_key: &str, signal_id: SignalId) -> Option<&SignalValue> {
        self.entries.get(&(instrument_key.to_string(), signal_id))
    }

    pub fn get_value(&self, instrument_key: &str, signal_id: SignalId) -> Option<Decimal> {
        self.get(instrument_key, signal_id).map(|s| s.value)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove entries older than `max_age` relative to `now`.
    pub fn evict_stale(&mut self, now: DateTime<Utc>, max_age: chrono::Duration) {
        self.entries.retain(|_, v| now - v.timestamp < max_age);
    }
}

/// Pluggable signal generator. Receives market data, produces signal values.
pub trait SignalProducer: Send + Sync {
    fn name(&self) -> SignalId;

    /// Called on every quote update. Return Some to update the cache.
    fn on_quote(&mut self, _key: &str, _quote: &Quote) -> Option<SignalValue> {
        None
    }

    /// Called on every order book update. Return Some to update the cache.
    fn on_book(&mut self, _key: &str, _book: &OrderBook) -> Option<SignalValue> {
        None
    }
}

/// Queue imbalance = (bid_size - ask_size) / (bid_size + ask_size)
/// Ranges from -1.0 (pure ask pressure) to +1.0 (pure bid pressure).
pub struct QueueImbalanceProducer;

impl SignalProducer for QueueImbalanceProducer {
    fn name(&self) -> SignalId {
        SIGNAL_QUEUE_IMBALANCE
    }

    fn on_quote(&mut self, _key: &str, quote: &Quote) -> Option<SignalValue> {
        let total = quote.bid_size + quote.ask_size;
        if total.is_zero() {
            return None;
        }
        let imbalance = (quote.bid_size - quote.ask_size) / total;
        Some(SignalValue {
            value: imbalance,
            confidence: Decimal::ONE,
            timestamp: quote.timestamp,
        })
    }
}

/// Microprice = (ask_price * bid_size + bid_price * ask_size) / (bid_size + ask_size)
/// Size-weighted mid-price that adjusts toward the heavier side.
pub struct MicropriceProducer;

impl SignalProducer for MicropriceProducer {
    fn name(&self) -> SignalId {
        SIGNAL_MICROPRICE
    }

    fn on_quote(&mut self, _key: &str, quote: &Quote) -> Option<SignalValue> {
        let total = quote.bid_size + quote.ask_size;
        if total.is_zero() {
            return None;
        }
        let microprice = (quote.ask * quote.bid_size + quote.bid * quote.ask_size) / total;
        Some(SignalValue {
            value: microprice,
            confidence: Decimal::ONE,
            timestamp: quote.timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::Venue;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use chrono::TimeDelta;
    use rust_decimal_macros::dec;

    fn make_quote(bid: Decimal, ask: Decimal, bid_size: Decimal, ask_size: Decimal) -> Quote {
        Quote {
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
            bid,
            ask,
            bid_size,
            ask_size,
            timestamp: Utc::now(),
        }
    }

    // --- SignalCache tests ---

    #[test]
    fn cache_update_and_get() {
        let mut cache = SignalCache::new();
        let val = SignalValue {
            value: dec!(0.5),
            confidence: Decimal::ONE,
            timestamp: Utc::now(),
        };
        cache.update("btc-usdt", SIGNAL_QUEUE_IMBALANCE, val.clone());
        let got = cache.get("btc-usdt", SIGNAL_QUEUE_IMBALANCE).unwrap();
        assert_eq!(got.value, dec!(0.5));
    }

    #[test]
    fn cache_get_value() {
        let mut cache = SignalCache::new();
        cache.update(
            "btc-usdt",
            SIGNAL_MICROPRICE,
            SignalValue {
                value: dec!(50000),
                confidence: Decimal::ONE,
                timestamp: Utc::now(),
            },
        );
        assert_eq!(
            cache.get_value("btc-usdt", SIGNAL_MICROPRICE),
            Some(dec!(50000))
        );
    }

    #[test]
    fn cache_empty_returns_none() {
        let cache = SignalCache::new();
        assert!(cache.get("btc-usdt", SIGNAL_QUEUE_IMBALANCE).is_none());
        assert!(cache.get_value("btc-usdt", SIGNAL_MICROPRICE).is_none());
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn cache_evict_stale() {
        let mut cache = SignalCache::new();
        let now = Utc::now();
        let old_ts = now - TimeDelta::seconds(120);
        let fresh_ts = now - TimeDelta::seconds(30);

        cache.update(
            "old-key",
            SIGNAL_OFI,
            SignalValue {
                value: dec!(1),
                confidence: Decimal::ONE,
                timestamp: old_ts,
            },
        );
        cache.update(
            "fresh-key",
            SIGNAL_OFI,
            SignalValue {
                value: dec!(2),
                confidence: Decimal::ONE,
                timestamp: fresh_ts,
            },
        );
        assert_eq!(cache.len(), 2);

        cache.evict_stale(now, chrono::Duration::seconds(60));
        assert_eq!(cache.len(), 1);
        assert!(cache.get("old-key", SIGNAL_OFI).is_none());
        assert!(cache.get("fresh-key", SIGNAL_OFI).is_some());
    }

    #[test]
    fn cache_len_and_is_empty() {
        let mut cache = SignalCache::new();
        assert!(cache.is_empty());
        cache.update(
            "a",
            SIGNAL_VPIN,
            SignalValue {
                value: dec!(0),
                confidence: Decimal::ONE,
                timestamp: Utc::now(),
            },
        );
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
    }

    // --- QueueImbalanceProducer tests ---

    #[test]
    fn queue_imbalance_balanced() {
        let mut producer = QueueImbalanceProducer;
        let q = make_quote(dec!(100), dec!(101), dec!(10), dec!(10));
        let sv = producer.on_quote("k", &q).unwrap();
        assert_eq!(sv.value, dec!(0));
    }

    #[test]
    fn queue_imbalance_bid_heavy() {
        let mut producer = QueueImbalanceProducer;
        let q = make_quote(dec!(100), dec!(101), dec!(30), dec!(10));
        let sv = producer.on_quote("k", &q).unwrap();
        assert_eq!(sv.value, dec!(0.5)); // (30-10)/40
    }

    #[test]
    fn queue_imbalance_ask_heavy() {
        let mut producer = QueueImbalanceProducer;
        let q = make_quote(dec!(100), dec!(101), dec!(10), dec!(30));
        let sv = producer.on_quote("k", &q).unwrap();
        assert_eq!(sv.value, dec!(-0.5)); // (10-30)/40
    }

    #[test]
    fn queue_imbalance_zero_sizes() {
        let mut producer = QueueImbalanceProducer;
        let q = make_quote(dec!(100), dec!(101), dec!(0), dec!(0));
        assert!(producer.on_quote("k", &q).is_none());
    }

    // --- MicropriceProducer tests ---

    #[test]
    fn microprice_equal_sizes() {
        let mut producer = MicropriceProducer;
        let q = make_quote(dec!(100), dec!(102), dec!(10), dec!(10));
        let sv = producer.on_quote("k", &q).unwrap();
        // (102*10 + 100*10) / 20 = 2020/20 = 101
        assert_eq!(sv.value, dec!(101));
    }

    #[test]
    fn microprice_bid_heavy() {
        let mut producer = MicropriceProducer;
        let q = make_quote(dec!(100), dec!(102), dec!(30), dec!(10));
        let sv = producer.on_quote("k", &q).unwrap();
        // (102*30 + 100*10) / 40 = (3060+1000)/40 = 4060/40 = 101.5
        assert_eq!(sv.value, dec!(101.5));
    }

    #[test]
    fn microprice_zero_sizes() {
        let mut producer = MicropriceProducer;
        let q = make_quote(dec!(100), dec!(102), dec!(0), dec!(0));
        assert!(producer.on_quote("k", &q).is_none());
    }

    #[test]
    fn producer_name_constants() {
        assert_eq!(QueueImbalanceProducer.name(), "queue_imbalance");
        assert_eq!(MicropriceProducer.name(), "microprice");
    }

    #[test]
    fn producer_default_on_book_returns_none() {
        let mut qi = QueueImbalanceProducer;
        let mut mp = MicropriceProducer;
        let book = OrderBook {
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
            bids: Default::default(),
            asks: Default::default(),
            timestamp: Utc::now(),
            local_timestamp: Utc::now(),
        };
        assert!(qi.on_book("k", &book).is_none());
        assert!(mp.on_book("k", &book).is_none());
    }
}
