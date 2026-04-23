//! Signal-momentum strategy: trades when intelligence signals align with
//! book micro-structure. Combines external signals (news sentiment, exchange
//! flow, macro events) with internal signals (queue imbalance, microprice)
//! to generate directional trades.
//!
//! This is the first event-driven strategy — it only fires when signal
//! confluence exceeds a configurable threshold, not on every tick.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use smallvec::smallvec;

use crate::engine::signal::{
    SIGNAL_EXCHANGE_FLOW, SIGNAL_NEWS_SENTIMENT, SIGNAL_QUEUE_IMBALANCE, SignalCache,
};
use crate::models::enums::{OrderType, Side, TimeInForce, Venue};
use crate::models::fee::FeeSchedule;
use crate::models::instrument::Instrument;
use crate::models::market::{BookMap, book_key};
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;

use super::base::ArbitrageStrategy;
use super::{Economics, Leg, Opportunity, OpportunityKind, OpportunityMeta};

pub struct SignalMomentumStrategy {
    pub venue: Venue,
    pub instrument: Instrument,
    pub fee: FeeSchedule,
    /// Minimum composite signal strength to trade (0.0–1.0).
    pub min_signal_strength: Decimal,
    /// Maximum position quantity per trade.
    pub max_quantity: Decimal,
    /// Maximum quote age before staleness rejection (ms).
    pub max_quote_age_ms: i64,
    /// Opportunity TTL (ms).
    pub ttl_ms: i64,
}

/// Weights for combining different signal types into a composite score.
struct SignalWeights {
    news_sentiment: Decimal,
    exchange_flow: Decimal,
    queue_imbalance: Decimal,
}

impl Default for SignalWeights {
    fn default() -> Self {
        Self {
            news_sentiment: dec!(0.35),
            exchange_flow: dec!(0.35),
            queue_imbalance: dec!(0.30),
        }
    }
}

#[async_trait]
impl ArbitrageStrategy for SignalMomentumStrategy {
    async fn evaluate(
        &self,
        books: &BookMap,
        _portfolios: &HashMap<String, PortfolioSnapshot>,
        now: DateTime<Utc>,
        signals: &SignalCache,
    ) -> Option<Opportunity> {
        let key = book_key(self.venue, &self.instrument);
        let book = books.get(key.as_str())?;

        // Staleness check.
        let age_ms = (now - book.timestamp).num_milliseconds();
        if age_ms > self.max_quote_age_ms || age_ms < 0 {
            return None;
        }

        let bid = book.bids.first()?;
        let ask = book.asks.first()?;
        if bid.price.is_zero() || ask.price.is_zero() {
            return None;
        }

        // Gather signals — any missing signal contributes 0.
        let news = signals
            .get_value(key.as_str(), SIGNAL_NEWS_SENTIMENT)
            .unwrap_or(Decimal::ZERO);
        let flow = signals
            .get_value(key.as_str(), SIGNAL_EXCHANGE_FLOW)
            .unwrap_or(Decimal::ZERO);
        let qi = signals
            .get_value(key.as_str(), SIGNAL_QUEUE_IMBALANCE)
            .unwrap_or(Decimal::ZERO);

        // Also try global signals (instrument_key = "*:btc-usdt:*" etc.)
        let global_key = format!(
            "*:{}-{}:*",
            self.instrument.base.to_lowercase(),
            self.instrument.quote.to_lowercase()
        );
        let news = if news.is_zero() {
            signals
                .get_value(&global_key, SIGNAL_NEWS_SENTIMENT)
                .unwrap_or(Decimal::ZERO)
        } else {
            news
        };
        let flow = if flow.is_zero() {
            signals
                .get_value(&global_key, SIGNAL_EXCHANGE_FLOW)
                .unwrap_or(Decimal::ZERO)
        } else {
            flow
        };

        // Composite score: weighted average, range [-1, 1].
        let w = SignalWeights::default();
        let composite = news * w.news_sentiment + flow * w.exchange_flow + qi * w.queue_imbalance;

        // Threshold check — only trade when signals are strong enough.
        if composite.abs() < self.min_signal_strength {
            return None;
        }

        // Direction: positive composite = bullish (buy), negative = bearish (sell).
        let (side, order_price) = if composite > Decimal::ZERO {
            (Side::Buy, ask.price)
        } else {
            (Side::Sell, bid.price)
        };

        // Size: scale with signal strength, capped at max_quantity.
        let strength_ratio = composite.abs().min(Decimal::ONE);
        let quantity = (self.max_quantity * strength_ratio).max(dec!(0.001));

        let mid = (bid.price + ask.price) / Decimal::TWO;
        let fee_est = quantity * mid * self.fee.taker();
        let spread_cost = (ask.price - bid.price) * quantity;
        let net_profit = -spread_cost - fee_est; // entering a position costs spread + fees

        let opp = Opportunity {
            id: uuid::Uuid::new_v4().to_string(),
            kind: OpportunityKind::StatArb {
                z_score: composite.to_string().parse::<f64>().unwrap_or(0.0),
                hedge_ratio: Decimal::ONE,
            },
            legs: smallvec![Leg {
                venue: self.venue,
                instrument: self.instrument.clone(),
                side,
                quote_price: order_price,
                order_price,
                quantity,
                fee_estimate: fee_est,
            }],
            economics: Economics {
                gross_profit: -spread_cost,
                fees_total: fee_est,
                net_profit,
                net_profit_bps: if mid.is_zero() {
                    Decimal::ZERO
                } else {
                    net_profit / (quantity * mid) * dec!(10_000)
                },
                notional: quantity * mid,
            },
            meta: OpportunityMeta {
                detected_at: now,
                quote_ts_per_leg: smallvec![book.timestamp],
                ttl: Duration::milliseconds(self.ttl_ms),
                strategy_id: self.name().to_string(),
            },
        };

        tracing::info!(
            composite = %composite,
            side = ?side,
            qty = %quantity,
            news = %news,
            flow = %flow,
            qi = %qi,
            "signal_momentum opportunity detected"
        );

        Some(opp)
    }

    fn compute_hedge_orders(&self, opportunity: &Opportunity) -> Vec<OrderRequest> {
        opportunity
            .legs
            .iter()
            .map(|leg| OrderRequest {
                venue: leg.venue,
                instrument: leg.instrument.clone(),
                side: leg.side,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::Ioc),
                price: Some(leg.order_price),
                quantity: leg.quantity,
                estimated_notional: None,
            })
            .collect()
    }

    fn name(&self) -> &str {
        "signal_momentum"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::signal::SignalValue;
    use crate::models::instrument::{AssetClass, InstrumentType};
    use crate::models::market::{OrderBook, OrderBookLevel};

    fn btc_usdt() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Swap,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: Some("USDT".into()),
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn make_strategy() -> SignalMomentumStrategy {
        SignalMomentumStrategy {
            venue: Venue::Binance,
            instrument: btc_usdt(),
            fee: FeeSchedule::new(Venue::Binance, dec!(0.0002), dec!(0.0004)),
            min_signal_strength: dec!(0.2),
            max_quantity: dec!(1),
            max_quote_age_ms: 5000,
            ttl_ms: 3000,
        }
    }

    fn make_book(bid: Decimal, ask: Decimal) -> BookMap {
        let inst = btc_usdt();
        let key = book_key(Venue::Binance, &inst);
        let now = Utc::now();
        let book = OrderBook {
            venue: Venue::Binance,
            instrument: inst,
            bids: smallvec![OrderBookLevel {
                price: bid,
                size: dec!(10),
            }],
            asks: smallvec![OrderBookLevel {
                price: ask,
                size: dec!(10),
            }],
            timestamp: now,
            local_timestamp: now,
        };
        let mut map = BookMap::default();
        map.insert(key, book);
        map
    }

    fn make_signals(news: Decimal, flow: Decimal, qi: Decimal) -> SignalCache {
        let mut cache = SignalCache::new();
        let now = Utc::now();
        let key = book_key(Venue::Binance, &btc_usdt());
        if !news.is_zero() {
            cache.update(
                key.as_str(),
                SIGNAL_NEWS_SENTIMENT,
                SignalValue {
                    value: news,
                    confidence: Decimal::ONE,
                    timestamp: now,
                },
            );
        }
        if !flow.is_zero() {
            cache.update(
                key.as_str(),
                SIGNAL_EXCHANGE_FLOW,
                SignalValue {
                    value: flow,
                    confidence: Decimal::ONE,
                    timestamp: now,
                },
            );
        }
        if !qi.is_zero() {
            cache.update(
                key.as_str(),
                SIGNAL_QUEUE_IMBALANCE,
                SignalValue {
                    value: qi,
                    confidence: Decimal::ONE,
                    timestamp: now,
                },
            );
        }
        cache
    }

    #[tokio::test]
    async fn no_signals_no_trade() {
        let s = make_strategy();
        let books = make_book(dec!(50000), dec!(50010));
        let signals = SignalCache::new();
        let result = s
            .evaluate(&books, &HashMap::new(), Utc::now(), &signals)
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn weak_signals_no_trade() {
        let s = make_strategy();
        let books = make_book(dec!(50000), dec!(50010));
        // All signals at 0.1 — composite ~0.1, below threshold 0.2
        let signals = make_signals(dec!(0.1), dec!(0.1), dec!(0.1));
        let result = s
            .evaluate(&books, &HashMap::new(), Utc::now(), &signals)
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn strong_bullish_signals_buy() {
        let s = make_strategy();
        let books = make_book(dec!(50000), dec!(50010));
        // Strong bullish: news=0.8, flow=0.6, qi=0.5 → composite ~0.65
        let signals = make_signals(dec!(0.8), dec!(0.6), dec!(0.5));
        let result = s
            .evaluate(&books, &HashMap::new(), Utc::now(), &signals)
            .await;
        assert!(result.is_some());
        let opp = result.unwrap();
        assert_eq!(opp.legs[0].side, Side::Buy);
        assert!(opp.legs[0].quantity > Decimal::ZERO);
    }

    #[tokio::test]
    async fn strong_bearish_signals_sell() {
        let s = make_strategy();
        let books = make_book(dec!(50000), dec!(50010));
        // Strong bearish: news=-0.7, flow=-0.8, qi=-0.4 → composite ~-0.64
        let signals = make_signals(dec!(-0.7), dec!(-0.8), dec!(-0.4));
        let result = s
            .evaluate(&books, &HashMap::new(), Utc::now(), &signals)
            .await;
        assert!(result.is_some());
        let opp = result.unwrap();
        assert_eq!(opp.legs[0].side, Side::Sell);
    }

    #[tokio::test]
    async fn quantity_scales_with_strength() {
        let s = make_strategy();
        let books = make_book(dec!(50000), dec!(50010));

        // Moderate signal
        let moderate = make_signals(dec!(0.4), dec!(0.3), dec!(0.3));
        let r1 = s
            .evaluate(&books, &HashMap::new(), Utc::now(), &moderate)
            .await
            .unwrap();

        // Strong signal
        let strong = make_signals(dec!(0.9), dec!(0.9), dec!(0.9));
        let r2 = s
            .evaluate(&books, &HashMap::new(), Utc::now(), &strong)
            .await
            .unwrap();

        assert!(r2.legs[0].quantity > r1.legs[0].quantity);
    }

    #[tokio::test]
    async fn stale_book_rejected() {
        let mut s = make_strategy();
        s.max_quote_age_ms = 0; // instant staleness
        let books = make_book(dec!(50000), dec!(50010));
        let signals = make_signals(dec!(0.8), dec!(0.8), dec!(0.8));
        // Book timestamp is "now" but max_quote_age_ms=0 means it's instantly stale
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        let now = Utc::now();
        let result = s.evaluate(&books, &HashMap::new(), now, &signals).await;
        assert!(result.is_none());
    }
}
