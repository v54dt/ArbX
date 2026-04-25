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
    /// Expected price move (in bps) when the composite signal is at full
    /// strength (|composite| = 1). At lower strengths the expected move
    /// scales linearly. This is the strategy's alpha estimate — calibrated
    /// from historical signal-to-return regressions, not assumed.
    pub alpha_bps_at_full_signal: Decimal,
    /// Minimum required net_profit_bps after fees + spread for the
    /// strategy to fire. Acts as the cost-floor: if the signal-implied
    /// edge doesn't beat half-spread + fees by this margin, skip.
    pub min_net_profit_bps: Decimal,
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

        // Size: scale with signal strength, capped at max_quantity. No
        // floor — if max_quantity is 0.0005 then qty stays 0.0005, never
        // gets lifted to 0.001 against operator intent.
        let strength_ratio = composite.abs().min(Decimal::ONE);
        let quantity = self.max_quantity * strength_ratio;
        if quantity.is_zero() {
            return None;
        }

        let mid = (bid.price + ask.price) / Decimal::TWO;
        if mid.is_zero() {
            return None;
        }
        let notional = quantity * mid;

        // Cost side: pay the half-spread on entry (cross from mid to taker
        // touch) plus taker fee.
        let half_spread = (ask.price - bid.price) / Decimal::TWO;
        let spread_cost = half_spread * quantity;
        let fee_est = notional * self.fee.taker();
        let cost_total = spread_cost + fee_est;

        // Edge side: alpha estimate scaled by signal strength.
        // expected_move_bps = composite.abs() * alpha_bps_at_full_signal.
        let expected_move_bps = strength_ratio * self.alpha_bps_at_full_signal;
        let expected_gross = notional * expected_move_bps / dec!(10_000);
        let net_profit = expected_gross - cost_total;
        let net_profit_bps = net_profit / notional * dec!(10_000);

        if net_profit_bps < self.min_net_profit_bps {
            return None;
        }

        let opp = Opportunity {
            // Deterministic ID keyed off (now, strategy, side) so replay
            // produces stable opportunity IDs. Other strategies follow the
            // same pattern (timestamp_nanos based) per #216.
            id: format!(
                "signal-momentum:{}:{}:{:?}",
                self.venue as u8,
                now.timestamp_nanos_opt().unwrap_or(0),
                side
            ),
            kind: OpportunityKind::SignalMomentum { composite },
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
                gross_profit: expected_gross,
                fees_total: fee_est,
                net_profit,
                net_profit_bps,
                notional,
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

    fn re_verify(&self, opp: &Opportunity, books: &BookMap) -> Option<Opportunity> {
        let leg = opp.legs.first()?;
        let key = book_key(leg.venue, &leg.instrument);
        let book = books.get(key.as_str())?;
        let bid = book.bids.first()?;
        let ask = book.asks.first()?;
        if bid.price.is_zero() || ask.price.is_zero() {
            return None;
        }
        // Spread re-check: if the half-spread blew out enough that the
        // original alpha estimate no longer covers cost + min_net_profit_bps,
        // skip. Recompute net_profit_bps inline from current book.
        let mid = (bid.price + ask.price) / Decimal::TWO;
        if mid.is_zero() {
            return None;
        }
        let qty = leg.quantity;
        let notional = qty * mid;
        let half_spread = (ask.price - bid.price) / Decimal::TWO;
        let spread_cost = half_spread * qty;
        let fee_est = notional * self.fee.taker();
        let composite_strength = match opp.kind {
            OpportunityKind::SignalMomentum { composite } => composite.abs().min(Decimal::ONE),
            _ => return Some(opp.clone()),
        };
        let expected_gross =
            notional * (composite_strength * self.alpha_bps_at_full_signal) / dec!(10_000);
        let net_profit = expected_gross - spread_cost - fee_est;
        let net_profit_bps = net_profit / notional * dec!(10_000);
        if net_profit_bps < self.min_net_profit_bps {
            return None;
        }
        let mut updated = opp.clone();
        updated.economics.gross_profit = expected_gross;
        updated.economics.fees_total = fee_est;
        updated.economics.net_profit = net_profit;
        updated.economics.net_profit_bps = net_profit_bps;
        updated.economics.notional = notional;
        Some(updated)
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
            // 50 bps move at full signal — comfortably above ~5 bps cost
            // (1bp half-spread on 50000/50010 + 4bps taker) so tests fire.
            alpha_bps_at_full_signal: dec!(50),
            min_net_profit_bps: dec!(0),
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

    /// Regression for review_new.md §1.2: previously net_profit was always
    /// negative because it only counted spread + fee. Now expected_gross
    /// (alpha-implied move) is included, so a strong signal yields net > 0.
    #[tokio::test]
    async fn strong_signal_produces_positive_net_profit() {
        let s = make_strategy();
        let books = make_book(dec!(50000), dec!(50010));
        let signals = make_signals(dec!(0.9), dec!(0.9), dec!(0.9));
        let opp = s
            .evaluate(&books, &HashMap::new(), Utc::now(), &signals)
            .await
            .unwrap();
        assert!(
            opp.economics.net_profit > Decimal::ZERO,
            "net_profit should be positive when alpha exceeds spread+fee; got {}",
            opp.economics.net_profit
        );
        assert!(
            opp.economics.net_profit_bps > Decimal::ZERO,
            "net_profit_bps should be positive; got {}",
            opp.economics.net_profit_bps
        );
    }

    /// Wide-spread book consumes the alpha — strategy should refuse to trade.
    #[tokio::test]
    async fn wide_spread_rejects_trade() {
        let s = make_strategy();
        // half_spread = 500 → 100 bps on 50k mid — exceeds 50 bps alpha at
        // full signal, even before fees.
        let books = make_book(dec!(49500), dec!(50500));
        let signals = make_signals(dec!(0.9), dec!(0.9), dec!(0.9));
        let result = s
            .evaluate(&books, &HashMap::new(), Utc::now(), &signals)
            .await;
        assert!(
            result.is_none(),
            "strategy must skip when spread > alpha — got an opportunity"
        );
    }

    /// max_quantity below 0.001 must NOT be lifted to 0.001 (no silent floor).
    #[tokio::test]
    async fn small_max_quantity_is_respected() {
        let mut s = make_strategy();
        s.max_quantity = dec!(0.0005);
        let books = make_book(dec!(50000), dec!(50010));
        let signals = make_signals(dec!(0.9), dec!(0.9), dec!(0.9));
        let opp = s
            .evaluate(&books, &HashMap::new(), Utc::now(), &signals)
            .await
            .unwrap();
        assert!(
            opp.legs[0].quantity <= s.max_quantity,
            "qty {} should not exceed max_quantity {}",
            opp.legs[0].quantity,
            s.max_quantity
        );
    }

    /// Same now + same quotes + same signals → same opportunity ID
    /// (replay determinism — see #216).
    #[tokio::test]
    async fn opportunity_id_is_deterministic() {
        let s = make_strategy();
        let books = make_book(dec!(50000), dec!(50010));
        let signals = make_signals(dec!(0.9), dec!(0.9), dec!(0.9));
        let now = Utc::now();
        let a = s
            .evaluate(&books, &HashMap::new(), now, &signals)
            .await
            .unwrap();
        let b = s
            .evaluate(&books, &HashMap::new(), now, &signals)
            .await
            .unwrap();
        assert_eq!(a.id, b.id, "same inputs should produce same opportunity ID");
    }

    #[tokio::test]
    async fn re_verify_rejects_widened_spread() {
        let s = make_strategy();
        let books_tight = make_book(dec!(50000), dec!(50010));
        let signals = make_signals(dec!(0.9), dec!(0.9), dec!(0.9));
        let opp = s
            .evaluate(&books_tight, &HashMap::new(), Utc::now(), &signals)
            .await
            .unwrap();
        // Spread blew out before submit (half-spread 100bps > 50bps alpha).
        let books_wide = make_book(dec!(49500), dec!(50500));
        let result = s.re_verify(&opp, &books_wide);
        assert!(
            result.is_none(),
            "re_verify must skip when spread now exceeds alpha"
        );
    }
}
