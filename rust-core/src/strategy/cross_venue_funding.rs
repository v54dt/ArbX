use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use smallvec::smallvec;

use crate::models::enums::{OrderType, Side, TimeInForce, Venue};
use crate::models::fee::FeeSchedule;
use crate::models::instrument::Instrument;
use crate::models::market::{BookMap, book_key};
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;

use super::base::ArbitrageStrategy;
use super::{Economics, Leg, Opportunity, OpportunityKind, OpportunityMeta};

const HOURS_PER_YEAR: i64 = 8760;
const BPS_MULTIPLIER: i64 = 10_000;

/// Cross-venue perp-perp funding arbitrage. Compares the same instrument's
/// perpetual price across two venues; when the price premium (proxy for
/// funding rate differential) exceeds `min_funding_diff_bps`, goes short on
/// the expensive venue (collecting higher funding) and long on the cheaper
/// one (paying lower funding). Delta-neutral by construction.
///
/// This uses the inter-venue price premium as a funding-rate signal. A future
/// enhancement will inject actual funding-rate snapshots via REST polling so
/// the trigger is the realized rate, not the implied one.
pub struct CrossVenueFundingStrategy {
    pub venue_a: Venue,
    pub venue_b: Venue,
    pub instrument_a: Instrument,
    pub instrument_b: Instrument,
    pub min_funding_diff_bps: Decimal,
    pub max_quantity: Decimal,
    pub fee_a: FeeSchedule,
    pub fee_b: FeeSchedule,
    pub max_quote_age_ms: i64,
    pub funding_interval_hours: i64,
    pub tick_size_a: Decimal,
    pub tick_size_b: Decimal,
    pub lot_size_a: Decimal,
    pub lot_size_b: Decimal,
    /// Number of funding intervals the position is expected to be held.
    /// Funding arb is a carry trade — entry fees amortise over many intervals.
    /// Default 21 (~7 days at 8h intervals).
    pub holding_intervals: u32,
}

impl CrossVenueFundingStrategy {
    fn implied_funding_diff(mid_a: Decimal, mid_b: Decimal) -> Decimal {
        if mid_b.is_zero() {
            return Decimal::ZERO;
        }
        (mid_a - mid_b) / mid_b
    }

    fn annualize_bps(rate: Decimal, interval_hours: i64) -> Decimal {
        rate * Decimal::from(HOURS_PER_YEAR) / Decimal::from(interval_hours)
            * Decimal::from(BPS_MULTIPLIER)
    }

    fn align_to_lot(qty: Decimal, lot: Decimal) -> Decimal {
        if lot.is_zero() {
            return qty;
        }
        (qty / lot).floor() * lot
    }
}

#[async_trait]
impl ArbitrageStrategy for CrossVenueFundingStrategy {
    async fn evaluate(
        &self,
        books: &BookMap,
        _portfolios: &HashMap<String, PortfolioSnapshot>,
        now: DateTime<Utc>,
        _signals: &crate::engine::signal::SignalCache,
    ) -> Option<Opportunity> {
        let book_a = books.get(&book_key(self.venue_a, &self.instrument_a))?;
        let book_b = books.get(&book_key(self.venue_b, &self.instrument_b))?;

        let max_age = Duration::milliseconds(self.max_quote_age_ms);
        if now - book_a.local_timestamp > max_age || now - book_b.local_timestamp > max_age {
            return None;
        }

        let mid_a = book_a.mid_price()?;
        let mid_b = book_b.mid_price()?;

        let diff = Self::implied_funding_diff(mid_a, mid_b);
        let ann_bps = Self::annualize_bps(diff, self.funding_interval_hours);

        // Positive ann_bps means venue_a is expensive → short A, long B.
        // Negative means venue_b is expensive → short B, long A.
        let (side_a, side_b) = if ann_bps > self.min_funding_diff_bps {
            (Side::Sell, Side::Buy)
        } else if ann_bps < -self.min_funding_diff_bps {
            (Side::Buy, Side::Sell)
        } else {
            return None;
        };

        let avail_a = match side_a {
            Side::Buy => book_a.best_ask()?.size,
            Side::Sell => book_a.best_bid()?.size,
        };
        let avail_b = match side_b {
            Side::Buy => book_b.best_ask()?.size,
            Side::Sell => book_b.best_bid()?.size,
        };
        let raw_qty = self.max_quantity.min(avail_a).min(avail_b);
        let quantity = Self::align_to_lot(raw_qty, self.lot_size_a.max(self.lot_size_b));
        if quantity <= Decimal::ZERO {
            return None;
        }

        let price_a = match side_a {
            Side::Buy => book_a.best_ask()?.price,
            Side::Sell => book_a.best_bid()?.price,
        };
        let price_b = match side_b {
            Side::Buy => book_b.best_ask()?.price,
            Side::Sell => book_b.best_bid()?.price,
        };

        let notional = ((price_a + price_b) / Decimal::from(2)) * quantity;
        let abs_diff = if diff < Decimal::ZERO { -diff } else { diff };
        let intervals = Decimal::from(self.holding_intervals.max(1));
        let expected_funding =
            abs_diff * notional * intervals * Decimal::from(self.funding_interval_hours)
                / Decimal::from(HOURS_PER_YEAR);

        let fee_a_cost = price_a * quantity * self.fee_a.taker();
        let fee_b_cost = price_b * quantity * self.fee_b.taker();
        let fees_total = fee_a_cost + fee_b_cost;
        let net_profit = expected_funding - fees_total;

        if net_profit <= Decimal::ZERO {
            return None;
        }

        let net_profit_bps = if notional > Decimal::ZERO {
            (net_profit / notional) * Decimal::from(BPS_MULTIPLIER)
        } else {
            Decimal::ZERO
        };

        let legs = smallvec![
            Leg {
                venue: self.venue_a,
                instrument: self.instrument_a.clone(),
                side: side_a,
                quote_price: price_a,
                order_price: price_a,
                quantity,
                fee_estimate: fee_a_cost,
            },
            Leg {
                venue: self.venue_b,
                instrument: self.instrument_b.clone(),
                side: side_b,
                quote_price: price_b,
                order_price: price_b,
                quantity,
                fee_estimate: fee_b_cost,
            },
        ];

        Some(Opportunity {
            id: format!("{}-{}", self.name(), now.timestamp_nanos_opt().unwrap_or(0)),
            kind: OpportunityKind::FundingArb {
                annualized_diff_bps: ann_bps,
            },
            legs,
            economics: Economics {
                gross_profit: expected_funding,
                fees_total,
                net_profit,
                net_profit_bps,
                notional,
            },
            meta: OpportunityMeta {
                detected_at: now,
                quote_ts_per_leg: smallvec![book_a.timestamp, book_b.timestamp],
                ttl: Duration::milliseconds(500),
                strategy_id: self.name().to_string(),
            },
        })
    }

    fn compute_hedge_orders(&self, opp: &Opportunity) -> Vec<OrderRequest> {
        opp.legs
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
        let book_a = books.get(&book_key(self.venue_a, &self.instrument_a))?;
        let book_b = books.get(&book_key(self.venue_b, &self.instrument_b))?;
        let mid_a = book_a.mid_price()?;
        let mid_b = book_b.mid_price()?;
        let current_diff_bps = if mid_b > Decimal::ZERO {
            ((mid_a - mid_b) / mid_b).abs() * Decimal::from(10_000)
        } else {
            Decimal::ZERO
        };
        if current_diff_bps < self.min_funding_diff_bps {
            return None;
        }
        Some(opp.clone())
    }

    fn name(&self) -> &str {
        "cross_venue_funding"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::signal::SignalCache;
    use crate::models::instrument::{AssetClass, InstrumentType};
    use crate::models::market::{OrderBook, OrderBookLevel};
    use rust_decimal_macros::dec;

    fn perp_instrument() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Swap,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn make_book(venue: Venue, bid: Decimal, ask: Decimal) -> OrderBook {
        OrderBook {
            venue,
            instrument: perp_instrument(),
            bids: smallvec![OrderBookLevel {
                price: bid,
                size: dec!(10),
            }],
            asks: smallvec![OrderBookLevel {
                price: ask,
                size: dec!(10),
            }],
            timestamp: Utc::now(),
            local_timestamp: Utc::now(),
        }
    }

    fn build_strategy() -> CrossVenueFundingStrategy {
        CrossVenueFundingStrategy {
            venue_a: Venue::Binance,
            venue_b: Venue::Bybit,
            instrument_a: perp_instrument(),
            instrument_b: perp_instrument(),
            min_funding_diff_bps: dec!(1),
            max_quantity: dec!(1),
            fee_a: FeeSchedule::new(Venue::Binance, dec!(0.0001), dec!(0.0001)),
            fee_b: FeeSchedule::new(Venue::Bybit, dec!(0.0001), dec!(0.0001)),
            max_quote_age_ms: 5000,
            funding_interval_hours: 8,
            tick_size_a: dec!(0.01),
            tick_size_b: dec!(0.01),
            lot_size_a: dec!(0.001),
            lot_size_b: dec!(0.001),
            holding_intervals: 21,
        }
    }

    #[tokio::test]
    async fn detects_opportunity_when_a_premium() {
        let strat = build_strategy();
        let mut books = BookMap::default();
        let key_a = book_key(Venue::Binance, &perp_instrument());
        let key_b = book_key(Venue::Bybit, &perp_instrument());
        // ~4% premium on venue A — wide enough that 21-interval carry covers fees.
        books.insert(key_a, make_book(Venue::Binance, dec!(52000), dec!(52100)));
        books.insert(key_b, make_book(Venue::Bybit, dec!(50000), dec!(50100)));

        let opp = strat
            .evaluate(&books, &HashMap::new(), Utc::now(), &SignalCache::new())
            .await;
        assert!(opp.is_some(), "should detect when A is premium");
        let opp = opp.unwrap();
        assert_eq!(opp.legs[0].side, Side::Sell);
        assert_eq!(opp.legs[1].side, Side::Buy);
    }

    #[tokio::test]
    async fn detects_opportunity_when_b_premium() {
        let strat = build_strategy();
        let mut books = BookMap::default();
        let key_a = book_key(Venue::Binance, &perp_instrument());
        let key_b = book_key(Venue::Bybit, &perp_instrument());
        books.insert(key_a, make_book(Venue::Binance, dec!(50000), dec!(50100)));
        books.insert(key_b, make_book(Venue::Bybit, dec!(52000), dec!(52100)));

        let opp = strat
            .evaluate(&books, &HashMap::new(), Utc::now(), &SignalCache::new())
            .await;
        assert!(opp.is_some(), "should detect when B is premium");
        let opp = opp.unwrap();
        assert_eq!(opp.legs[0].side, Side::Buy);
        assert_eq!(opp.legs[1].side, Side::Sell);
    }

    #[tokio::test]
    async fn no_opportunity_when_spread_too_small() {
        let strat = build_strategy();
        let mut books = BookMap::default();
        let key_a = book_key(Venue::Binance, &perp_instrument());
        let key_b = book_key(Venue::Bybit, &perp_instrument());
        books.insert(key_a, make_book(Venue::Binance, dec!(50000), dec!(50001)));
        books.insert(key_b, make_book(Venue::Bybit, dec!(50000), dec!(50001)));

        let opp = strat
            .evaluate(&books, &HashMap::new(), Utc::now(), &SignalCache::new())
            .await;
        assert!(opp.is_none(), "no opportunity when prices are near-equal");
    }

    #[test]
    fn annualize_bps_matches_expected() {
        let rate = dec!(0.001); // 0.1% per interval
        let bps = CrossVenueFundingStrategy::annualize_bps(rate, 8);
        // 0.001 * 8760/8 * 10000 = 10950
        assert_eq!(bps, dec!(10950));
    }

    #[test]
    fn align_to_lot_rounds_down() {
        assert_eq!(
            CrossVenueFundingStrategy::align_to_lot(dec!(1.2345), dec!(0.001)),
            dec!(1.234)
        );
        assert_eq!(
            CrossVenueFundingStrategy::align_to_lot(dec!(0.999), dec!(0.001)),
            dec!(0.999)
        );
    }
}
