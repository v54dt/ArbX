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
pub const DEFAULT_FUNDING_INTERVAL_HOURS: i64 = 8;
const BPS_MULTIPLIER: i64 = 10_000;

pub struct FundingRateStrategy {
    pub venue: Venue,
    pub instrument_perp: Instrument,
    pub instrument_spot: Instrument,
    pub min_funding_rate_bps: Decimal,
    pub max_quantity: Decimal,
    pub fee_perp: FeeSchedule,
    pub fee_spot: FeeSchedule,
    pub max_quote_age_ms: i64,
    /// Funding interval in hours. Defaults to 8 (Binance / Bybit / OKX standard),
    /// but some symbols use 4 — drive from config.
    pub funding_interval_hours: i64,
}

impl FundingRateStrategy {
    fn estimate_funding_rate(perp_mid: Decimal, spot_mid: Decimal) -> Decimal {
        if spot_mid.is_zero() {
            return Decimal::ZERO;
        }
        (perp_mid - spot_mid) / spot_mid
    }

    fn annualize_bps(rate: Decimal, funding_interval_hours: i64) -> Decimal {
        rate * Decimal::from(HOURS_PER_YEAR) / Decimal::from(funding_interval_hours)
            * Decimal::from(BPS_MULTIPLIER)
    }
}

#[async_trait]
impl ArbitrageStrategy for FundingRateStrategy {
    async fn evaluate(
        &self,
        books: &BookMap,
        _portfolios: &HashMap<String, PortfolioSnapshot>,
        now: DateTime<Utc>,
    ) -> Option<Opportunity> {
        let perp_book = books.get(&book_key(self.venue, &self.instrument_perp))?;
        let spot_book = books.get(&book_key(self.venue, &self.instrument_spot))?;

        let max_age = Duration::milliseconds(self.max_quote_age_ms);
        if now - perp_book.local_timestamp > max_age || now - spot_book.local_timestamp > max_age {
            return None;
        }

        let perp_mid = perp_book.mid_price()?;
        let spot_mid = spot_book.mid_price()?;

        let funding_rate = Self::estimate_funding_rate(perp_mid, spot_mid);
        let annualized_bps = Self::annualize_bps(funding_rate, self.funding_interval_hours);

        let (perp_side, spot_side) = if annualized_bps > self.min_funding_rate_bps {
            (Side::Sell, Side::Buy)
        } else if annualized_bps < -self.min_funding_rate_bps {
            (Side::Buy, Side::Sell)
        } else {
            return None;
        };

        let perp_available = match perp_side {
            Side::Buy => perp_book.best_ask()?.size,
            Side::Sell => perp_book.best_bid()?.size,
        };
        let spot_available = match spot_side {
            Side::Buy => spot_book.best_ask()?.size,
            Side::Sell => spot_book.best_bid()?.size,
        };
        let quantity = self.max_quantity.min(perp_available).min(spot_available);
        if quantity <= Decimal::ZERO {
            return None;
        }

        let perp_price = match perp_side {
            Side::Buy => perp_book.best_ask()?.price,
            Side::Sell => perp_book.best_bid()?.price,
        };
        let spot_price = match spot_side {
            Side::Buy => spot_book.best_ask()?.price,
            Side::Sell => spot_book.best_bid()?.price,
        };

        let notional = spot_price * quantity;
        let abs_rate = if funding_rate < Decimal::ZERO {
            -funding_rate
        } else {
            funding_rate
        };
        let expected_funding = abs_rate * notional * Decimal::from(self.funding_interval_hours)
            / Decimal::from(HOURS_PER_YEAR);

        let fee_perp_cost = perp_price * quantity * self.fee_perp.taker();
        let fee_spot_cost = spot_price * quantity * self.fee_spot.taker();
        let fees_total = fee_perp_cost + fee_spot_cost;
        let net_profit = expected_funding - fees_total;

        if net_profit <= Decimal::ZERO {
            return None;
        }

        let net_profit_bps = if notional > Decimal::ZERO {
            (net_profit / notional) * Decimal::from(BPS_MULTIPLIER)
        } else {
            Decimal::ZERO
        };

        let basis_bps = (perp_mid - spot_mid) / spot_mid * Decimal::from(BPS_MULTIPLIER);

        let legs = smallvec![
            Leg {
                venue: self.venue,
                instrument: self.instrument_perp.clone(),
                side: perp_side,
                quote_price: perp_price,
                order_price: perp_price,
                quantity,
                fee_estimate: fee_perp_cost,
            },
            Leg {
                venue: self.venue,
                instrument: self.instrument_spot.clone(),
                side: spot_side,
                quote_price: spot_price,
                order_price: spot_price,
                quantity,
                fee_estimate: fee_spot_cost,
            },
        ];

        Some(Opportunity {
            id: format!("{}-{}", self.name(), now.timestamp_nanos_opt().unwrap_or(0)),
            kind: OpportunityKind::SpotFuturesBasis {
                basis_bps,
                days_to_expiry: 0,
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
                quote_ts_per_leg: smallvec![perp_book.timestamp, spot_book.timestamp],
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

    fn name(&self) -> &str {
        "funding_rate"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::Venue;
    use crate::models::fee::FeeSchedule;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use crate::models::market::{BookMap, OrderBook, OrderBookLevel, book_key};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn perp_instrument() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Swap,
            base: "BTC".to_string(),
            quote: "USDT".to_string(),
            settle_currency: Some("USDT".to_string()),
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn spot_instrument() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".to_string(),
            quote: "USDT".to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn make_strategy(min_bps: Decimal, max_qty: Decimal) -> FundingRateStrategy {
        FundingRateStrategy {
            venue: Venue::Binance,
            instrument_perp: perp_instrument(),
            instrument_spot: spot_instrument(),
            min_funding_rate_bps: min_bps,
            max_quantity: max_qty,
            fee_perp: FeeSchedule::new(Venue::Binance, dec!(0.00001), dec!(0.00001)),
            fee_spot: FeeSchedule::new(Venue::Binance, dec!(0.00001), dec!(0.00001)),
            max_quote_age_ms: 5000,
            funding_interval_hours: DEFAULT_FUNDING_INTERVAL_HOURS,
        }
    }

    fn book(instrument: &Instrument, bid: Decimal, ask: Decimal, size: Decimal) -> OrderBook {
        let now = Utc::now();
        OrderBook {
            venue: Venue::Binance,
            instrument: instrument.clone(),
            bids: smallvec::smallvec![OrderBookLevel { price: bid, size }],
            asks: smallvec::smallvec![OrderBookLevel { price: ask, size }],
            timestamp: now,
            local_timestamp: now,
        }
    }

    fn make_books(perp: OrderBook, spot: OrderBook) -> BookMap {
        let mut m = BookMap::default();
        m.insert(book_key(perp.venue, &perp.instrument), perp);
        m.insert(book_key(spot.venue, &spot.instrument), spot);
        m
    }

    fn empty_portfolios() -> HashMap<String, PortfolioSnapshot> {
        HashMap::new()
    }

    #[tokio::test]
    async fn no_opportunity_when_funding_rate_below_threshold() {
        let s = make_strategy(dec!(10000), dec!(1));
        let perp = book(&perp_instrument(), dec!(50010), dec!(50011), dec!(10));
        let spot = book(&spot_instrument(), dec!(50000), dec!(50001), dec!(10));
        let books = make_books(perp, spot);
        assert!(
            s.evaluate(&books, &empty_portfolios(), Utc::now())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn detects_positive_funding_opportunity() {
        let s = make_strategy(dec!(10), dec!(1));
        let perp = book(&perp_instrument(), dec!(52500), dec!(52501), dec!(10));
        let spot = book(&spot_instrument(), dec!(50000), dec!(50001), dec!(10));
        let books = make_books(perp, spot);
        let opp = s
            .evaluate(&books, &empty_portfolios(), Utc::now())
            .await
            .unwrap();
        assert_eq!(opp.legs[0].side, Side::Sell);
        assert_eq!(opp.legs[1].side, Side::Buy);
        assert!(opp.economics.net_profit > Decimal::ZERO);
    }

    #[tokio::test]
    async fn detects_negative_funding_opportunity() {
        let s = make_strategy(dec!(10), dec!(1));
        let perp = book(&perp_instrument(), dec!(47500), dec!(47501), dec!(10));
        let spot = book(&spot_instrument(), dec!(50000), dec!(50001), dec!(10));
        let books = make_books(perp, spot);
        let opp = s
            .evaluate(&books, &empty_portfolios(), Utc::now())
            .await
            .unwrap();
        assert_eq!(opp.legs[0].side, Side::Buy);
        assert_eq!(opp.legs[1].side, Side::Sell);
        assert!(opp.economics.net_profit > Decimal::ZERO);
    }

    #[tokio::test]
    async fn respects_max_quantity() {
        let s = make_strategy(dec!(10), dec!(0.5));
        let perp = book(&perp_instrument(), dec!(52500), dec!(52501), dec!(10));
        let spot = book(&spot_instrument(), dec!(50000), dec!(50001), dec!(10));
        let books = make_books(perp, spot);
        let opp = s
            .evaluate(&books, &empty_portfolios(), Utc::now())
            .await
            .unwrap();
        assert_eq!(opp.legs[0].quantity, dec!(0.5));
        assert_eq!(opp.legs[1].quantity, dec!(0.5));
    }

    #[tokio::test]
    async fn stale_books_return_none() {
        let s = make_strategy(dec!(10), dec!(1));
        let mut perp = book(&perp_instrument(), dec!(52500), dec!(52501), dec!(10));
        perp.local_timestamp = Utc::now() - Duration::seconds(30);
        let spot = book(&spot_instrument(), dec!(50000), dec!(50001), dec!(10));
        let books = make_books(perp, spot);
        assert!(
            s.evaluate(&books, &empty_portfolios(), Utc::now())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn computes_correct_hedge_orders() {
        let s = make_strategy(dec!(10), dec!(1));
        let perp = book(&perp_instrument(), dec!(52500), dec!(52501), dec!(10));
        let spot = book(&spot_instrument(), dec!(50000), dec!(50001), dec!(10));
        let books = make_books(perp, spot);
        let opp = s
            .evaluate(&books, &empty_portfolios(), Utc::now())
            .await
            .unwrap();
        let orders = s.compute_hedge_orders(&opp);
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].order_type, OrderType::Limit);
        assert_eq!(orders[0].time_in_force, Some(TimeInForce::Ioc));
        assert_eq!(orders[1].order_type, OrderType::Limit);
        assert_eq!(orders[1].time_in_force, Some(TimeInForce::Ioc));
    }

    #[tokio::test]
    async fn quantity_limited_by_book_size() {
        let s = make_strategy(dec!(10), dec!(10));
        let perp = book(&perp_instrument(), dec!(52500), dec!(52501), dec!(0.3));
        let spot = book(&spot_instrument(), dec!(50000), dec!(50001), dec!(0.5));
        let books = make_books(perp, spot);
        let opp = s
            .evaluate(&books, &empty_portfolios(), Utc::now())
            .await
            .unwrap();
        assert_eq!(opp.legs[0].quantity, dec!(0.3));
    }

    #[tokio::test]
    async fn uses_spot_futures_basis_kind() {
        let s = make_strategy(dec!(10), dec!(1));
        let perp = book(&perp_instrument(), dec!(52500), dec!(52501), dec!(10));
        let spot = book(&spot_instrument(), dec!(50000), dec!(50001), dec!(10));
        let books = make_books(perp, spot);
        let opp = s
            .evaluate(&books, &empty_portfolios(), Utc::now())
            .await
            .unwrap();
        assert!(matches!(opp.kind, OpportunityKind::SpotFuturesBasis { .. }));
    }

    #[test]
    fn name_returns_funding_rate() {
        let s = make_strategy(dec!(10), dec!(1));
        assert_eq!(s.name(), "funding_rate");
    }
}
