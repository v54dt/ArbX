use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use smallvec::smallvec;

use crate::models::enums::{OrderType, Side, TimeInForce, Venue};
use crate::models::fee::FeeSchedule;
use crate::models::instrument::Instrument;
use crate::models::market::{BookMap, book_key};
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;
use crate::models::tick_rules::{tw_futures_tick_size, tw_stock_tick_size};

use super::base::ArbitrageStrategy;
use super::{Economics, Leg, Opportunity, OpportunityKind, OpportunityMeta};

pub struct TwEtfFuturesStrategy {
    pub venue: Venue,
    pub etf_instrument: Instrument,
    pub futures_instrument: Instrument,
    pub hedge_ratio: Decimal,
    pub min_net_profit_bps: Decimal,
    pub max_quantity: Decimal,
    pub fee_etf: FeeSchedule,
    pub fee_futures: FeeSchedule,
    pub max_quote_age_ms: i64,
    pub cost_of_carry_bps: Decimal,
    pub days_to_expiry: i64,
}

#[async_trait]
impl ArbitrageStrategy for TwEtfFuturesStrategy {
    async fn evaluate(
        &self,
        books: &BookMap,
        _portfolios: &HashMap<String, PortfolioSnapshot>,
    ) -> Option<Opportunity> {
        let etf_book = books.get(&book_key(self.venue, &self.etf_instrument))?;
        let fut_book = books.get(&book_key(self.venue, &self.futures_instrument))?;

        let now = Utc::now();
        let max_age = Duration::milliseconds(self.max_quote_age_ms);
        if now - etf_book.local_timestamp > max_age || now - fut_book.local_timestamp > max_age {
            return None;
        }

        let etf_ask = etf_book.best_ask()?;
        let etf_bid = etf_book.best_bid()?;
        let fut_bid = fut_book.best_bid()?;
        let fut_ask = fut_book.best_ask()?;

        let etf_mid = (etf_ask.price + etf_bid.price) / Decimal::TWO;
        let carry_rate = self.cost_of_carry_bps / Decimal::from(10_000)
            * Decimal::from(self.days_to_expiry)
            / Decimal::from(365);
        let fair_value = etf_mid * (Decimal::ONE + carry_rate);
        let threshold = fair_value * self.min_net_profit_bps / Decimal::from(10_000);

        let (fut_side, fut_price, etf_side, etf_price, basis_bps) =
            if fut_bid.price > fair_value + threshold {
                let bps = (fut_bid.price - fair_value) / fair_value * Decimal::from(10_000);
                (Side::Sell, fut_bid.price, Side::Buy, etf_ask.price, bps)
            } else if fut_ask.price < fair_value - threshold {
                let bps = (fair_value - fut_ask.price) / fair_value * Decimal::from(10_000);
                (Side::Buy, fut_ask.price, Side::Sell, etf_bid.price, bps)
            } else {
                return None;
            };

        let fut_qty_available = match fut_side {
            Side::Sell => fut_bid.size,
            Side::Buy => fut_ask.size,
        };
        let fut_qty = fut_qty_available.min(self.max_quantity);
        if fut_qty <= Decimal::ZERO {
            return None;
        }

        let etf_qty = fut_qty * self.hedge_ratio;

        let carry_cost = etf_price * carry_rate;
        let gross_profit = match fut_side {
            Side::Sell => (fut_price - etf_price - carry_cost) * fut_qty,
            Side::Buy => (etf_price - carry_cost - fut_price) * fut_qty,
        };

        let fee_etf_total = etf_price * etf_qty * self.fee_etf.taker();
        let fee_fut_total = fut_price * fut_qty * self.fee_futures.taker();
        let fees_total = fee_etf_total + fee_fut_total;
        let net_profit = gross_profit - fees_total;

        if net_profit <= Decimal::ZERO {
            return None;
        }

        let notional = fut_price * fut_qty;
        let net_profit_bps = (net_profit / notional) * Decimal::from(10_000);

        if net_profit_bps < self.min_net_profit_bps {
            return None;
        }

        let days_to_expiry_u16 = self.days_to_expiry.min(u16::MAX as i64).max(0) as u16;

        let legs = smallvec![
            Leg {
                venue: self.venue,
                instrument: self.etf_instrument.clone(),
                side: etf_side,
                quote_price: etf_price,
                order_price: etf_price,
                quantity: etf_qty,
                fee_estimate: fee_etf_total,
            },
            Leg {
                venue: self.venue,
                instrument: self.futures_instrument.clone(),
                side: fut_side,
                quote_price: fut_price,
                order_price: fut_price,
                quantity: fut_qty,
                fee_estimate: fee_fut_total,
            },
        ];

        Some(Opportunity {
            id: format!("{}-{}", self.name(), now.timestamp_nanos_opt().unwrap_or(0)),
            kind: OpportunityKind::SpotFuturesBasis {
                basis_bps,
                days_to_expiry: days_to_expiry_u16,
            },
            legs,
            economics: Economics {
                gross_profit,
                fees_total,
                net_profit,
                net_profit_bps,
                notional,
            },
            meta: OpportunityMeta {
                detected_at: now,
                quote_ts_per_leg: smallvec![etf_book.timestamp, fut_book.timestamp],
                ttl: Duration::milliseconds(50),
                strategy_id: self.name().to_string(),
            },
        })
    }

    fn compute_hedge_orders(&self, opp: &Opportunity) -> Vec<OrderRequest> {
        opp.legs
            .iter()
            .map(|leg| {
                let tick = if leg.instrument == self.etf_instrument {
                    tw_stock_tick_size(leg.order_price)
                } else {
                    tw_futures_tick_size()
                };
                let aligned_price = align_to_tick(leg.order_price, tick, leg.side);
                OrderRequest {
                    venue: leg.venue,
                    instrument: leg.instrument.clone(),
                    side: leg.side,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::Ioc),
                    price: Some(aligned_price),
                    quantity: leg.quantity,
                }
            })
            .collect()
    }

    fn re_verify(&self, opp: &Opportunity, books: &BookMap) -> Option<Opportunity> {
        let book_etf = books.get(&book_key(self.venue_etf, &self.instrument_etf))?;
        let book_fut = books.get(&book_key(self.venue_futures, &self.instrument_futures))?;
        let mid_etf = book_etf.mid_price()?;
        let mid_fut = book_fut.mid_price()?;
        let basis_bps = if mid_etf > Decimal::ZERO {
            (mid_fut - mid_etf) / mid_etf * Decimal::from(10_000)
        } else {
            Decimal::ZERO
        };
        let threshold = opp.economics.net_profit_bps / Decimal::from(2);
        if basis_bps.abs() < threshold {
            return None;
        }
        Some(opp.clone())
    }

    fn name(&self) -> &str {
        "tw_etf_futures"
    }
}

/// Align to tick grid. Buy rounds UP, Sell rounds DOWN — both stay aggressive
/// (crossing). Tick=0 passes through.
fn align_to_tick(price: Decimal, tick: Decimal, side: Side) -> Decimal {
    if tick.is_zero() {
        return price;
    }
    let units = price / tick;
    let snapped = match side {
        Side::Buy => units.ceil(),
        Side::Sell => units.floor(),
    };
    snapped * tick
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::Venue;
    use crate::models::instrument::{AssetClass, InstrumentType};
    use crate::models::market::{BookMap, OrderBook, OrderBookLevel, book_key};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn etf_instrument() -> Instrument {
        Instrument {
            asset_class: AssetClass::Equity,
            instrument_type: InstrumentType::Spot,
            base: "0050".to_string(),
            quote: "TWD".to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn futures_instrument() -> Instrument {
        Instrument {
            asset_class: AssetClass::Index,
            instrument_type: InstrumentType::Futures,
            base: "TX".to_string(),
            quote: "TWD".to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn make_strategy() -> TwEtfFuturesStrategy {
        TwEtfFuturesStrategy {
            venue: Venue::Fubon,
            etf_instrument: etf_instrument(),
            futures_instrument: futures_instrument(),
            hedge_ratio: dec!(200),
            min_net_profit_bps: dec!(1),
            max_quantity: dec!(10),
            fee_etf: FeeSchedule::new(Venue::Fubon, dec!(0.0001), dec!(0.0001)),
            fee_futures: FeeSchedule::new(Venue::Fubon, dec!(0.0001), dec!(0.0001)),
            max_quote_age_ms: 5000,
            cost_of_carry_bps: dec!(200),
            days_to_expiry: 30,
        }
    }

    fn orderbook(instrument: &Instrument, bid: Decimal, ask: Decimal) -> OrderBook {
        let now = Utc::now();
        OrderBook {
            venue: Venue::Fubon,
            instrument: instrument.clone(),
            bids: smallvec::smallvec![OrderBookLevel {
                price: bid,
                size: dec!(100),
            }],
            asks: smallvec::smallvec![OrderBookLevel {
                price: ask,
                size: dec!(100),
            }],
            timestamp: now,
            local_timestamp: now,
        }
    }

    fn make_books(
        etf_bid: Decimal,
        etf_ask: Decimal,
        fut_bid: Decimal,
        fut_ask: Decimal,
    ) -> BookMap {
        let etf = etf_instrument();
        let fut = futures_instrument();
        let mut m = BookMap::default();
        m.insert(
            book_key(Venue::Fubon, &etf),
            orderbook(&etf, etf_bid, etf_ask),
        );
        m.insert(
            book_key(Venue::Fubon, &fut),
            orderbook(&fut, fut_bid, fut_ask),
        );
        m
    }

    fn empty_portfolios() -> HashMap<String, PortfolioSnapshot> {
        HashMap::new()
    }

    #[tokio::test]
    async fn no_opportunity_when_futures_at_fair_value() {
        let s = make_strategy();
        // ETF mid = 150, fair = 150 * (1 + 200/10000 * 30/365) ≈ 150.2466
        // Set futures at fair value
        let etf_mid = dec!(150);
        let fair = etf_mid * (Decimal::ONE + dec!(200) / dec!(10000) * dec!(30) / dec!(365));
        let books = make_books(dec!(149), dec!(151), fair, fair);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn detects_futures_overpriced() {
        let s = make_strategy();
        // ETF mid = 150, fair ≈ 150.2466
        // Futures bid well above fair value
        let books = make_books(dec!(149), dec!(151), dec!(155), dec!(156));
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].side, Side::Buy); // buy ETF
        assert_eq!(opp.legs[1].side, Side::Sell); // sell futures
        assert_eq!(opp.legs[0].instrument, etf_instrument());
        assert_eq!(opp.legs[1].instrument, futures_instrument());
        assert!(opp.economics.gross_profit > Decimal::ZERO);
        assert!(opp.economics.net_profit > Decimal::ZERO);
    }

    #[tokio::test]
    async fn detects_futures_underpriced() {
        let s = make_strategy();
        // ETF mid = 150, fair ≈ 150.2466
        // Futures ask well below fair value
        let books = make_books(dec!(149), dec!(151), dec!(140), dec!(141));
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].side, Side::Sell); // sell ETF
        assert_eq!(opp.legs[1].side, Side::Buy); // buy futures
        assert!(opp.economics.gross_profit > Decimal::ZERO);
        assert!(opp.economics.net_profit > Decimal::ZERO);
    }

    #[tokio::test]
    async fn respects_min_profit_bps() {
        let mut s = make_strategy();
        s.min_net_profit_bps = dec!(500);
        // Small deviation that won't meet 500 bps threshold
        let books = make_books(dec!(149), dec!(151), dec!(151), dec!(152));
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn hedge_ratio_scales_etf_quantity() {
        let s = make_strategy();
        let books = make_books(dec!(149), dec!(151), dec!(155), dec!(156));
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        let fut_qty = opp.legs[1].quantity;
        let etf_qty = opp.legs[0].quantity;
        assert_eq!(etf_qty, fut_qty * dec!(200));
    }

    #[tokio::test]
    async fn compute_hedge_orders_returns_ioc_limits() {
        let s = make_strategy();
        let books = make_books(dec!(149), dec!(151), dec!(155), dec!(156));
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        let orders = s.compute_hedge_orders(&opp);
        assert_eq!(orders.len(), 2);
        for order in &orders {
            assert_eq!(order.venue, Venue::Fubon);
            assert_eq!(order.order_type, OrderType::Limit);
            assert_eq!(order.time_in_force, Some(TimeInForce::Ioc));
        }
    }

    #[tokio::test]
    async fn stale_book_returns_none() {
        let s = make_strategy();
        let etf = etf_instrument();
        let fut = futures_instrument();
        let now = Utc::now();
        let mut etf_book = orderbook(&etf, dec!(149), dec!(151));
        etf_book.local_timestamp = now - Duration::seconds(10);
        let fut_book = orderbook(&fut, dec!(155), dec!(156));
        let mut m = BookMap::default();
        m.insert(book_key(Venue::Fubon, &etf), etf_book);
        m.insert(book_key(Venue::Fubon, &fut), fut_book);
        assert!(s.evaluate(&m, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn max_quantity_caps_futures_contracts() {
        let mut s = make_strategy();
        s.max_quantity = dec!(2);
        let books = make_books(dec!(149), dec!(151), dec!(155), dec!(156));
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert!(opp.legs[1].quantity <= dec!(2));
    }

    #[tokio::test]
    async fn opportunity_kind_is_spot_futures_basis() {
        let s = make_strategy();
        let books = make_books(dec!(149), dec!(151), dec!(155), dec!(156));
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        matches!(opp.kind, OpportunityKind::SpotFuturesBasis { .. });
    }

    #[test]
    fn align_to_tick_buy_rounds_up() {
        // Buy at 150.27 with tick 0.50 → 150.50 (next tick up).
        assert_eq!(
            align_to_tick(dec!(150.27), dec!(0.50), Side::Buy),
            dec!(150.50)
        );
        // Already on tick: stays.
        assert_eq!(
            align_to_tick(dec!(150.50), dec!(0.50), Side::Buy),
            dec!(150.50)
        );
    }

    #[test]
    fn align_to_tick_sell_rounds_down() {
        // Sell at 150.27 with tick 0.50 → 150.00 (next tick down).
        assert_eq!(
            align_to_tick(dec!(150.27), dec!(0.50), Side::Sell),
            dec!(150.00)
        );
        assert_eq!(
            align_to_tick(dec!(150.50), dec!(0.50), Side::Sell),
            dec!(150.50)
        );
    }

    #[tokio::test]
    async fn compute_hedge_orders_aligns_etf_to_tw_tick() {
        // ETF tick rule: 100..500 → 0.50; futures tick = 1.0.
        let s = make_strategy();
        let books = make_books(dec!(149), dec!(151), dec!(155), dec!(156));
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        let orders = s.compute_hedge_orders(&opp);
        for order in &orders {
            let price = order.price.unwrap();
            let tick = if order.instrument == s.etf_instrument {
                tw_stock_tick_size(price)
            } else {
                tw_futures_tick_size()
            };
            // Aligned price must be a multiple of tick (within Decimal exactness).
            let units = price / tick;
            assert_eq!(
                units.fract(),
                Decimal::ZERO,
                "{:?} order price {} not aligned to tick {}",
                order.side,
                price,
                tick
            );
        }
    }
}
