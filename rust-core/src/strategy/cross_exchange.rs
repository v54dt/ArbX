use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use smallvec::smallvec;

use crate::models::enums::{OrderType, Side, TimeInForce, Venue};
use crate::models::fee::FeeSchedule;
use crate::models::instrument::Instrument;
use crate::models::market::{OrderBook, book_key};
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;

use super::base::ArbitrageStrategy;
use super::{Economics, Leg, Opportunity, OpportunityKind, OpportunityMeta};

/// Cross-exchange spot arbitrage between two venues.
/// TODO: walk-the-book VWAP, staleness rejection
pub struct CrossExchangeStrategy {
    pub venue_a: Venue,
    pub venue_b: Venue,
    pub instrument_a: Instrument,
    pub instrument_b: Instrument,
    pub min_net_profit_bps: Decimal,
    pub max_quantity: Decimal,
    pub fee_a: FeeSchedule,
    pub fee_b: FeeSchedule,
}

struct DirectionParams<'a> {
    buy_venue: Venue,
    buy_instrument: &'a Instrument,
    sell_venue: Venue,
    sell_instrument: &'a Instrument,
    buy_fee: Decimal,
    sell_fee: Decimal,
}

impl CrossExchangeStrategy {
    /// Returns None if no profitable trade exists in this direction.
    fn evaluate_direction(
        &self,
        books: &HashMap<String, OrderBook>,
        params: DirectionParams<'_>,
    ) -> Option<Opportunity> {
        let DirectionParams {
            buy_venue,
            buy_instrument,
            sell_venue,
            sell_instrument,
            buy_fee,
            sell_fee,
        } = params;
        let buy_book = books.get(&book_key(buy_venue, buy_instrument))?;
        let sell_book = books.get(&book_key(sell_venue, sell_instrument))?;

        let best_ask = buy_book.best_ask()?;
        let best_bid = sell_book.best_bid()?;

        if best_bid.price <= best_ask.price {
            return None;
        }

        let quantity = best_ask.size.min(best_bid.size).min(self.max_quantity);
        if quantity <= Decimal::ZERO {
            return None;
        }

        let gross_profit = (best_bid.price - best_ask.price) * quantity;

        let fee_buy = best_ask.price * quantity * buy_fee;
        let fee_sell = best_bid.price * quantity * sell_fee;
        let fees_total = fee_buy + fee_sell;

        let net_profit = gross_profit - fees_total;
        if net_profit <= Decimal::ZERO {
            return None;
        }

        let notional = best_ask.price * quantity;
        let net_profit_bps = (net_profit / notional) * Decimal::from(10_000);

        if net_profit_bps < self.min_net_profit_bps {
            return None;
        }

        let now = Utc::now();
        let legs = smallvec![
            Leg {
                venue: buy_venue,
                instrument: buy_instrument.clone(),
                side: Side::Buy,
                quote_price: best_ask.price,
                order_price: best_ask.price,
                quantity,
                fee_estimate: fee_buy,
            },
            Leg {
                venue: sell_venue,
                instrument: sell_instrument.clone(),
                side: Side::Sell,
                quote_price: best_bid.price,
                order_price: best_bid.price,
                quantity,
                fee_estimate: fee_sell,
            },
        ];

        Some(Opportunity {
            id: format!("{}-{}", self.name(), now.timestamp_nanos_opt().unwrap_or(0)),
            kind: OpportunityKind::CrossExchange,
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
                quote_ts_per_leg: smallvec![buy_book.timestamp, sell_book.timestamp],
                ttl: Duration::milliseconds(50),
                strategy_id: self.name().to_string(),
            },
        })
    }
}

#[async_trait]
impl ArbitrageStrategy for CrossExchangeStrategy {
    async fn evaluate(
        &self,
        books: &HashMap<String, OrderBook>,
        _portfolios: &HashMap<String, PortfolioSnapshot>,
    ) -> Option<Opportunity> {
        let a_to_b = self.evaluate_direction(
            books,
            DirectionParams {
                buy_venue: self.venue_a,
                buy_instrument: &self.instrument_a,
                sell_venue: self.venue_b,
                sell_instrument: &self.instrument_b,
                buy_fee: self.fee_a.taker(),
                sell_fee: self.fee_b.taker(),
            },
        );
        let b_to_a = self.evaluate_direction(
            books,
            DirectionParams {
                buy_venue: self.venue_b,
                buy_instrument: &self.instrument_b,
                sell_venue: self.venue_a,
                sell_instrument: &self.instrument_a,
                buy_fee: self.fee_b.taker(),
                sell_fee: self.fee_a.taker(),
            },
        );

        match (a_to_b, b_to_a) {
            (Some(x), Some(y)) => {
                if x.economics.net_profit >= y.economics.net_profit {
                    Some(x)
                } else {
                    Some(y)
                }
            }
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        }
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
            })
            .collect()
    }

    fn name(&self) -> &str {
        "cross_exchange"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::{Side, Venue};
    use crate::models::fee::FeeSchedule;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use crate::models::market::{OrderBook, OrderBookLevel, book_key};
    use crate::models::position::PortfolioSnapshot;
    use crate::strategy::base::ArbitrageStrategy;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn test_instrument(inst_type: InstrumentType) -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: inst_type,
            base: "BTC".to_string(),
            quote: "USDT".to_string(),
            settle_currency: None,
            expiry: None,
        }
    }

    fn fee(maker: Decimal, taker: Decimal) -> FeeSchedule {
        FeeSchedule::new(Venue::Binance, maker, taker)
    }

    fn strategy() -> CrossExchangeStrategy {
        CrossExchangeStrategy {
            venue_a: Venue::Binance,
            venue_b: Venue::Bybit,
            instrument_a: test_instrument(InstrumentType::Spot),
            instrument_b: test_instrument(InstrumentType::Spot),
            min_net_profit_bps: dec!(1),
            max_quantity: dec!(1),
            fee_a: fee(dec!(0.001), dec!(0.001)),
            fee_b: fee(dec!(0.001), dec!(0.001)),
        }
    }

    fn orderbook(
        venue: Venue,
        instrument: &Instrument,
        bid: Decimal,
        bid_size: Decimal,
        ask: Decimal,
        ask_size: Decimal,
    ) -> OrderBook {
        let now = Utc::now();
        OrderBook {
            venue,
            instrument: instrument.clone(),
            bids: vec![OrderBookLevel {
                price: bid,
                size: bid_size,
            }],
            asks: vec![OrderBookLevel {
                price: ask,
                size: ask_size,
            }],
            timestamp: now,
            local_timestamp: now,
        }
    }

    fn empty_portfolios() -> HashMap<String, PortfolioSnapshot> {
        HashMap::new()
    }

    fn make_books(books: Vec<OrderBook>) -> HashMap<String, OrderBook> {
        books
            .into_iter()
            .map(|b| (book_key(b.venue, &b.instrument), b))
            .collect()
    }

    // No opportunity when spreads are flat in both directions.
    #[tokio::test]
    async fn no_opportunity_when_no_spread() {
        let s = strategy();
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(100),
                dec!(1),
                dec!(101),
                dec!(1),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(100),
                dec!(1),
                dec!(101),
                dec!(1),
            ),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    // No opportunity when best bid < best ask in both directions.
    #[tokio::test]
    async fn no_opportunity_when_spread_negative() {
        let s = strategy();
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(102),
                dec!(1),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(99),
                dec!(1),
                dec!(102),
                dec!(1),
            ),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    // Buy cheap on A, sell expensive on B.
    #[tokio::test]
    async fn detects_opportunity_a_to_b() {
        let s = strategy();
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(102),
                dec!(1),
                dec!(103),
                dec!(1),
            ),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs.len(), 2);
        assert_eq!(opp.legs[0].venue, Venue::Binance);
        assert_eq!(opp.legs[0].side, Side::Buy);
        assert_eq!(opp.legs[1].venue, Venue::Bybit);
        assert_eq!(opp.legs[1].side, Side::Sell);
    }

    // Buy cheap on B, sell expensive on A.
    #[tokio::test]
    async fn detects_opportunity_b_to_a() {
        let s = strategy();
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(102),
                dec!(1),
                dec!(103),
                dec!(1),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].venue, Venue::Bybit);
        assert_eq!(opp.legs[0].side, Side::Buy);
        assert_eq!(opp.legs[1].venue, Venue::Binance);
        assert_eq!(opp.legs[1].side, Side::Sell);
    }

    // When both directions are profitable, return the more profitable one.
    #[tokio::test]
    async fn picks_more_profitable_direction() {
        let s = strategy();
        // a_to_b: buy at 100, sell at 102 => gross 2
        // b_to_a: buy at 101, sell at 104 => gross 3
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(104),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(102),
                dec!(1),
                dec!(101),
                dec!(1),
            ),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        // b_to_a is more profitable: buy on Bybit at 101, sell on Binance at 104
        assert_eq!(opp.legs[0].venue, Venue::Bybit);
        assert_eq!(opp.legs[0].quote_price, dec!(101));
        assert_eq!(opp.legs[1].venue, Venue::Binance);
        assert_eq!(opp.legs[1].quote_price, dec!(104));
    }

    // Spread exists but net_profit_bps < min after fees.
    #[tokio::test]
    async fn respects_min_net_profit_bps() {
        let mut s = strategy();
        s.min_net_profit_bps = dec!(100); // require 100 bps = 1%
        // Spread: buy at 100, sell at 100.5 => 0.5% gross, minus fees => below 1%
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(100.5),
                dec!(1),
                dec!(101),
                dec!(1),
            ),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    // Quantity capped at max_quantity even when more size is available.
    #[tokio::test]
    async fn respects_max_quantity() {
        let mut s = strategy();
        s.max_quantity = dec!(1);
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(10),
                dec!(100),
                dec!(10),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(102),
                dec!(10),
                dec!(103),
                dec!(10),
            ),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].quantity, dec!(1));
    }

    // Quantity is min of bid_size, ask_size, and max_quantity.
    #[tokio::test]
    async fn quantity_is_min_of_available_sizes() {
        let mut s = strategy();
        s.max_quantity = dec!(10);
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(5),
                dec!(100),
                dec!(5),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(102),
                dec!(2),
                dec!(103),
                dec!(2),
            ),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        // ask_size on A = 5, bid_size on B = 2, max_qty = 10 => quantity = 2
        assert_eq!(opp.legs[0].quantity, dec!(2));
    }

    // Verify fees_total = (ask_price * qty * taker_fee_a) + (bid_price * qty * taker_fee_b).
    #[tokio::test]
    async fn fees_are_calculated_correctly() {
        let mut s = strategy();
        s.fee_a = fee(dec!(0.0005), dec!(0.001));
        s.fee_b = fee(dec!(0.0005), dec!(0.002));
        s.max_quantity = dec!(1);
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(105),
                dec!(1),
                dec!(106),
                dec!(1),
            ),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        // fee_buy = 100 * 1 * 0.001 = 0.1, fee_sell = 105 * 1 * 0.002 = 0.21
        let expected_fees = dec!(100) * dec!(1) * dec!(0.001) + dec!(105) * dec!(1) * dec!(0.002);
        assert_eq!(opp.economics.fees_total, expected_fees);
    }

    // Zero quantity on either side means no opportunity.
    #[tokio::test]
    async fn zero_quantity_returns_none() {
        let s = strategy();
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(0),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(102),
                dec!(0),
                dec!(103),
                dec!(1),
            ),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }
}
