use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use smallvec::smallvec;

use crate::models::enums::{OrderType, Side, TimeInForce, Venue};
use crate::models::fee::FeeSchedule;
use crate::models::instrument::Instrument;
use crate::models::market::{BookMap, OrderBook, book_key};
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;

use super::base::ArbitrageStrategy;
use super::{Economics, Leg, Opportunity, OpportunityKind, OpportunityMeta};

pub struct CrossExchangeStrategy {
    pub venue_a: Venue,
    pub venue_b: Venue,
    pub instrument_a: Instrument,
    pub instrument_b: Instrument,
    pub min_net_profit_bps: Decimal,
    pub max_quantity: Decimal,
    pub fee_a: FeeSchedule,
    pub fee_b: FeeSchedule,
    pub max_quote_age_ms: i64,
    pub tick_size_a: Decimal,
    pub tick_size_b: Decimal,
    pub lot_size_a: Decimal,
    pub lot_size_b: Decimal,
    pub max_book_depth: usize,
}

struct DirectionParams<'a> {
    buy_venue: Venue,
    buy_instrument: &'a Instrument,
    sell_venue: Venue,
    sell_instrument: &'a Instrument,
    buy_fee: Decimal,
    sell_fee: Decimal,
    tick_size_buy: Decimal,
    tick_size_sell: Decimal,
    lot_size: Decimal,
}

fn quantize(value: Decimal, step: Decimal) -> Decimal {
    if step.is_zero() {
        return value;
    }
    (value / step).floor() * step
}

fn quantize_up(value: Decimal, step: Decimal) -> Decimal {
    if step.is_zero() {
        return value;
    }
    (value / step).ceil() * step
}

pub(crate) fn vwap_ask(
    book: &OrderBook,
    qty: Decimal,
    max_depth: usize,
) -> Option<(Decimal, Decimal)> {
    let mut remaining = qty;
    let mut total_cost = Decimal::ZERO;
    let mut filled = Decimal::ZERO;
    for level in book.asks.iter().take(max_depth) {
        let take = remaining.min(level.size);
        total_cost += take * level.price;
        filled += take;
        remaining -= take;
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    if filled > Decimal::ZERO {
        Some((total_cost / filled, filled))
    } else {
        None
    }
}

pub(crate) fn vwap_bid(
    book: &OrderBook,
    qty: Decimal,
    max_depth: usize,
) -> Option<(Decimal, Decimal)> {
    let mut remaining = qty;
    let mut total_cost = Decimal::ZERO;
    let mut filled = Decimal::ZERO;
    for level in book.bids.iter().take(max_depth) {
        let take = remaining.min(level.size);
        total_cost += take * level.price;
        filled += take;
        remaining -= take;
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    if filled > Decimal::ZERO {
        Some((total_cost / filled, filled))
    } else {
        None
    }
}

impl CrossExchangeStrategy {
    fn check_inventory(
        &self,
        portfolios: &HashMap<String, PortfolioSnapshot>,
        buy_venue: Venue,
        buy_instrument: &Instrument,
        quantity: Decimal,
    ) -> Decimal {
        let half_max = self.max_quantity * Decimal::new(5, 1);
        for snapshot in portfolios.values() {
            if snapshot.venue != buy_venue {
                continue;
            }
            for pos in &snapshot.positions {
                if pos.instrument == *buy_instrument && pos.quantity > half_max {
                    let excess = pos.quantity - half_max;
                    let reduced = quantity - excess;
                    return if reduced > Decimal::ZERO {
                        reduced
                    } else {
                        Decimal::ZERO
                    };
                }
            }
        }
        quantity
    }

    fn evaluate_direction(
        &self,
        books: &BookMap,
        portfolios: &HashMap<String, PortfolioSnapshot>,
        params: DirectionParams<'_>,
    ) -> Option<Opportunity> {
        let DirectionParams {
            buy_venue,
            buy_instrument,
            sell_venue,
            sell_instrument,
            buy_fee,
            sell_fee,
            tick_size_buy,
            tick_size_sell,
            lot_size,
        } = params;
        let buy_book = books.get(&book_key(buy_venue, buy_instrument))?;
        let sell_book = books.get(&book_key(sell_venue, sell_instrument))?;

        let now = Utc::now();
        let max_age = Duration::milliseconds(self.max_quote_age_ms);
        if now - buy_book.local_timestamp > max_age || now - sell_book.local_timestamp > max_age {
            return None;
        }

        let (vwap_ask_price, ask_fill) =
            vwap_ask(buy_book, self.max_quantity, self.max_book_depth)?;
        let (vwap_bid_price, bid_fill) =
            vwap_bid(sell_book, self.max_quantity, self.max_book_depth)?;

        if vwap_bid_price <= vwap_ask_price {
            return None;
        }

        let raw_qty = ask_fill.min(bid_fill).min(self.max_quantity);

        let quantity = quantize(
            self.check_inventory(portfolios, buy_venue, buy_instrument, raw_qty),
            lot_size,
        );
        if quantity <= Decimal::ZERO {
            return None;
        }

        let (final_ask, _) = vwap_ask(buy_book, quantity, self.max_book_depth)?;
        let (final_bid, _) = vwap_bid(sell_book, quantity, self.max_book_depth)?;

        let order_ask = quantize_up(final_ask, tick_size_buy);
        let order_bid = quantize(final_bid, tick_size_sell);

        let gross_profit = (order_bid - order_ask) * quantity;
        let fee_buy = order_ask * quantity * buy_fee;
        let fee_sell = order_bid * quantity * sell_fee;
        let fees_total = fee_buy + fee_sell;
        let net_profit = gross_profit - fees_total;

        if net_profit <= Decimal::ZERO {
            return None;
        }

        let notional = order_ask * quantity;
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
                quote_price: final_ask,
                order_price: order_ask,
                quantity,
                fee_estimate: fee_buy,
            },
            Leg {
                venue: sell_venue,
                instrument: sell_instrument.clone(),
                side: Side::Sell,
                quote_price: final_bid,
                order_price: order_bid,
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
        books: &BookMap,
        portfolios: &HashMap<String, PortfolioSnapshot>,
    ) -> Option<Opportunity> {
        let a_to_b = self.evaluate_direction(
            books,
            portfolios,
            DirectionParams {
                buy_venue: self.venue_a,
                buy_instrument: &self.instrument_a,
                sell_venue: self.venue_b,
                sell_instrument: &self.instrument_b,
                buy_fee: self.fee_a.taker(),
                sell_fee: self.fee_b.taker(),
                tick_size_buy: self.tick_size_a,
                tick_size_sell: self.tick_size_b,
                lot_size: self.lot_size_a.max(self.lot_size_b),
            },
        );
        let b_to_a = self.evaluate_direction(
            books,
            portfolios,
            DirectionParams {
                buy_venue: self.venue_b,
                buy_instrument: &self.instrument_b,
                sell_venue: self.venue_a,
                sell_instrument: &self.instrument_a,
                buy_fee: self.fee_b.taker(),
                sell_fee: self.fee_a.taker(),
                tick_size_buy: self.tick_size_b,
                tick_size_sell: self.tick_size_a,
                lot_size: self.lot_size_a.max(self.lot_size_b),
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
    use crate::models::market::{BookMap, OrderBook, OrderBookLevel, book_key};
    use crate::models::position::{PortfolioSnapshot, Position};
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
            last_trade_time: None,
            settlement_time: None,
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
            max_quote_age_ms: 5000,
            tick_size_a: dec!(0.01),
            tick_size_b: dec!(0.01),
            lot_size_a: dec!(0.001),
            lot_size_b: dec!(0.001),
            max_book_depth: 10,
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
            bids: smallvec::smallvec![OrderBookLevel {
                price: bid,
                size: bid_size,
            }],
            asks: smallvec::smallvec![OrderBookLevel {
                price: ask,
                size: ask_size,
            }],
            timestamp: now,
            local_timestamp: now,
        }
    }

    fn multi_level_book(
        venue: Venue,
        instrument: &Instrument,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    ) -> OrderBook {
        let now = Utc::now();
        OrderBook {
            venue,
            instrument: instrument.clone(),
            bids: bids
                .into_iter()
                .map(|(price, size)| OrderBookLevel { price, size })
                .collect(),
            asks: asks
                .into_iter()
                .map(|(price, size)| OrderBookLevel { price, size })
                .collect(),
            timestamp: now,
            local_timestamp: now,
        }
    }

    fn empty_portfolios() -> HashMap<String, PortfolioSnapshot> {
        HashMap::new()
    }

    fn make_books(books: Vec<OrderBook>) -> BookMap {
        books
            .into_iter()
            .map(|b| (book_key(b.venue, &b.instrument), b))
            .collect()
    }

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

    #[tokio::test]
    async fn picks_more_profitable_direction() {
        let s = strategy();
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
        assert_eq!(opp.legs[0].venue, Venue::Bybit);
        assert_eq!(opp.legs[0].quote_price, dec!(101));
        assert_eq!(opp.legs[1].venue, Venue::Binance);
        assert_eq!(opp.legs[1].quote_price, dec!(104));
    }

    #[tokio::test]
    async fn respects_min_net_profit_bps() {
        let mut s = strategy();
        s.min_net_profit_bps = dec!(100);
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
        assert_eq!(opp.legs[0].quantity, dec!(2));
    }

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
        let expected_fees = dec!(100) * dec!(1) * dec!(0.001) + dec!(105) * dec!(1) * dec!(0.002);
        assert_eq!(opp.economics.fees_total, expected_fees);
    }

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

    #[tokio::test]
    async fn stale_buy_book_returns_none() {
        let s = strategy();
        let mut buy = orderbook(
            Venue::Binance,
            &s.instrument_a,
            dec!(99),
            dec!(1),
            dec!(100),
            dec!(1),
        );
        buy.local_timestamp = Utc::now() - Duration::seconds(10);
        let sell = orderbook(
            Venue::Bybit,
            &s.instrument_b,
            dec!(102),
            dec!(1),
            dec!(103),
            dec!(1),
        );
        let books = make_books(vec![buy, sell]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn stale_sell_book_returns_none() {
        let s = strategy();
        let buy = orderbook(
            Venue::Binance,
            &s.instrument_a,
            dec!(99),
            dec!(1),
            dec!(100),
            dec!(1),
        );
        let mut sell = orderbook(
            Venue::Bybit,
            &s.instrument_b,
            dec!(102),
            dec!(1),
            dec!(103),
            dec!(1),
        );
        sell.local_timestamp = Utc::now() - Duration::seconds(10);
        let books = make_books(vec![buy, sell]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn fresh_books_still_detect_opportunity() {
        let s = strategy();
        let mut buy = orderbook(
            Venue::Binance,
            &s.instrument_a,
            dec!(99),
            dec!(1),
            dec!(100),
            dec!(1),
        );
        buy.local_timestamp = Utc::now();
        let mut sell = orderbook(
            Venue::Bybit,
            &s.instrument_b,
            dec!(102),
            dec!(1),
            dec!(103),
            dec!(1),
        );
        sell.local_timestamp = Utc::now();
        let books = make_books(vec![buy, sell]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_some());
    }

    // --- New tests ---

    #[tokio::test]
    async fn vwap_uses_multiple_levels() {
        let mut s = strategy();
        s.max_quantity = dec!(3);
        s.lot_size_a = dec!(0.001);
        s.lot_size_b = dec!(0.001);
        // 3 ask levels: 100@1, 101@1, 102@1 => VWAP = (100+101+102)/3 = 101
        let buy = multi_level_book(
            Venue::Binance,
            &s.instrument_a,
            vec![(dec!(99), dec!(5))],
            vec![
                (dec!(100), dec!(1)),
                (dec!(101), dec!(1)),
                (dec!(102), dec!(1)),
            ],
        );
        let sell = multi_level_book(
            Venue::Bybit,
            &s.instrument_b,
            vec![
                (dec!(106), dec!(1)),
                (dec!(105), dec!(1)),
                (dec!(104), dec!(1)),
            ],
            vec![(dec!(110), dec!(5))],
        );
        let books = make_books(vec![buy, sell]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].quantity, dec!(3));
        assert_eq!(opp.legs[0].quote_price, dec!(101));
    }

    #[tokio::test]
    async fn vwap_partial_fill_from_last_level() {
        let mut s = strategy();
        s.max_quantity = dec!(1.5);
        s.lot_size_a = dec!(0.001);
        s.lot_size_b = dec!(0.001);
        // asks: 100@1, 102@2 => need 1.5 => take 1@100 + 0.5@102 => VWAP = (100+51)/1.5 = 100.666...
        let buy = multi_level_book(
            Venue::Binance,
            &s.instrument_a,
            vec![(dec!(99), dec!(5))],
            vec![(dec!(100), dec!(1)), (dec!(102), dec!(2))],
        );
        let sell = multi_level_book(
            Venue::Bybit,
            &s.instrument_b,
            vec![(dec!(110), dec!(2))],
            vec![(dec!(115), dec!(5))],
        );
        let books = make_books(vec![buy, sell]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].quantity, dec!(1.5));
        // VWAP = (1*100 + 0.5*102) / 1.5 = 151/1.5
        let expected_vwap = dec!(151) / dec!(1.5);
        assert_eq!(opp.legs[0].quote_price, expected_vwap);
    }

    #[test]
    fn quantize_rounds_down() {
        assert_eq!(quantize(dec!(0.0057), dec!(0.001)), dec!(0.005));
        assert_eq!(quantize(dec!(1.999), dec!(0.01)), dec!(1.99));
        assert_eq!(quantize(dec!(5.0), dec!(1)), dec!(5));
    }

    #[tokio::test]
    async fn quantize_profit_recheck_rejects_when_rounding_kills_profit() {
        let mut s = strategy();
        s.min_net_profit_bps = dec!(1);
        s.tick_size_a = dec!(1);
        s.tick_size_b = dec!(1);
        s.lot_size_a = dec!(0.001);
        s.lot_size_b = dec!(0.001);
        // ask=100.1, bid=100.9 => tiny spread.
        // After tick quantize: ask rounds UP to 101, bid rounds DOWN to 100 => negative spread.
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100.1),
                dec!(1),
            ),
            orderbook(
                Venue::Bybit,
                &s.instrument_b,
                dec!(100.9),
                dec!(1),
                dec!(103),
                dec!(1),
            ),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn inventory_reduces_quantity_when_existing_position() {
        let mut s = strategy();
        s.max_quantity = dec!(1);
        s.lot_size_a = dec!(0.001);
        s.lot_size_b = dec!(0.001);

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

        let mut portfolios = HashMap::new();
        portfolios.insert(
            "binance".to_string(),
            PortfolioSnapshot {
                venue: Venue::Binance,
                positions: vec![Position {
                    venue: Venue::Binance,
                    instrument: s.instrument_a.clone(),
                    quantity: dec!(0.8),
                    average_cost: dec!(100),
                    unrealized_pnl: Decimal::ZERO,
                    realized_pnl: Decimal::ZERO,
                    settlement_date: None,
                }],
                total_equity: dec!(10000),
                available_balance: dec!(5000),
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
            },
        );

        let opp = s.evaluate(&books, &portfolios).await.unwrap();
        // max_quantity=1, half_max=0.5, existing=0.8 > 0.5, excess=0.3
        // raw_qty=1, reduced=1-0.3=0.7
        assert!(opp.legs[0].quantity < dec!(1));
        assert_eq!(opp.legs[0].quantity, dec!(0.7));
    }
}
