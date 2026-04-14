use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use smallvec::smallvec;

use crate::models::enums::{OrderType, Side, Venue};
use crate::models::fee::FeeSchedule;
use crate::models::instrument::Instrument;
use crate::models::market::{BookMap, book_key};
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;

use super::base::ArbitrageStrategy;
use super::{Economics, Leg, Opportunity, OpportunityKind, OpportunityMeta};

pub struct TriangleLeg {
    pub instrument: Instrument,
    pub side: Side,
}

pub struct TriangleCycle {
    pub venue: Venue,
    pub leg_a: TriangleLeg,
    pub leg_b: TriangleLeg,
    pub leg_c: TriangleLeg,
    pub fee: FeeSchedule,
    pub max_notional_usdt: Decimal,
    pub min_net_profit_bps: Decimal,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
}

pub struct TriangularArbStrategy {
    pub cycles: Vec<TriangleCycle>,
    pub max_quote_age_ms: i64,
    pub max_book_depth: usize,
}

fn quantize(value: Decimal, step: Decimal) -> Decimal {
    if step.is_zero() {
        return value;
    }
    (value / step).floor() * step
}

fn tob_ask(book: &crate::models::market::OrderBook) -> Option<Decimal> {
    book.asks.first().map(|l| l.price)
}

fn tob_bid(book: &crate::models::market::OrderBook) -> Option<Decimal> {
    book.bids.first().map(|l| l.price)
}

struct CycleResult {
    final_wallet: Decimal,
    qty_a: Decimal,
    qty_b: Decimal,
    qty_c: Decimal,
    price_a: Decimal,
    price_b: Decimal,
    price_c: Decimal,
    fee_a: Decimal,
    fee_b: Decimal,
    fee_c: Decimal,
}

fn simulate_cycle(
    cycle: &TriangleCycle,
    price_a: Decimal,
    price_b: Decimal,
    price_c: Decimal,
) -> CycleResult {
    let fee_rate = cycle.fee.taker();
    let start = cycle.max_notional_usdt;

    // Leg A
    let (wallet_after_a, qty_a, fee_a) = apply_leg(&cycle.leg_a, start, price_a, fee_rate);
    // Leg B
    let (wallet_after_b, qty_b, fee_b) = apply_leg(&cycle.leg_b, wallet_after_a, price_b, fee_rate);
    // Leg C
    let (final_wallet, qty_c, fee_c) = apply_leg(&cycle.leg_c, wallet_after_b, price_c, fee_rate);

    CycleResult {
        final_wallet,
        qty_a,
        qty_b,
        qty_c,
        price_a,
        price_b,
        price_c,
        fee_a,
        fee_b,
        fee_c,
    }
}

/// Apply one leg, returning (new_wallet, traded_qty, fee_in_output_ccy).
fn apply_leg(
    leg: &TriangleLeg,
    wallet: Decimal,
    price: Decimal,
    fee_rate: Decimal,
) -> (Decimal, Decimal, Decimal) {
    match leg.side {
        Side::Buy => {
            let gross = wallet / price;
            let fee = gross * fee_rate;
            let net = gross - fee;
            (net, gross, fee)
        }
        Side::Sell => {
            let gross = wallet * price;
            let fee = gross * fee_rate;
            let net = gross - fee;
            (net, wallet, fee)
        }
    }
}

impl TriangularArbStrategy {
    fn evaluate_cycle(&self, books: &BookMap, cycle: &TriangleCycle) -> Option<Opportunity> {
        let book_a = books.get(&book_key(cycle.venue, &cycle.leg_a.instrument))?;
        let book_b = books.get(&book_key(cycle.venue, &cycle.leg_b.instrument))?;
        let book_c = books.get(&book_key(cycle.venue, &cycle.leg_c.instrument))?;

        let now = Utc::now();
        let max_age = Duration::milliseconds(self.max_quote_age_ms);
        if now - book_a.local_timestamp > max_age
            || now - book_b.local_timestamp > max_age
            || now - book_c.local_timestamp > max_age
        {
            return None;
        }

        let price_a = match cycle.leg_a.side {
            Side::Buy => tob_ask(book_a)?,
            Side::Sell => tob_bid(book_a)?,
        };
        let price_b = match cycle.leg_b.side {
            Side::Buy => tob_ask(book_b)?,
            Side::Sell => tob_bid(book_b)?,
        };
        let price_c = match cycle.leg_c.side {
            Side::Buy => tob_ask(book_c)?,
            Side::Sell => tob_bid(book_c)?,
        };

        let result = simulate_cycle(cycle, price_a, price_b, price_c);

        let notional = cycle.max_notional_usdt;
        let net_profit = result.final_wallet - notional;
        let fees_total = result.fee_a + result.fee_b + result.fee_c;
        let gross_profit = net_profit + fees_total;

        if net_profit <= Decimal::ZERO {
            return None;
        }

        let net_profit_bps = (net_profit / notional) * Decimal::from(10_000);
        if net_profit_bps < cycle.min_net_profit_bps {
            return None;
        }

        let qty_a = quantize(result.qty_a, cycle.lot_size);
        let qty_b = quantize(result.qty_b, cycle.lot_size);
        let qty_c = quantize(result.qty_c, cycle.lot_size);

        let detected_at = Utc::now();
        let legs = smallvec![
            Leg {
                venue: cycle.venue,
                instrument: cycle.leg_a.instrument.clone(),
                side: cycle.leg_a.side,
                quote_price: result.price_a,
                order_price: result.price_a,
                quantity: qty_a,
                fee_estimate: result.fee_a,
            },
            Leg {
                venue: cycle.venue,
                instrument: cycle.leg_b.instrument.clone(),
                side: cycle.leg_b.side,
                quote_price: result.price_b,
                order_price: result.price_b,
                quantity: qty_b,
                fee_estimate: result.fee_b,
            },
            Leg {
                venue: cycle.venue,
                instrument: cycle.leg_c.instrument.clone(),
                side: cycle.leg_c.side,
                quote_price: result.price_c,
                order_price: result.price_c,
                quantity: qty_c,
                fee_estimate: result.fee_c,
            },
        ];

        Some(Opportunity {
            id: format!(
                "{}-{}",
                self.name(),
                detected_at.timestamp_nanos_opt().unwrap_or(0)
            ),
            kind: OpportunityKind::Triangular,
            legs,
            economics: Economics {
                gross_profit,
                fees_total,
                net_profit,
                net_profit_bps,
                notional,
            },
            meta: OpportunityMeta {
                detected_at,
                quote_ts_per_leg: smallvec![book_a.timestamp, book_b.timestamp, book_c.timestamp],
                ttl: Duration::milliseconds(50),
                strategy_id: self.name().to_string(),
            },
        })
    }
}

#[async_trait]
impl ArbitrageStrategy for TriangularArbStrategy {
    async fn evaluate(
        &self,
        books: &BookMap,
        _portfolios: &HashMap<String, PortfolioSnapshot>,
    ) -> Option<Opportunity> {
        self.cycles
            .iter()
            .filter_map(|cycle| self.evaluate_cycle(books, cycle))
            .max_by(|a, b| {
                a.economics
                    .net_profit_bps
                    .partial_cmp(&b.economics.net_profit_bps)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    fn compute_hedge_orders(&self, opp: &Opportunity) -> Vec<OrderRequest> {
        opp.legs
            .iter()
            .map(|leg| OrderRequest {
                venue: leg.venue,
                instrument: leg.instrument.clone(),
                side: leg.side,
                order_type: OrderType::Market,
                time_in_force: None,
                price: Some(leg.order_price),
                quantity: leg.quantity,
            })
            .collect()
    }

    fn name(&self) -> &str {
        "triangular_arb"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use crate::models::market::{BookMap, OrderBook, OrderBookLevel, book_key};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn spot(base: &str, quote: &str) -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: base.to_string(),
            quote: quote.to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn fee(rate: Decimal) -> FeeSchedule {
        FeeSchedule::new(Venue::Binance, rate, rate)
    }

    fn orderbook(venue: Venue, instrument: &Instrument, bid: Decimal, ask: Decimal) -> OrderBook {
        let now = Utc::now();
        OrderBook {
            venue,
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

    fn make_books(books: Vec<OrderBook>) -> BookMap {
        books
            .into_iter()
            .map(|b| (book_key(b.venue, &b.instrument), b))
            .collect()
    }

    fn empty_portfolios() -> HashMap<String, crate::models::position::PortfolioSnapshot> {
        HashMap::new()
    }

    /// BTC/USDT buy, BTC/ETH sell, ETH/USDT sell
    /// Start USDT -> buy BTC -> sell BTC for ETH -> sell ETH for USDT
    fn forward_cycle() -> TriangleCycle {
        TriangleCycle {
            venue: Venue::Binance,
            leg_a: TriangleLeg {
                instrument: spot("BTC", "USDT"),
                side: Side::Buy,
            },
            leg_b: TriangleLeg {
                instrument: spot("BTC", "ETH"),
                side: Side::Sell,
            },
            leg_c: TriangleLeg {
                instrument: spot("ETH", "USDT"),
                side: Side::Sell,
            },
            fee: fee(Decimal::ZERO),
            max_notional_usdt: dec!(10000),
            min_net_profit_bps: dec!(1),
            tick_size: dec!(0.01),
            lot_size: dec!(0.00001),
        }
    }

    fn strategy_with_cycle(cycle: TriangleCycle) -> TriangularArbStrategy {
        TriangularArbStrategy {
            cycles: vec![cycle],
            max_quote_age_ms: 5000,
            max_book_depth: 10,
        }
    }

    #[tokio::test]
    async fn no_opportunity_when_book_missing() {
        let cycle = forward_cycle();
        let s = strategy_with_cycle(cycle);
        // Only insert 2 of the 3 books
        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(50000), dec!(50001)),
            orderbook(Venue::Binance, &btc_eth, dec!(15), dec!(15.01)),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn no_opportunity_when_spread_is_zero() {
        let cycle = forward_cycle();
        let s = strategy_with_cycle(cycle);
        // Perfect prices: 1 BTC = 50000 USDT, 1 BTC = 15 ETH, 1 ETH = 50000/15 USDT
        // forward: start 10000 USDT
        // -> buy BTC at ask 50000 -> 0.2 BTC
        // -> sell BTC/ETH at bid 15 -> 3 ETH
        // -> sell ETH/USDT at bid 3333.33... -> 10000 USDT (neutral)
        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");
        let eth_price = dec!(50000) / dec!(15);
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(50000), dec!(50000)),
            orderbook(Venue::Binance, &btc_eth, dec!(15), dec!(15)),
            orderbook(Venue::Binance, &eth_usdt, eth_price, eth_price),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn detects_forward_cycle() {
        let cycle = forward_cycle();
        let s = strategy_with_cycle(cycle);
        // Mispriced: ETH/USDT is slightly high relative to BTC/USDT and BTC/ETH
        // 10000 USDT / 50000 (ask BTC/USDT) = 0.2 BTC
        // 0.2 BTC * 16 (bid BTC/ETH) = 3.2 ETH
        // 3.2 ETH * 3200 (bid ETH/USDT) = 10240 USDT -> profit
        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(49999), dec!(50000)),
            orderbook(Venue::Binance, &btc_eth, dec!(16), dec!(16.1)),
            orderbook(Venue::Binance, &eth_usdt, dec!(3200), dec!(3201)),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await;
        assert!(opp.is_some());
        let opp = opp.unwrap();
        assert_eq!(opp.legs.len(), 3);
        assert_eq!(opp.legs[0].side, Side::Buy);
        assert_eq!(opp.legs[1].side, Side::Sell);
        assert_eq!(opp.legs[2].side, Side::Sell);
        assert!(opp.economics.net_profit > Decimal::ZERO);
    }

    #[tokio::test]
    async fn detects_reverse_cycle() {
        // Reverse: buy ETH/USDT, buy BTC/ETH (buy BTC with ETH), sell BTC/USDT
        let cycle = TriangleCycle {
            venue: Venue::Binance,
            leg_a: TriangleLeg {
                instrument: spot("ETH", "USDT"),
                side: Side::Buy,
            },
            leg_b: TriangleLeg {
                instrument: spot("BTC", "ETH"),
                side: Side::Buy,
            },
            leg_c: TriangleLeg {
                instrument: spot("BTC", "USDT"),
                side: Side::Sell,
            },
            fee: fee(Decimal::ZERO),
            max_notional_usdt: dec!(10000),
            min_net_profit_bps: dec!(1),
            tick_size: dec!(0.01),
            lot_size: dec!(0.00001),
        };
        let s = strategy_with_cycle(cycle);
        // 10000 USDT / 2000 (ask ETH/USDT) = 5 ETH
        // 5 ETH / 16 (ask BTC/ETH) = 0.3125 BTC
        // 0.3125 BTC * 51200 (bid BTC/USDT) = 16000 USDT -> profit (16000-10000=6000)
        let eth_usdt = spot("ETH", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let btc_usdt = spot("BTC", "USDT");
        let books = make_books(vec![
            orderbook(Venue::Binance, &eth_usdt, dec!(1999), dec!(2000)),
            orderbook(Venue::Binance, &btc_eth, dec!(15.9), dec!(16)),
            orderbook(Venue::Binance, &btc_usdt, dec!(51200), dec!(51201)),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await;
        assert!(opp.is_some());
        let opp = opp.unwrap();
        assert!(opp.economics.net_profit > Decimal::ZERO);
        assert_eq!(opp.legs[0].side, Side::Buy);
        assert_eq!(opp.legs[1].side, Side::Buy);
        assert_eq!(opp.legs[2].side, Side::Sell);
    }

    #[tokio::test]
    async fn min_profit_bps_respected() {
        let mut cycle = forward_cycle();
        cycle.min_net_profit_bps = dec!(1000); // require 10% profit
        let s = strategy_with_cycle(cycle);
        // Tiny mispricing: will yield < 10% profit
        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(49999), dec!(50000)),
            orderbook(Venue::Binance, &btc_eth, dec!(16), dec!(16.1)),
            orderbook(Venue::Binance, &eth_usdt, dec!(3200), dec!(3201)),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn stale_book_returns_none() {
        let cycle = forward_cycle();
        let s = strategy_with_cycle(cycle);
        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");
        let mut stale_book = orderbook(Venue::Binance, &btc_eth, dec!(16), dec!(16.1));
        stale_book.local_timestamp = Utc::now() - Duration::seconds(10);
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(49999), dec!(50000)),
            stale_book,
            orderbook(Venue::Binance, &eth_usdt, dec!(3200), dec!(3201)),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn fee_reduces_profit() {
        // With high fees a small gross profit becomes net loss
        let mut cycle = forward_cycle();
        cycle.fee = fee(dec!(0.001)); // 10bps per leg = 30bps total
        cycle.min_net_profit_bps = dec!(1);
        let s = strategy_with_cycle(cycle);
        // Tiny mispricing that yields ~20bps gross but 30bps fees -> net loss
        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");
        // neutral prices: ETH = 50000/15 USDT, BTC/ETH bid=15, ETH/USDT bid=3333.33
        // tiny advantage: ETH/USDT bid = 3334 instead of 3333.33
        // gross: 10000 -> 0.2 BTC -> 3 ETH -> 3*3334 = 10002 (+2 USDT = 2bps gross)
        // fees ~ 30bps -> net negative
        let eth_price = dec!(50000) / dec!(15);
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(49999), dec!(50000)),
            orderbook(Venue::Binance, &btc_eth, dec!(15), dec!(15.01)),
            orderbook(Venue::Binance, &eth_usdt, eth_price, eth_price + dec!(1)),
        ]);
        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    #[tokio::test]
    async fn max_notional_scales_quantity() {
        let mut cycle_small = forward_cycle();
        cycle_small.max_notional_usdt = dec!(1000);
        let mut cycle_large = forward_cycle();
        cycle_large.max_notional_usdt = dec!(100000);

        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(49999), dec!(50000)),
            orderbook(Venue::Binance, &btc_eth, dec!(16), dec!(16.1)),
            orderbook(Venue::Binance, &eth_usdt, dec!(3200), dec!(3201)),
        ]);

        let s_small = strategy_with_cycle(cycle_small);
        let s_large = strategy_with_cycle(cycle_large);

        let opp_small = s_small.evaluate(&books, &empty_portfolios()).await.unwrap();
        let opp_large = s_large.evaluate(&books, &empty_portfolios()).await.unwrap();

        // larger notional -> larger leg quantities
        assert!(opp_large.legs[0].quantity > opp_small.legs[0].quantity);
        assert_eq!(opp_large.economics.notional, dec!(100000));
        assert_eq!(opp_small.economics.notional, dec!(1000));
    }

    #[tokio::test]
    async fn multiple_cycles_picks_best() {
        // One profitable cycle, one without books -> returns the profitable one
        let profitable = forward_cycle();
        let mut no_books_cycle = TriangleCycle {
            venue: Venue::Bybit,
            leg_a: TriangleLeg {
                instrument: spot("ETH", "USDT"),
                side: Side::Buy,
            },
            leg_b: TriangleLeg {
                instrument: spot("BTC", "ETH"),
                side: Side::Buy,
            },
            leg_c: TriangleLeg {
                instrument: spot("BTC", "USDT"),
                side: Side::Sell,
            },
            fee: fee(Decimal::ZERO),
            max_notional_usdt: dec!(10000),
            min_net_profit_bps: dec!(1),
            tick_size: dec!(0.01),
            lot_size: dec!(0.00001),
        };
        // Use a different venue so books won't be found
        no_books_cycle.venue = Venue::Okx;

        let s = TriangularArbStrategy {
            cycles: vec![profitable, no_books_cycle],
            max_quote_age_ms: 5000,
            max_book_depth: 10,
        };

        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(49999), dec!(50000)),
            orderbook(Venue::Binance, &btc_eth, dec!(16), dec!(16.1)),
            orderbook(Venue::Binance, &eth_usdt, dec!(3200), dec!(3201)),
        ]);

        let opp = s.evaluate(&books, &empty_portfolios()).await;
        assert!(opp.is_some());
        assert_eq!(opp.unwrap().legs[0].venue, Venue::Binance);
    }

    #[tokio::test]
    async fn both_cycles_profitable_picks_best_bps() {
        // Two profitable cycles with different profit levels; should return higher bps
        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");

        let cycle_low = TriangleCycle {
            venue: Venue::Binance,
            leg_a: TriangleLeg {
                instrument: btc_usdt.clone(),
                side: Side::Buy,
            },
            leg_b: TriangleLeg {
                instrument: btc_eth.clone(),
                side: Side::Sell,
            },
            leg_c: TriangleLeg {
                instrument: eth_usdt.clone(),
                side: Side::Sell,
            },
            fee: fee(Decimal::ZERO),
            max_notional_usdt: dec!(10000),
            min_net_profit_bps: dec!(1),
            tick_size: dec!(0.01),
            lot_size: dec!(0.00001),
        };
        // Reverse cycle with more favorable prices
        let cycle_high = TriangleCycle {
            venue: Venue::Binance,
            leg_a: TriangleLeg {
                instrument: eth_usdt.clone(),
                side: Side::Buy,
            },
            leg_b: TriangleLeg {
                instrument: btc_eth.clone(),
                side: Side::Buy,
            },
            leg_c: TriangleLeg {
                instrument: btc_usdt.clone(),
                side: Side::Sell,
            },
            fee: fee(Decimal::ZERO),
            max_notional_usdt: dec!(10000),
            min_net_profit_bps: dec!(1),
            tick_size: dec!(0.01),
            lot_size: dec!(0.00001),
        };

        let s = TriangularArbStrategy {
            cycles: vec![cycle_low, cycle_high],
            max_quote_age_ms: 5000,
            max_book_depth: 10,
        };

        // Forward: 10000/50000=0.2BTC, 0.2*16=3.2ETH, 3.2*3200=10240 -> +240 (240bps)
        // Reverse: 10000/2000=5ETH, 5/16=0.3125BTC, 0.3125*40000=12500 -> +2500 (2500bps)
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(40000), dec!(50000)),
            orderbook(Venue::Binance, &btc_eth, dec!(15.9), dec!(16)),
            orderbook(Venue::Binance, &eth_usdt, dec!(3199), dec!(2000)),
        ]);

        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        // reverse cycle should win (higher bps)
        assert_eq!(opp.legs[0].side, Side::Buy); // ETH/USDT buy
        assert_eq!(opp.legs[0].instrument, eth_usdt);
        assert!(opp.economics.net_profit_bps > dec!(240));
    }

    #[tokio::test]
    async fn lot_size_quantization() {
        let mut cycle = forward_cycle();
        cycle.lot_size = dec!(0.01);
        let s = strategy_with_cycle(cycle);
        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(49999), dec!(50000)),
            orderbook(Venue::Binance, &btc_eth, dec!(16), dec!(16.1)),
            orderbook(Venue::Binance, &eth_usdt, dec!(3200), dec!(3201)),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        // All quantities should be multiples of 0.01
        for leg in &opp.legs {
            let remainder = leg.quantity % dec!(0.01);
            assert_eq!(
                remainder,
                Decimal::ZERO,
                "quantity {} not quantized to 0.01",
                leg.quantity
            );
        }
    }

    #[tokio::test]
    async fn economics_fields_correct() {
        let cycle = forward_cycle(); // zero fees
        let s = strategy_with_cycle(cycle);
        let btc_usdt = spot("BTC", "USDT");
        let btc_eth = spot("BTC", "ETH");
        let eth_usdt = spot("ETH", "USDT");
        // 10000 / 50000 = 0.2 BTC (no fee)
        // 0.2 * 16 = 3.2 ETH (no fee)
        // 3.2 * 3200 = 10240 USDT (no fee)
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc_usdt, dec!(49999), dec!(50000)),
            orderbook(Venue::Binance, &btc_eth, dec!(16), dec!(16.1)),
            orderbook(Venue::Binance, &eth_usdt, dec!(3200), dec!(3201)),
        ]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.economics.notional, dec!(10000));
        assert_eq!(opp.economics.fees_total, Decimal::ZERO);
        // net = gross when fees = 0
        assert_eq!(opp.economics.net_profit, opp.economics.gross_profit);
        assert_eq!(opp.economics.net_profit, dec!(240));
        // bps = (240 / 10000) * 10000 = 240
        assert_eq!(opp.economics.net_profit_bps, dec!(240));
    }
}
