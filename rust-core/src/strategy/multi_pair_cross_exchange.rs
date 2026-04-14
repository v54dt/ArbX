use std::collections::HashMap;

use crate::models::enums::{OrderType, TimeInForce, Venue};
use crate::models::fee::FeeSchedule;
use crate::models::instrument::Instrument;
use crate::models::market::BookMap;
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;
use async_trait::async_trait;
use rust_decimal::Decimal;

use super::Opportunity;
use super::base::ArbitrageStrategy;
use super::cross_exchange::{DirectionParams, evaluate_cross_direction};

pub struct PairConfig {
    pub venue_a: Venue,
    pub venue_b: Venue,
    pub instrument_a: Instrument,
    pub instrument_b: Instrument,
    pub max_quantity: Decimal,
    pub tick_size_a: Decimal,
    pub tick_size_b: Decimal,
    pub lot_size_a: Decimal,
    pub lot_size_b: Decimal,
    pub fee_a: FeeSchedule,
    pub fee_b: FeeSchedule,
}

pub struct MultiPairCrossExchangeStrategy {
    pub pairs: Vec<PairConfig>,
    pub min_net_profit_bps: Decimal,
    pub max_quote_age_ms: i64,
    pub max_book_depth: usize,
}

#[async_trait]
impl ArbitrageStrategy for MultiPairCrossExchangeStrategy {
    async fn evaluate(
        &self,
        books: &BookMap,
        portfolios: &HashMap<String, PortfolioSnapshot>,
    ) -> Option<Opportunity> {
        let mut best: Option<Opportunity> = None;

        for pair in &self.pairs {
            let lot_size = pair.lot_size_a.max(pair.lot_size_b);

            let a_to_b = evaluate_cross_direction(
                books,
                portfolios,
                DirectionParams {
                    buy_venue: pair.venue_a,
                    buy_instrument: &pair.instrument_a,
                    sell_venue: pair.venue_b,
                    sell_instrument: &pair.instrument_b,
                    buy_fee: pair.fee_a.taker(),
                    sell_fee: pair.fee_b.taker(),
                    tick_size_buy: pair.tick_size_a,
                    tick_size_sell: pair.tick_size_b,
                    lot_size,
                    max_quantity: pair.max_quantity,
                    min_net_profit_bps: self.min_net_profit_bps,
                    strategy_id: self.name(),
                },
                self.max_quote_age_ms,
                self.max_book_depth,
            );

            let b_to_a = evaluate_cross_direction(
                books,
                portfolios,
                DirectionParams {
                    buy_venue: pair.venue_b,
                    buy_instrument: &pair.instrument_b,
                    sell_venue: pair.venue_a,
                    sell_instrument: &pair.instrument_a,
                    buy_fee: pair.fee_b.taker(),
                    sell_fee: pair.fee_a.taker(),
                    tick_size_buy: pair.tick_size_b,
                    tick_size_sell: pair.tick_size_a,
                    lot_size,
                    max_quantity: pair.max_quantity,
                    min_net_profit_bps: self.min_net_profit_bps,
                    strategy_id: self.name(),
                },
                self.max_quote_age_ms,
                self.max_book_depth,
            );

            // pick better direction for this pair
            let pair_best = match (a_to_b, b_to_a) {
                (Some(x), Some(y)) => {
                    if x.economics.net_profit_bps >= y.economics.net_profit_bps {
                        Some(x)
                    } else {
                        Some(y)
                    }
                }
                (Some(x), None) => Some(x),
                (None, Some(y)) => Some(y),
                (None, None) => None,
            };

            if let Some(candidate) = pair_best {
                let replace = match &best {
                    None => true,
                    Some(current) => {
                        candidate.economics.net_profit_bps > current.economics.net_profit_bps
                    }
                };
                if replace {
                    best = Some(candidate);
                }
            }
        }

        best
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
        "multi_pair_cross_exchange"
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
    use chrono::{Duration, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn instrument(base: &str, inst_type: InstrumentType) -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: inst_type,
            base: base.to_string(),
            quote: "USDT".to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn fee(taker: Decimal) -> FeeSchedule {
        FeeSchedule::new(Venue::Binance, taker, taker)
    }

    fn pair_config(
        base: &str,
        venue_a: Venue,
        venue_b: Venue,
        max_qty: Decimal,
        fee_rate_a: Decimal,
        fee_rate_b: Decimal,
    ) -> PairConfig {
        PairConfig {
            venue_a,
            venue_b,
            instrument_a: instrument(base, InstrumentType::Spot),
            instrument_b: instrument(base, InstrumentType::Spot),
            max_quantity: max_qty,
            tick_size_a: dec!(0.01),
            tick_size_b: dec!(0.01),
            lot_size_a: dec!(0.001),
            lot_size_b: dec!(0.001),
            fee_a: fee(fee_rate_a),
            fee_b: fee(fee_rate_b),
        }
    }

    fn strategy_with_pairs(pairs: Vec<PairConfig>) -> MultiPairCrossExchangeStrategy {
        MultiPairCrossExchangeStrategy {
            pairs,
            min_net_profit_bps: dec!(1),
            max_quote_age_ms: 5000,
            max_book_depth: 10,
        }
    }

    fn orderbook(
        venue: Venue,
        inst: &Instrument,
        bid: Decimal,
        bid_size: Decimal,
        ask: Decimal,
        ask_size: Decimal,
    ) -> OrderBook {
        let now = Utc::now();
        OrderBook {
            venue,
            instrument: inst.clone(),
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

    fn stale_orderbook(venue: Venue, inst: &Instrument, bid: Decimal, ask: Decimal) -> OrderBook {
        let old = Utc::now() - Duration::seconds(30);
        OrderBook {
            venue,
            instrument: inst.clone(),
            bids: vec![OrderBookLevel {
                price: bid,
                size: dec!(1),
            }],
            asks: vec![OrderBookLevel {
                price: ask,
                size: dec!(1),
            }],
            timestamp: old,
            local_timestamp: old,
        }
    }

    fn make_books(books: Vec<OrderBook>) -> BookMap {
        books
            .into_iter()
            .map(|b| (book_key(b.venue, &b.instrument), b))
            .collect()
    }

    fn empty_portfolios() -> HashMap<String, PortfolioSnapshot> {
        HashMap::new()
    }

    // 1. empty BookMap returns None
    #[tokio::test]
    async fn no_opportunities_with_empty_books() {
        let s = strategy_with_pairs(vec![pair_config(
            "BTC",
            Venue::Binance,
            Venue::Bybit,
            dec!(1),
            dec!(0.001),
            dec!(0.001),
        )]);
        assert!(
            s.evaluate(&BookMap::default(), &empty_portfolios())
                .await
                .is_none()
        );
    }

    // 2. 3 pairs, only one profitable
    #[tokio::test]
    async fn detects_best_opportunity_across_pairs() {
        let btc = instrument("BTC", InstrumentType::Spot);
        let eth = instrument("ETH", InstrumentType::Spot);
        let sol = instrument("SOL", InstrumentType::Spot);

        let s = strategy_with_pairs(vec![
            pair_config(
                "BTC",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
            pair_config(
                "ETH",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
            pair_config(
                "SOL",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
        ]);

        // BTC: no spread
        // ETH: no spread
        // SOL: profitable A->B
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc, dec!(100), dec!(1), dec!(101), dec!(1)),
            orderbook(Venue::Bybit, &btc, dec!(100), dec!(1), dec!(101), dec!(1)),
            orderbook(Venue::Binance, &eth, dec!(50), dec!(1), dec!(51), dec!(1)),
            orderbook(Venue::Bybit, &eth, dec!(50), dec!(1), dec!(51), dec!(1)),
            orderbook(Venue::Binance, &sol, dec!(9), dec!(1), dec!(10), dec!(1)),
            orderbook(Venue::Bybit, &sol, dec!(12), dec!(1), dec!(13), dec!(1)),
        ]);

        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].instrument.base, "SOL");
    }

    // 3. picks highest bps when multiple profitable
    #[tokio::test]
    async fn picks_highest_bps_when_multiple_profitable() {
        let btc = instrument("BTC", InstrumentType::Spot);
        let eth = instrument("ETH", InstrumentType::Spot);
        let sol = instrument("SOL", InstrumentType::Spot);

        let s = strategy_with_pairs(vec![
            pair_config(
                "BTC",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
            pair_config(
                "ETH",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
            pair_config(
                "SOL",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
        ]);

        // BTC: small spread ~2%
        // ETH: medium spread ~5%
        // SOL: large spread ~20% => highest bps
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc, dec!(99), dec!(1), dec!(100), dec!(1)),
            orderbook(Venue::Bybit, &btc, dec!(102), dec!(1), dec!(103), dec!(1)),
            orderbook(Venue::Binance, &eth, dec!(49), dec!(1), dec!(50), dec!(1)),
            orderbook(Venue::Bybit, &eth, dec!(55), dec!(1), dec!(56), dec!(1)),
            orderbook(Venue::Binance, &sol, dec!(9), dec!(1), dec!(10), dec!(1)),
            orderbook(Venue::Bybit, &sol, dec!(14), dec!(1), dec!(15), dec!(1)),
        ]);

        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].instrument.base, "SOL");
        assert!(opp.economics.net_profit_bps > dec!(0));
    }

    // 4. single pair behaves like CrossExchangeStrategy
    #[tokio::test]
    async fn single_pair_behaves_like_cross_exchange() {
        use crate::strategy::cross_exchange::CrossExchangeStrategy;

        let inst = instrument("BTC", InstrumentType::Spot);
        let multi = strategy_with_pairs(vec![pair_config(
            "BTC",
            Venue::Binance,
            Venue::Bybit,
            dec!(1),
            dec!(0.001),
            dec!(0.001),
        )]);
        let single = CrossExchangeStrategy {
            venue_a: Venue::Binance,
            venue_b: Venue::Bybit,
            instrument_a: inst.clone(),
            instrument_b: inst.clone(),
            min_net_profit_bps: dec!(1),
            max_quantity: dec!(1),
            fee_a: fee(dec!(0.001)),
            fee_b: fee(dec!(0.001)),
            max_quote_age_ms: 5000,
            tick_size_a: dec!(0.01),
            tick_size_b: dec!(0.01),
            lot_size_a: dec!(0.001),
            lot_size_b: dec!(0.001),
            max_book_depth: 10,
        };

        let books = make_books(vec![
            orderbook(Venue::Binance, &inst, dec!(99), dec!(1), dec!(100), dec!(1)),
            orderbook(Venue::Bybit, &inst, dec!(105), dec!(1), dec!(106), dec!(1)),
        ]);

        let opp_multi = multi.evaluate(&books, &empty_portfolios()).await.unwrap();
        let opp_single = single.evaluate(&books, &empty_portfolios()).await.unwrap();

        assert_eq!(
            opp_multi.economics.net_profit_bps,
            opp_single.economics.net_profit_bps
        );
        assert_eq!(opp_multi.legs[0].side, opp_single.legs[0].side);
        assert_eq!(opp_multi.legs[0].quantity, opp_single.legs[0].quantity);
    }

    // 5. stale book excluded
    #[tokio::test]
    async fn stale_book_excluded() {
        let inst = instrument("BTC", InstrumentType::Spot);
        let s = strategy_with_pairs(vec![pair_config(
            "BTC",
            Venue::Binance,
            Venue::Bybit,
            dec!(1),
            dec!(0.001),
            dec!(0.001),
        )]);

        let stale = stale_orderbook(Venue::Binance, &inst, dec!(99), dec!(100));
        let fresh = orderbook(Venue::Bybit, &inst, dec!(105), dec!(1), dec!(106), dec!(1));
        let books = make_books(vec![stale, fresh]);

        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    // 6. B->A profitable but A->B not
    #[tokio::test]
    async fn both_directions_evaluated() {
        let inst = instrument("BTC", InstrumentType::Spot);
        let s = strategy_with_pairs(vec![pair_config(
            "BTC",
            Venue::Binance,
            Venue::Bybit,
            dec!(1),
            dec!(0.001),
            dec!(0.001),
        )]);

        // Bybit ask < Binance bid => B->A is profitable
        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &inst,
                dec!(105),
                dec!(1),
                dec!(108),
                dec!(1),
            ),
            orderbook(Venue::Bybit, &inst, dec!(99), dec!(1), dec!(100), dec!(1)),
        ]);

        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].venue, Venue::Bybit);
        assert_eq!(opp.legs[0].side, Side::Buy);
        assert_eq!(opp.legs[1].venue, Venue::Binance);
        assert_eq!(opp.legs[1].side, Side::Sell);
    }

    // 7. min_net_profit_bps respected
    #[tokio::test]
    async fn min_net_profit_bps_respected() {
        let inst = instrument("BTC", InstrumentType::Spot);
        let mut s = strategy_with_pairs(vec![pair_config(
            "BTC",
            Venue::Binance,
            Venue::Bybit,
            dec!(1),
            dec!(0.001),
            dec!(0.001),
        )]);
        s.min_net_profit_bps = dec!(100); // require 100 bps net

        // tiny spread, won't survive fees + threshold
        let books = make_books(vec![
            orderbook(Venue::Binance, &inst, dec!(99), dec!(1), dec!(100), dec!(1)),
            orderbook(
                Venue::Bybit,
                &inst,
                dec!(100.5),
                dec!(1),
                dec!(101),
                dec!(1),
            ),
        ]);

        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }

    // 8. max_quantity per pair respected
    #[tokio::test]
    async fn max_quantity_per_pair_respected() {
        let btc = instrument("BTC", InstrumentType::Spot);
        let eth = instrument("ETH", InstrumentType::Spot);

        let s = strategy_with_pairs(vec![
            pair_config(
                "BTC",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
            pair_config(
                "ETH",
                Venue::Binance,
                Venue::Bybit,
                dec!(5),
                dec!(0.001),
                dec!(0.001),
            ),
        ]);

        let books = make_books(vec![
            orderbook(
                Venue::Binance,
                &btc,
                dec!(99),
                dec!(10),
                dec!(100),
                dec!(10),
            ),
            orderbook(Venue::Bybit, &btc, dec!(105), dec!(10), dec!(106), dec!(10)),
            orderbook(Venue::Binance, &eth, dec!(49), dec!(10), dec!(50), dec!(10)),
            orderbook(Venue::Bybit, &eth, dec!(55), dec!(10), dec!(56), dec!(10)),
        ]);

        // evaluate only BTC to check its cap
        let s_btc = strategy_with_pairs(vec![pair_config(
            "BTC",
            Venue::Binance,
            Venue::Bybit,
            dec!(1),
            dec!(0.001),
            dec!(0.001),
        )]);
        let books_btc = make_books(vec![
            orderbook(
                Venue::Binance,
                &btc,
                dec!(99),
                dec!(10),
                dec!(100),
                dec!(10),
            ),
            orderbook(Venue::Bybit, &btc, dec!(105), dec!(10), dec!(106), dec!(10)),
        ]);
        let opp_btc = s_btc
            .evaluate(&books_btc, &empty_portfolios())
            .await
            .unwrap();
        assert_eq!(opp_btc.legs[0].quantity, dec!(1)); // capped at max_quantity=1

        let s_eth = strategy_with_pairs(vec![pair_config(
            "ETH",
            Venue::Binance,
            Venue::Bybit,
            dec!(5),
            dec!(0.001),
            dec!(0.001),
        )]);
        let books_eth = make_books(vec![
            orderbook(Venue::Binance, &eth, dec!(49), dec!(10), dec!(50), dec!(10)),
            orderbook(Venue::Bybit, &eth, dec!(55), dec!(10), dec!(56), dec!(10)),
        ]);
        let opp_eth = s_eth
            .evaluate(&books_eth, &empty_portfolios())
            .await
            .unwrap();
        assert_eq!(opp_eth.legs[0].quantity, dec!(5)); // capped at max_quantity=5

        // full multi-pair: best is SOL-like by bps, but here ETH wins because spread is larger
        let _ = s.evaluate(&books, &empty_portfolios()).await.unwrap();
    }

    // 9. inventory reduces quantity
    #[tokio::test]
    async fn inventory_reduces_quantity() {
        let inst = instrument("BTC", InstrumentType::Spot);
        let s = strategy_with_pairs(vec![pair_config(
            "BTC",
            Venue::Binance,
            Venue::Bybit,
            dec!(1),
            dec!(0.001),
            dec!(0.001),
        )]);

        let books = make_books(vec![
            orderbook(Venue::Binance, &inst, dec!(99), dec!(1), dec!(100), dec!(1)),
            orderbook(Venue::Bybit, &inst, dec!(105), dec!(1), dec!(106), dec!(1)),
        ]);

        let mut portfolios = HashMap::new();
        portfolios.insert(
            "binance".to_string(),
            PortfolioSnapshot {
                venue: Venue::Binance,
                positions: vec![Position {
                    venue: Venue::Binance,
                    instrument: inst.clone(),
                    quantity: dec!(0.8), // exceeds half_max=0.5 => excess=0.3
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
        // raw_qty=1, reduced=1-0.3=0.7
        assert_eq!(opp.legs[0].quantity, dec!(0.7));
    }

    // 10. multi-level VWAP gives different result than TOB
    #[tokio::test]
    async fn vwap_multi_level_used() {
        let inst = instrument("BTC", InstrumentType::Spot);
        let now = Utc::now();
        let buy_book = OrderBook {
            venue: Venue::Binance,
            instrument: inst.clone(),
            bids: vec![OrderBookLevel {
                price: dec!(99),
                size: dec!(5),
            }],
            asks: vec![
                OrderBookLevel {
                    price: dec!(100),
                    size: dec!(1),
                },
                OrderBookLevel {
                    price: dec!(101),
                    size: dec!(1),
                },
                OrderBookLevel {
                    price: dec!(102),
                    size: dec!(1),
                },
            ],
            timestamp: now,
            local_timestamp: now,
        };
        let sell_book = OrderBook {
            venue: Venue::Bybit,
            instrument: inst.clone(),
            bids: vec![
                OrderBookLevel {
                    price: dec!(108),
                    size: dec!(1),
                },
                OrderBookLevel {
                    price: dec!(107),
                    size: dec!(1),
                },
                OrderBookLevel {
                    price: dec!(106),
                    size: dec!(1),
                },
            ],
            asks: vec![OrderBookLevel {
                price: dec!(115),
                size: dec!(5),
            }],
            timestamp: now,
            local_timestamp: now,
        };

        let s = strategy_with_pairs(vec![PairConfig {
            venue_a: Venue::Binance,
            venue_b: Venue::Bybit,
            instrument_a: inst.clone(),
            instrument_b: inst.clone(),
            max_quantity: dec!(3),
            tick_size_a: dec!(0.01),
            tick_size_b: dec!(0.01),
            lot_size_a: dec!(0.001),
            lot_size_b: dec!(0.001),
            fee_a: fee(dec!(0.001)),
            fee_b: fee(dec!(0.001)),
        }]);

        let books = make_books(vec![buy_book, sell_book]);
        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        // VWAP ask = (100+101+102)/3 = 101; TOB ask = 100 => different
        assert_eq!(opp.legs[0].quantity, dec!(3));
        assert_eq!(opp.legs[0].quote_price, dec!(101));
    }

    // 11. different fee per pair: high-fee pair misses, low-fee pair hits
    #[tokio::test]
    async fn different_fee_per_pair() {
        let btc = instrument("BTC", InstrumentType::Spot);
        let eth = instrument("ETH", InstrumentType::Spot);

        let s = strategy_with_pairs(vec![
            // BTC: 10bps fee each side => total 20bps costs eat the spread
            pair_config(
                "BTC",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
            // ETH: 1bps fee each side => minimal cost, opportunity survives
            pair_config(
                "ETH",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.0001),
                dec!(0.0001),
            ),
        ]);

        // BTC spread: ask=100, bid=100.1 => tiny, fees kill it
        // ETH spread: ask=100, bid=102 => large enough even with low fee
        let books = make_books(vec![
            orderbook(Venue::Binance, &btc, dec!(99), dec!(1), dec!(100), dec!(1)),
            orderbook(
                Venue::Bybit,
                &btc,
                dec!(100.1),
                dec!(1),
                dec!(100.5),
                dec!(1),
            ),
            orderbook(Venue::Binance, &eth, dec!(99), dec!(1), dec!(100), dec!(1)),
            orderbook(Venue::Bybit, &eth, dec!(102), dec!(1), dec!(103), dec!(1)),
        ]);

        let opp = s.evaluate(&books, &empty_portfolios()).await.unwrap();
        assert_eq!(opp.legs[0].instrument.base, "ETH");
    }

    // 12. all pairs stale returns None
    #[tokio::test]
    async fn all_pairs_stale_returns_none() {
        let btc = instrument("BTC", InstrumentType::Spot);
        let eth = instrument("ETH", InstrumentType::Spot);

        let s = strategy_with_pairs(vec![
            pair_config(
                "BTC",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
            pair_config(
                "ETH",
                Venue::Binance,
                Venue::Bybit,
                dec!(1),
                dec!(0.001),
                dec!(0.001),
            ),
        ]);

        let books = make_books(vec![
            stale_orderbook(Venue::Binance, &btc, dec!(99), dec!(100)),
            stale_orderbook(Venue::Bybit, &btc, dec!(105), dec!(106)),
            stale_orderbook(Venue::Binance, &eth, dec!(49), dec!(50)),
            stale_orderbook(Venue::Bybit, &eth, dec!(55), dec!(56)),
        ]);

        assert!(s.evaluate(&books, &empty_portfolios()).await.is_none());
    }
}
