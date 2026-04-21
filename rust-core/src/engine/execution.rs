use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::models::enums::Side;
use crate::models::market::{BookMap, book_key};
use crate::models::order::OrderRequest;

/// Execution algorithm that transforms logical order requests into
/// venue-ready orders. Sits between compute_hedge_orders and submit_order.
#[async_trait]
pub trait ExecutionAlgorithm: Send + Sync {
    fn name(&self) -> &str;

    /// Transform a batch of logical orders into venue-ready orders.
    /// May split, re-price, or annotate orders based on book state.
    async fn prepare(&self, orders: Vec<OrderRequest>, books: &BookMap) -> Vec<OrderRequest>;
}

/// Passthrough: forwards orders unchanged (current default behavior).
pub struct IocAtTouch;

#[async_trait]
impl ExecutionAlgorithm for IocAtTouch {
    fn name(&self) -> &str {
        "ioc_at_touch"
    }

    async fn prepare(&self, orders: Vec<OrderRequest>, _books: &BookMap) -> Vec<OrderRequest> {
        orders
    }
}

/// Switch to limit post-only when spread exceeds a fee threshold.
pub struct PostOnlyMaker {
    /// Minimum spread in bps to switch from IOC taker to post-only maker.
    pub min_spread_bps: Decimal,
}

#[async_trait]
impl ExecutionAlgorithm for PostOnlyMaker {
    fn name(&self) -> &str {
        "post_only_maker"
    }

    async fn prepare(&self, orders: Vec<OrderRequest>, books: &BookMap) -> Vec<OrderRequest> {
        orders
            .into_iter()
            .map(|mut order| {
                let key = book_key(order.venue, &order.instrument);
                if let Some(book) = books.get(key.as_str()) {
                    if let (Some(bid), Some(ask)) = (book.bids.first(), book.asks.first()) {
                        let mid = (bid.price + ask.price) / Decimal::TWO;
                        if !mid.is_zero() {
                            let spread_bps = (ask.price - bid.price) / mid * Decimal::from(10_000);
                            if spread_bps > self.min_spread_bps {
                                // Wide spread: post as maker at touch
                                order.order_type = crate::models::enums::OrderType::Limit;
                                order.time_in_force = Some(crate::models::enums::TimeInForce::Rod);
                                order.price = Some(match order.side {
                                    Side::Buy => bid.price,
                                    Side::Sell => ask.price,
                                });
                            }
                        }
                    }
                }
                order
            })
            .collect()
    }
}

/// Walk L2 levels until marginal cost per level drops below threshold.
pub struct WalkTheBook {
    /// Stop walking when the next level adds less than this many bps.
    pub min_marginal_bps: Decimal,
}

#[async_trait]
impl ExecutionAlgorithm for WalkTheBook {
    fn name(&self) -> &str {
        "walk_the_book"
    }

    async fn prepare(&self, orders: Vec<OrderRequest>, books: &BookMap) -> Vec<OrderRequest> {
        orders
            .into_iter()
            .map(|mut order| {
                let key = book_key(order.venue, &order.instrument);
                if let Some(book) = books.get(key.as_str()) {
                    let levels = match order.side {
                        Side::Buy => &book.asks,
                        Side::Sell => &book.bids,
                    };
                    let mut filled = Decimal::ZERO;
                    let mut cost = Decimal::ZERO;
                    for (i, level) in levels.iter().enumerate() {
                        if i > 0 {
                            let prev_vwap = cost / filled;
                            let marginal_bps = ((level.price - prev_vwap).abs() / prev_vwap)
                                * Decimal::from(10_000);
                            if marginal_bps < self.min_marginal_bps {
                                break;
                            }
                        }
                        let take = level.size.min(order.quantity - filled);
                        filled += take;
                        cost += take * level.price;
                        if filled >= order.quantity {
                            break;
                        }
                    }
                    if filled < order.quantity && !filled.is_zero() {
                        order.quantity = filled;
                    }
                }
                order
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::{OrderType, TimeInForce, Venue};
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use crate::models::market::{OrderBook, OrderBookLevel};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use smallvec::smallvec;

    fn btc_usdt() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn make_order(side: Side, qty: Decimal) -> OrderRequest {
        OrderRequest {
            venue: Venue::Binance,
            instrument: btc_usdt(),
            side,
            order_type: OrderType::Market,
            time_in_force: Some(TimeInForce::Ioc),
            price: None,
            quantity: qty,
            estimated_notional: None,
        }
    }

    fn make_book(bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)]) -> BookMap {
        let inst = btc_usdt();
        let key = book_key(Venue::Binance, &inst);
        let now = Utc::now();
        let book = OrderBook {
            venue: Venue::Binance,
            instrument: inst,
            bids: bids
                .iter()
                .map(|(p, s)| OrderBookLevel {
                    price: *p,
                    size: *s,
                })
                .collect(),
            asks: asks
                .iter()
                .map(|(p, s)| OrderBookLevel {
                    price: *p,
                    size: *s,
                })
                .collect(),
            timestamp: now,
            local_timestamp: now,
        };
        let mut map = BookMap::default();
        map.insert(key, book);
        map
    }

    // --- IocAtTouch ---

    #[tokio::test]
    async fn ioc_at_touch_passthrough() {
        let algo = IocAtTouch;
        let orders = vec![
            make_order(Side::Buy, dec!(1)),
            make_order(Side::Sell, dec!(2)),
        ];
        let books = make_book(&[(dec!(100), dec!(5))], &[(dec!(101), dec!(5))]);
        let result = algo.prepare(orders.clone(), &books).await;

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].quantity, dec!(1));
        assert_eq!(result[0].order_type, OrderType::Market);
        assert_eq!(result[0].time_in_force, Some(TimeInForce::Ioc));
        assert_eq!(result[1].quantity, dec!(2));
    }

    // --- PostOnlyMaker ---

    #[tokio::test]
    async fn post_only_narrow_spread_no_change() {
        // Spread = 1 bps relative to mid ~ 100.005 => ~0.9995 bps
        // Threshold = 5 bps => no switch
        let algo = PostOnlyMaker {
            min_spread_bps: dec!(5),
        };
        let orders = vec![make_order(Side::Buy, dec!(1))];
        let books = make_book(&[(dec!(100_000), dec!(5))], &[(dec!(100_010), dec!(5))]);
        let result = algo.prepare(orders, &books).await;

        assert_eq!(result[0].order_type, OrderType::Market);
        assert_eq!(result[0].time_in_force, Some(TimeInForce::Ioc));
        assert!(result[0].price.is_none());
    }

    #[tokio::test]
    async fn post_only_wide_spread_switches_buy() {
        // bid=100, ask=110 => mid=105, spread_bps=(10/105)*10000 ~= 952 bps
        let algo = PostOnlyMaker {
            min_spread_bps: dec!(50),
        };
        let orders = vec![make_order(Side::Buy, dec!(1))];
        let books = make_book(&[(dec!(100), dec!(5))], &[(dec!(110), dec!(5))]);
        let result = algo.prepare(orders, &books).await;

        assert_eq!(result[0].order_type, OrderType::Limit);
        assert_eq!(result[0].time_in_force, Some(TimeInForce::Rod));
        // Buy posts at best bid
        assert_eq!(result[0].price, Some(dec!(100)));
    }

    #[tokio::test]
    async fn post_only_wide_spread_switches_sell() {
        let algo = PostOnlyMaker {
            min_spread_bps: dec!(50),
        };
        let orders = vec![make_order(Side::Sell, dec!(1))];
        let books = make_book(&[(dec!(100), dec!(5))], &[(dec!(110), dec!(5))]);
        let result = algo.prepare(orders, &books).await;

        assert_eq!(result[0].order_type, OrderType::Limit);
        assert_eq!(result[0].time_in_force, Some(TimeInForce::Rod));
        // Sell posts at best ask
        assert_eq!(result[0].price, Some(dec!(110)));
    }

    // --- WalkTheBook ---

    #[tokio::test]
    async fn walk_the_book_shallow_reduces_quantity() {
        // Buy order for 10, but only 3 available at level 0 (100.00)
        // and level 1 (200.00) is way too far from vwap => walk stops.
        let algo = WalkTheBook {
            min_marginal_bps: dec!(100),
        };
        let orders = vec![make_order(Side::Buy, dec!(10))];
        let books = make_book(
            &[(dec!(99), dec!(5))],
            &[(dec!(100), dec!(3)), (dec!(200), dec!(7))],
        );
        let result = algo.prepare(orders, &books).await;

        // Should reduce to 3 (only level 0 taken)
        assert_eq!(result[0].quantity, dec!(3));
    }

    #[tokio::test]
    async fn walk_the_book_deep_fills_full() {
        // Buy order for 5, levels at 100.00 (3) and 100.01 (5).
        // Marginal bps from 100.00 to 100.01 = (0.01/100)*10000 = 1 bps.
        // Threshold = 0.5 bps => both levels taken => fills 5 total.
        let algo = WalkTheBook {
            min_marginal_bps: dec!(0.5),
        };
        let orders = vec![make_order(Side::Buy, dec!(5))];
        let books = make_book(
            &[(dec!(99), dec!(5))],
            &[(dec!(100), dec!(3)), (dec!(100.01), dec!(5))],
        );
        let result = algo.prepare(orders, &books).await;

        assert_eq!(result[0].quantity, dec!(5));
    }

    #[tokio::test]
    async fn walk_the_book_no_book_passthrough() {
        let algo = WalkTheBook {
            min_marginal_bps: dec!(10),
        };
        let orders = vec![make_order(Side::Buy, dec!(5))];
        let books = BookMap::default();
        let result = algo.prepare(orders, &books).await;

        assert_eq!(result[0].quantity, dec!(5));
    }

    #[tokio::test]
    async fn walk_the_book_sell_side() {
        // Sell order for 10, bids at 100 (2) and 50 (8).
        // vwap after level 0 = 100, level 1 price = 50,
        // marginal_bps = |50-100|/100 * 10000 = 5000 bps => huge.
        // With threshold 100 bps both levels should be taken.
        let algo = WalkTheBook {
            min_marginal_bps: dec!(100),
        };
        let orders = vec![make_order(Side::Sell, dec!(10))];
        let books = make_book(
            &[(dec!(100), dec!(2)), (dec!(50), dec!(8))],
            &[(dec!(110), dec!(5))],
        );
        let result = algo.prepare(orders, &books).await;

        assert_eq!(result[0].quantity, dec!(10));
    }
}
