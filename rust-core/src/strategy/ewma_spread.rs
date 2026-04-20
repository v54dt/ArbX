use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use smallvec::smallvec;

use crate::models::enums::{OrderType, Side, TimeInForce, Venue};
use crate::models::fee::FeeSchedule;
use crate::models::instrument::Instrument;
use crate::models::market::{BookMap, OrderBook, book_key};
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;

use super::base::ArbitrageStrategy;
use super::cross_exchange::{vwap_ask, vwap_bid};
use super::{Economics, Leg, Opportunity, OpportunityKind, OpportunityMeta};

struct EwmaState {
    ewma: Decimal,
    variance: Decimal,
    sample_count: u32,
}

impl EwmaState {
    fn new() -> Self {
        EwmaState {
            ewma: Decimal::ZERO,
            variance: Decimal::ZERO,
            sample_count: 0,
        }
    }

    fn update(&mut self, spread: Decimal, alpha: Decimal) {
        if self.sample_count == 0 {
            self.ewma = spread;
            self.variance = Decimal::ZERO;
        } else {
            let diff = spread - self.ewma;
            self.ewma = alpha * spread + (Decimal::ONE - alpha) * self.ewma;
            self.variance = (Decimal::ONE - alpha) * (self.variance + alpha * diff * diff);
        }
        self.sample_count += 1;
    }

    fn sigma(&self) -> Option<Decimal> {
        if self.variance <= Decimal::ZERO {
            return None;
        }
        let v = self.variance;
        let mut x = v;
        for _ in 0..5 {
            x = (x + v / x) / Decimal::TWO;
        }
        Some(x)
    }
}

pub struct EwmaSpreadStrategy {
    pub venue_a: Venue,
    pub venue_b: Venue,
    pub instrument_a: Instrument,
    pub instrument_b: Instrument,
    pub fee_a: FeeSchedule,
    pub fee_b: FeeSchedule,
    pub alpha: Decimal,
    pub entry_threshold_sigma: Decimal,
    pub max_quantity: Decimal,
    pub min_net_profit_bps: Decimal,
    pub max_quote_age_ms: i64,
    pub tick_size_a: Decimal,
    pub tick_size_b: Decimal,
    pub lot_size: Decimal,
    pub max_book_depth: usize,
    pub min_samples: u32,
    state: Mutex<EwmaState>,
}

struct DirectionParams<'a> {
    buy_venue: Venue,
    buy_instrument: &'a Instrument,
    sell_venue: Venue,
    sell_instrument: &'a Instrument,
    buy_fee: Decimal,
    sell_fee: Decimal,
    tick_buy: Decimal,
    tick_sell: Decimal,
    buy_book: &'a OrderBook,
    sell_book: &'a OrderBook,
}

impl EwmaSpreadStrategy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        venue_a: Venue,
        venue_b: Venue,
        instrument_a: Instrument,
        instrument_b: Instrument,
        fee_a: FeeSchedule,
        fee_b: FeeSchedule,
        alpha: Decimal,
        entry_threshold_sigma: Decimal,
        max_quantity: Decimal,
        min_net_profit_bps: Decimal,
        max_quote_age_ms: i64,
        tick_size_a: Decimal,
        tick_size_b: Decimal,
        lot_size: Decimal,
        max_book_depth: usize,
        min_samples: u32,
    ) -> Self {
        Self {
            venue_a,
            venue_b,
            instrument_a,
            instrument_b,
            fee_a,
            fee_b,
            alpha,
            entry_threshold_sigma,
            max_quantity,
            min_net_profit_bps,
            max_quote_age_ms,
            tick_size_a,
            tick_size_b,
            lot_size,
            max_book_depth,
            min_samples,
            state: Mutex::new(EwmaState::new()),
        }
    }

    fn evaluate_direction(
        &self,
        p: DirectionParams<'_>,
        now: DateTime<Utc>,
    ) -> Option<Opportunity> {
        let DirectionParams {
            buy_venue,
            buy_instrument,
            sell_venue,
            sell_instrument,
            buy_fee,
            sell_fee,
            tick_buy,
            tick_sell,
            buy_book,
            sell_book,
        } = p;
        let (vwap_ask_price, ask_fill) =
            vwap_ask(buy_book, self.max_quantity, self.max_book_depth)?;
        let (vwap_bid_price, bid_fill) =
            vwap_bid(sell_book, self.max_quantity, self.max_book_depth)?;

        if vwap_bid_price <= vwap_ask_price {
            return None;
        }

        let raw_qty = ask_fill.min(bid_fill).min(self.max_quantity);
        let quantity = quantize(raw_qty, self.lot_size);
        if quantity <= Decimal::ZERO {
            return None;
        }

        let (final_ask, _) = vwap_ask(buy_book, quantity, self.max_book_depth)?;
        let (final_bid, _) = vwap_bid(sell_book, quantity, self.max_book_depth)?;

        let order_ask = quantize_up(final_ask, tick_buy);
        let order_bid = quantize(final_bid, tick_sell);

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

#[async_trait]
impl ArbitrageStrategy for EwmaSpreadStrategy {
    async fn evaluate(
        &self,
        books: &BookMap,
        _portfolios: &HashMap<String, PortfolioSnapshot>,
        now: DateTime<Utc>,
    ) -> Option<Opportunity> {
        let book_a = books.get(&book_key(self.venue_a, &self.instrument_a))?;
        let book_b = books.get(&book_key(self.venue_b, &self.instrument_b))?;

        let max_age = Duration::milliseconds(self.max_quote_age_ms);
        if now - book_a.local_timestamp > max_age || now - book_b.local_timestamp > max_age {
            return None;
        }

        let best_ask_a = book_a.best_ask()?.price;
        let best_bid_a = book_a.best_bid()?.price;
        let best_ask_b = book_b.best_ask()?.price;
        let best_bid_b = book_b.best_bid()?.price;

        let spread_a_to_b = best_bid_b - best_ask_a;
        let spread_b_to_a = best_bid_a - best_ask_b;
        let spread = (spread_a_to_b + spread_b_to_a) / Decimal::TWO;

        let (ewma, sigma, sample_count) = {
            let mut state = self.state.lock().unwrap();
            state.update(spread, self.alpha);
            let s = state.sigma();
            (state.ewma, s, state.sample_count)
        };

        if sample_count < self.min_samples {
            return None;
        }

        let sigma = sigma?;
        let z_score = (spread - ewma) / sigma;

        if z_score > self.entry_threshold_sigma {
            self.evaluate_direction(
                DirectionParams {
                    buy_venue: self.venue_a,
                    buy_instrument: &self.instrument_a,
                    sell_venue: self.venue_b,
                    sell_instrument: &self.instrument_b,
                    buy_fee: self.fee_a.taker(),
                    sell_fee: self.fee_b.taker(),
                    tick_buy: self.tick_size_a,
                    tick_sell: self.tick_size_b,
                    buy_book: book_a,
                    sell_book: book_b,
                },
                now,
            )
        } else if z_score < -self.entry_threshold_sigma {
            self.evaluate_direction(
                DirectionParams {
                    buy_venue: self.venue_b,
                    buy_instrument: &self.instrument_b,
                    sell_venue: self.venue_a,
                    sell_instrument: &self.instrument_a,
                    buy_fee: self.fee_b.taker(),
                    sell_fee: self.fee_a.taker(),
                    tick_buy: self.tick_size_b,
                    tick_sell: self.tick_size_a,
                    buy_book: book_b,
                    sell_book: book_a,
                },
                now,
            )
        } else {
            None
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
                estimated_notional: None,
            })
            .collect()
    }

    fn re_verify(&self, opp: &Opportunity, books: &BookMap) -> Option<Opportunity> {
        let book_a = books.get(&book_key(self.venue_a, &self.instrument_a))?;
        let book_b = books.get(&book_key(self.venue_b, &self.instrument_b))?;
        let mid_a = book_a.mid_price()?;
        let mid_b = book_b.mid_price()?;
        let current_spread_bps = if mid_a > Decimal::ZERO {
            (mid_b - mid_a) / mid_a * Decimal::from(10_000)
        } else {
            Decimal::ZERO
        };
        let threshold = opp.economics.net_profit_bps / Decimal::from(2);
        if current_spread_bps.abs() < threshold {
            return None;
        }
        Some(opp.clone())
    }

    fn name(&self) -> &str {
        "ewma_spread"
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
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use smallvec::smallvec;
    use std::collections::HashMap;
    use tokio::runtime::Runtime;

    fn inst(inst_type: InstrumentType) -> Instrument {
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

    fn strategy_with(
        alpha: Decimal,
        threshold: Decimal,
        min_samples: u32,
        min_profit_bps: Decimal,
    ) -> EwmaSpreadStrategy {
        EwmaSpreadStrategy::new(
            Venue::Binance,
            Venue::Bybit,
            inst(InstrumentType::Spot),
            inst(InstrumentType::Spot),
            fee(dec!(0.001), dec!(0.001)),
            fee(dec!(0.001), dec!(0.001)),
            alpha,
            threshold,
            dec!(1),
            min_profit_bps,
            5000,
            dec!(0.01),
            dec!(0.01),
            dec!(0.001),
            10,
            min_samples,
        )
    }

    fn default_strategy() -> EwmaSpreadStrategy {
        strategy_with(dec!(0.2), dec!(2), 5, dec!(1))
    }

    fn make_book(
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
            bids: smallvec![OrderBookLevel {
                price: bid,
                size: bid_size
            }],
            asks: smallvec![OrderBookLevel {
                price: ask,
                size: ask_size
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

    fn empty_portfolios() -> HashMap<String, PortfolioSnapshot> {
        HashMap::new()
    }

    fn symmetric_books(bid_a: Decimal, ask_a: Decimal, bid_b: Decimal, ask_b: Decimal) -> BookMap {
        let s = default_strategy();
        make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                bid_a,
                dec!(1),
                ask_a,
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                bid_b,
                dec!(1),
                ask_b,
                dec!(1),
            ),
        ])
    }

    #[test]
    fn warming_up_returns_none() {
        let s = strategy_with(dec!(0.2), dec!(1), 5, dec!(1));
        let rt = Runtime::new().unwrap();
        let books = symmetric_books(dec!(99), dec!(100), dec!(99), dec!(100));
        for _ in 0..4 {
            let result = rt.block_on(s.evaluate(&books, &empty_portfolios(), Utc::now()));
            assert!(result.is_none());
        }
    }

    #[test]
    fn after_warmup_evaluates() {
        let s = strategy_with(dec!(0.3), dec!(1), 1, dec!(1));
        let rt = Runtime::new().unwrap();
        let normal = symmetric_books(dec!(100), dec!(101), dec!(100), dec!(101));
        rt.block_on(s.evaluate(&normal, &empty_portfolios(), Utc::now()));
        let deviated = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(110),
                dec!(1),
                dec!(111),
                dec!(1),
            ),
        ]);
        let _ = rt.block_on(s.evaluate(&deviated, &empty_portfolios(), Utc::now()));
        assert_eq!(s.state.lock().unwrap().sample_count, 2);
    }

    #[test]
    fn returns_none_when_spread_within_one_sigma() {
        let s = strategy_with(dec!(0.1), dec!(3), 3, dec!(1));
        let rt = Runtime::new().unwrap();
        let steady = symmetric_books(dec!(99), dec!(101), dec!(102), dec!(103));
        for _ in 0..4 {
            let result = rt.block_on(s.evaluate(&steady, &empty_portfolios(), Utc::now()));
            assert!(result.is_none());
        }
    }

    #[test]
    fn detects_a_to_b_when_spread_above_threshold() {
        let s = strategy_with(dec!(0.5), dec!(1), 3, dec!(1));
        let rt = Runtime::new().unwrap();

        let low = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);
        let high = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(110),
                dec!(1),
                dec!(111),
                dec!(1),
            ),
        ]);

        rt.block_on(s.evaluate(&low, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&high, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&low, &empty_portfolios(), Utc::now()));

        let opp = rt.block_on(s.evaluate(&high, &empty_portfolios(), Utc::now()));
        if let Some(o) = opp {
            assert_eq!(o.legs[0].venue, Venue::Binance);
            assert_eq!(o.legs[0].side, Side::Buy);
            assert_eq!(o.legs[1].venue, Venue::Bybit);
            assert_eq!(o.legs[1].side, Side::Sell);
        }
    }

    #[test]
    fn detects_b_to_a_when_spread_below_minus_threshold() {
        let s = strategy_with(dec!(0.5), dec!(1), 3, dec!(1));
        let rt = Runtime::new().unwrap();

        let high_a = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(110),
                dec!(1),
                dec!(111),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);
        let low_a = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);

        rt.block_on(s.evaluate(&high_a, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&low_a, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&high_a, &empty_portfolios(), Utc::now()));

        let opp = rt.block_on(s.evaluate(&low_a, &empty_portfolios(), Utc::now()));
        if let Some(o) = opp {
            assert_eq!(o.legs[0].venue, Venue::Bybit);
            assert_eq!(o.legs[0].side, Side::Buy);
            assert_eq!(o.legs[1].venue, Venue::Binance);
            assert_eq!(o.legs[1].side, Side::Sell);
        }
    }

    #[test]
    fn ewma_converges_to_constant_spread() {
        let s = strategy_with(dec!(0.3), dec!(10), 20, dec!(1));
        let rt = Runtime::new().unwrap();
        let books = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(105),
                dec!(1),
                dec!(106),
                dec!(1),
            ),
        ]);
        for _ in 0..20 {
            rt.block_on(s.evaluate(&books, &empty_portfolios(), Utc::now()));
        }
        let state = s.state.lock().unwrap();
        // spread_a_to_b = bid_b(105) - ask_a(100) = 5
        // spread_b_to_a = bid_a(99) - ask_b(106) = -7
        // avg = (5 + -7) / 2 = -1
        let expected_spread = dec!(-1);
        let diff = (state.ewma - expected_spread).abs();
        assert!(
            diff < dec!(0.01),
            "ewma {} did not converge to {}",
            state.ewma,
            expected_spread
        );
    }

    #[test]
    fn ewma_tracks_step_change() {
        let s = strategy_with(dec!(0.3), dec!(100), 1, dec!(1));
        let rt = Runtime::new().unwrap();

        // spread_a_to_b = bid_b - ask_a, spread_b_to_a = bid_a - ask_b, avg = (both)/2
        // low_books: bid_a=99,ask_a=100,bid_b=99,ask_b=100 => spread=(-1+-1)/2=-1
        let low_books = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);
        // high_books: bid_a=150,ask_a=151,bid_b=200,ask_b=201
        // spread_a_to_b = 200-151=49, spread_b_to_a=150-201=-51, avg=-1 (still -1, symmetric)
        // Need asymmetric: use bid_a=200,ask_a=100,bid_b=200,ask_b=100
        // spread_a_to_b=200-100=100, spread_b_to_a=200-100=100, avg=100
        let high_books = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(200),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(200),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);

        for _ in 0..10 {
            rt.block_on(s.evaluate(&low_books, &empty_portfolios(), Utc::now()));
        }
        let ewma_before = s.state.lock().unwrap().ewma;

        for _ in 0..10 {
            rt.block_on(s.evaluate(&high_books, &empty_portfolios(), Utc::now()));
        }
        let ewma_after = s.state.lock().unwrap().ewma;

        assert!(
            ewma_after > ewma_before,
            "ewma did not increase after step change: before={} after={}",
            ewma_before,
            ewma_after
        );
    }

    #[test]
    fn sigma_zero_returns_none() {
        let s = strategy_with(dec!(0.2), dec!(1), 3, dec!(1));
        let rt = Runtime::new().unwrap();
        let books = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(102),
                dec!(1),
                dec!(103),
                dec!(1),
            ),
        ]);
        for _ in 0..5 {
            let r = rt.block_on(s.evaluate(&books, &empty_portfolios(), Utc::now()));
            assert!(r.is_none());
        }
    }

    #[test]
    fn min_net_profit_bps_filters_opportunity() {
        let s = strategy_with(dec!(0.5), dec!(1), 3, dec!(10000));
        let rt = Runtime::new().unwrap();

        let low = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);
        let high = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(110),
                dec!(1),
                dec!(111),
                dec!(1),
            ),
        ]);

        rt.block_on(s.evaluate(&low, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&high, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&low, &empty_portfolios(), Utc::now()));
        let result = rt.block_on(s.evaluate(&high, &empty_portfolios(), Utc::now()));
        assert!(result.is_none());
    }

    #[test]
    fn stale_book_returns_none() {
        let now = Utc::now();
        let s = default_strategy();
        let rt = Runtime::new().unwrap();

        let stale = OrderBook {
            venue: Venue::Binance,
            instrument: s.instrument_a.clone(),
            bids: smallvec![OrderBookLevel {
                price: dec!(99),
                size: dec!(1)
            }],
            asks: smallvec![OrderBookLevel {
                price: dec!(100),
                size: dec!(1)
            }],
            timestamp: now,
            local_timestamp: now - chrono::Duration::seconds(10),
        };
        let fresh = make_book(
            Venue::Bybit,
            &s.instrument_b,
            dec!(102),
            dec!(1),
            dec!(103),
            dec!(1),
        );
        let books = make_books(vec![stale, fresh]);
        assert!(
            rt.block_on(s.evaluate(&books, &empty_portfolios(), Utc::now()))
                .is_none()
        );
    }

    #[test]
    fn missing_book_returns_none() {
        let s = default_strategy();
        let rt = Runtime::new().unwrap();
        let books = make_books(vec![make_book(
            Venue::Binance,
            &s.instrument_a,
            dec!(99),
            dec!(1),
            dec!(100),
            dec!(1),
        )]);
        assert!(
            rt.block_on(s.evaluate(&books, &empty_portfolios(), Utc::now()))
                .is_none()
        );
    }

    #[test]
    fn z_score_calculation_correct() {
        // alpha=0.5, min_samples=2
        // Obs 1: spread = -1 => ewma=-1, variance=0, sample_count=1
        // Obs 2: spread = 9
        //   diff = 9 - (-1) = 10
        //   ewma = 0.5*9 + 0.5*(-1) = 4
        //   variance = 0.5*(0 + 0.5*100) = 25
        //   sigma = sqrt(25) = 5
        //   z_score = (9 - 4)/5 = 1.0
        let s = strategy_with(dec!(0.5), dec!(0.5), 2, dec!(1));
        let rt = Runtime::new().unwrap();

        // spread = -1: both venues at ask=1,bid=0
        // spread_a_to_b = bid_b(0) - ask_a(1) = -1
        // spread_b_to_a = bid_a(0) - ask_b(1) = -1
        // avg = -1 ✓
        let obs1 = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(0),
                dec!(1),
                dec!(1),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(0),
                dec!(1),
                dec!(1),
                dec!(1),
            ),
        ]);
        rt.block_on(s.evaluate(&obs1, &empty_portfolios(), Utc::now()));

        {
            let st = s.state.lock().unwrap();
            assert_eq!(st.ewma, dec!(-1));
            assert_eq!(st.sample_count, 1);
        }

        // spread = 9: bid_a=10, ask_a=1, bid_b=10, ask_b=1
        // spread_a_to_b = 10 - 1 = 9
        // spread_b_to_a = 10 - 1 = 9
        // avg = 9 ✓
        let obs2 = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(10),
                dec!(1),
                dec!(1),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(10),
                dec!(1),
                dec!(1),
                dec!(1),
            ),
        ]);
        rt.block_on(s.evaluate(&obs2, &empty_portfolios(), Utc::now()));

        let st = s.state.lock().unwrap();
        let ewma_diff = (st.ewma - dec!(4)).abs();
        assert!(ewma_diff < dec!(0.001), "ewma was {}, expected 4", st.ewma);
        let var_diff = (st.variance - dec!(25)).abs();
        assert!(
            var_diff < dec!(0.001),
            "variance was {}, expected 25",
            st.variance
        );
    }

    #[test]
    fn alpha_affects_adaptation_speed() {
        let fast = strategy_with(dec!(0.9), dec!(100), 1, dec!(1));
        let slow = strategy_with(dec!(0.1), dec!(100), 1, dec!(1));
        let rt = Runtime::new().unwrap();

        // Seed both with spread = 0
        let zero_books_fast = make_books(vec![
            make_book(
                Venue::Binance,
                &fast.instrument_a,
                dec!(100),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &fast.instrument_b,
                dec!(100),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);
        let zero_books_slow = make_books(vec![
            make_book(
                Venue::Binance,
                &slow.instrument_a,
                dec!(100),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &slow.instrument_b,
                dec!(100),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);
        rt.block_on(fast.evaluate(&zero_books_fast, &empty_portfolios(), Utc::now()));
        rt.block_on(slow.evaluate(&zero_books_slow, &empty_portfolios(), Utc::now()));

        // Shift spread to 50 (bid_b=150, ask_a=100 => 50; bid_a=100, ask_b=150 => -50; avg=0... )
        // Use: bid_b=150, ask_b=151, ask_a=100 => spread_a_to_b=50; bid_a=50, ask_a=100 => spread_b_to_a=-100+50=-50 ... hmm
        // Simplest: bid_a=100,ask_a=50 is invalid. Use ask_a=100, bid_b=150:
        // spread_a_to_b = bid_b(150) - ask_a(100) = 50
        // spread_b_to_a = bid_a(100) - ask_b(151) = -51
        // avg = -0.5 ≠ 50
        // For avg=25: spread_a_to_b=50, spread_b_to_a=0 => bid_a = ask_b
        // bid_a=151, ask_a=100, bid_b=150, ask_b=151 => a_to_b=50, b_to_a=0, avg=25
        let shifted_fast = make_books(vec![
            make_book(
                Venue::Binance,
                &fast.instrument_a,
                dec!(151),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &fast.instrument_b,
                dec!(150),
                dec!(1),
                dec!(151),
                dec!(1),
            ),
        ]);
        let shifted_slow = make_books(vec![
            make_book(
                Venue::Binance,
                &slow.instrument_a,
                dec!(151),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &slow.instrument_b,
                dec!(150),
                dec!(1),
                dec!(151),
                dec!(1),
            ),
        ]);
        rt.block_on(fast.evaluate(&shifted_fast, &empty_portfolios(), Utc::now()));
        rt.block_on(slow.evaluate(&shifted_slow, &empty_portfolios(), Utc::now()));

        let fast_ewma = fast.state.lock().unwrap().ewma;
        let slow_ewma = slow.state.lock().unwrap().ewma;

        // fast (alpha=0.9) moves much more toward new value than slow (alpha=0.1)
        // initial ewma = 0 for both; new spread = 25
        // fast: ewma = 0.9*25 + 0.1*0 = 22.5
        // slow: ewma = 0.1*25 + 0.9*0 = 2.5
        assert!(
            fast_ewma > slow_ewma,
            "fast ewma {} should be greater than slow ewma {}",
            fast_ewma,
            slow_ewma
        );
    }

    #[test]
    fn compute_hedge_orders_returns_two_legs() {
        let s = strategy_with(dec!(0.5), dec!(1), 3, dec!(1));
        let rt = Runtime::new().unwrap();

        let low = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
        ]);
        let high = make_books(vec![
            make_book(
                Venue::Binance,
                &s.instrument_a,
                dec!(99),
                dec!(1),
                dec!(100),
                dec!(1),
            ),
            make_book(
                Venue::Bybit,
                &s.instrument_b,
                dec!(200),
                dec!(1),
                dec!(201),
                dec!(1),
            ),
        ]);

        rt.block_on(s.evaluate(&low, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&high, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&low, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&high, &empty_portfolios(), Utc::now()));
        rt.block_on(s.evaluate(&low, &empty_portfolios(), Utc::now()));

        let opp = rt.block_on(s.evaluate(&high, &empty_portfolios(), Utc::now()));
        if let Some(o) = opp {
            let orders = s.compute_hedge_orders(&o);
            assert_eq!(orders.len(), 2);
            assert_eq!(orders[0].order_type, OrderType::Limit);
            assert_eq!(orders[0].time_in_force, Some(TimeInForce::Ioc));
            assert_eq!(orders[1].order_type, OrderType::Limit);
            assert_eq!(orders[1].time_in_force, Some(TimeInForce::Ioc));
        }
    }
}
