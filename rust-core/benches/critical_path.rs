use criterion::{Criterion, black_box, criterion_group, criterion_main};

use std::collections::{HashMap, HashSet, VecDeque};

use arbx_core::ipc::flatbuf_codec::{decode_quote, encode_quote};
use arbx_core::models::enums::{OrderType, Side, Venue};
use arbx_core::models::fee::FeeSchedule;
use arbx_core::models::instrument::{AssetClass, Instrument, InstrumentType};
use arbx_core::models::market::{BookMap, OrderBook, OrderBookLevel, Quote, book_key};
use arbx_core::models::order::OrderRequest;
use arbx_core::models::position::PortfolioSnapshot;
use arbx_core::risk::limits::{MaxDailyLoss, MaxNotionalExposure, MaxPositionSize};
use arbx_core::risk::manager::RiskManager;
use arbx_core::strategy::base::ArbitrageStrategy;
use arbx_core::strategy::cross_exchange::CrossExchangeStrategy;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn make_instrument(inst_type: InstrumentType) -> Instrument {
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

fn make_orderbook(venue: Venue, instrument: &Instrument, bid: Decimal, ask: Decimal) -> OrderBook {
    let now = Utc::now();
    OrderBook {
        venue,
        instrument: instrument.clone(),
        bids: smallvec::smallvec![OrderBookLevel {
            price: bid,
            size: dec!(1),
        }],
        asks: smallvec::smallvec![OrderBookLevel {
            price: ask,
            size: dec!(1),
        }],
        timestamp: now,
        local_timestamp: now,
    }
}

fn make_quote() -> Quote {
    Quote {
        venue: Venue::Binance,
        instrument: make_instrument(InstrumentType::Spot),
        bid: dec!(50000),
        ask: dec!(50010),
        bid_size: dec!(1.5),
        ask_size: dec!(2.0),
        timestamp: Utc::now(),
    }
}

fn bench_strategy_evaluate(c: &mut Criterion) {
    let inst_a = make_instrument(InstrumentType::Spot);
    let inst_b = make_instrument(InstrumentType::Spot);
    let strategy = CrossExchangeStrategy {
        venue_a: Venue::Binance,
        venue_b: Venue::Bybit,
        instrument_a: inst_a.clone(),
        instrument_b: inst_b.clone(),
        min_net_profit_bps: dec!(1),
        max_quantity: dec!(1),
        fee_a: FeeSchedule::new(Venue::Binance, dec!(0.001), dec!(0.001)),
        fee_b: FeeSchedule::new(Venue::Bybit, dec!(0.001), dec!(0.001)),
        max_quote_age_ms: 5000,
        tick_size_a: dec!(0.01),
        tick_size_b: dec!(0.01),
        lot_size_a: dec!(0.001),
        lot_size_b: dec!(0.001),
        max_book_depth: 10,
    };

    let book_a = make_orderbook(Venue::Binance, &inst_a, dec!(99), dec!(100));
    let book_b = make_orderbook(Venue::Bybit, &inst_b, dec!(102), dec!(103));
    let mut books = BookMap::default();
    books.insert(book_key(Venue::Binance, &inst_a), book_a);
    books.insert(book_key(Venue::Bybit, &inst_b), book_b);
    let portfolios: HashMap<String, PortfolioSnapshot> = HashMap::new();

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("strategy_evaluate", |b| {
        b.iter(|| {
            let mut books = books.clone();
            for book in books.values_mut() {
                let now = Utc::now();
                book.timestamp = now;
                book.local_timestamp = now;
            }
            rt.block_on(strategy.evaluate(black_box(&books), black_box(&portfolios), Utc::now()))
        })
    });
}

fn bench_risk_check(c: &mut Criterion) {
    let limits: Vec<Box<dyn arbx_core::risk::limits::RiskLimit>> = vec![
        Box::new(MaxPositionSize {
            max_quantity: dec!(10),
        }),
        Box::new(MaxDailyLoss {
            max_loss: dec!(1000),
        }),
        Box::new(MaxNotionalExposure {
            max_notional: dec!(100000),
        }),
    ];
    let manager = RiskManager::new(limits);

    let order = OrderRequest {
        venue: Venue::Binance,
        instrument: make_instrument(InstrumentType::Spot),
        side: Side::Buy,
        order_type: OrderType::Limit,
        time_in_force: None,
        price: Some(dec!(50000)),
        quantity: dec!(1),
    };

    let portfolio = PortfolioSnapshot {
        venue: Venue::Binance,
        positions: vec![],
        total_equity: dec!(100000),
        available_balance: dec!(50000),
        unrealized_pnl: Decimal::ZERO,
        realized_pnl: Decimal::ZERO,
    };

    c.bench_function("risk_check_pre_trade", |b| {
        b.iter(|| manager.check_pre_trade(black_box(&order), black_box(&portfolio)))
    });
}

fn bench_flatbuf_encode_quote(c: &mut Criterion) {
    let quote = make_quote();

    c.bench_function("flatbuf_encode_quote", |b| {
        b.iter(|| encode_quote(black_box(&quote)))
    });
}

fn bench_flatbuf_decode_quote(c: &mut Criterion) {
    let quote = make_quote();
    let bytes = encode_quote(&quote);

    c.bench_function("flatbuf_decode_quote", |b| {
        b.iter(|| decode_quote(black_box(&bytes)))
    });
}

// Skipped: quote_to_book is pub(crate), not accessible from external benchmarks.

fn bench_book_key(c: &mut Criterion) {
    let instrument = make_instrument(InstrumentType::Spot);
    let venue = Venue::Binance;

    c.bench_function("book_key", |b| {
        b.iter(|| book_key(black_box(venue), black_box(&instrument)))
    });
}

fn bench_orderbook_mid_price(c: &mut Criterion) {
    let inst = make_instrument(InstrumentType::Spot);
    let book = make_orderbook(Venue::Binance, &inst, dec!(50000), dec!(50010));
    c.bench_function("orderbook_mid_price", |b| {
        b.iter(|| black_box(&book).mid_price())
    });
}

fn bench_orderbook_spread_bps(c: &mut Criterion) {
    let inst = make_instrument(InstrumentType::Spot);
    let book = make_orderbook(Venue::Binance, &inst, dec!(50000), dec!(50010));
    c.bench_function("orderbook_spread_bps", |b| {
        b.iter(|| black_box(&book).spread_bps())
    });
}

fn bench_bookmap_insert_lookup(c: &mut Criterion) {
    let inst = make_instrument(InstrumentType::Spot);
    let book = make_orderbook(Venue::Binance, &inst, dec!(50000), dec!(50010));
    let key = book_key(Venue::Binance, &inst);
    c.bench_function("bookmap_insert_lookup", |b| {
        b.iter(|| {
            let mut books = BookMap::default();
            books.insert(black_box(key), black_box(book.clone()));
            books.contains_key(&key)
        })
    });
}

const DEDUP_CAP: usize = 1024;

fn fill_fingerprint(order_id: &str, ts_ms: i64, price: Decimal, qty: Decimal) -> String {
    format!("{}:{}:{}:{}", order_id, ts_ms, price, qty)
}

fn bench_dedup_check(c: &mut Criterion) {
    // Mirrors engine's fill-dedup hot path: HashSet<String> + VecDeque<String>
    // at 1024 capacity, steady-state insert+evict.
    let mut seen_set: HashSet<String> = HashSet::with_capacity(DEDUP_CAP);
    let mut seen_queue: VecDeque<String> = VecDeque::with_capacity(DEDUP_CAP);
    for i in 0..DEDUP_CAP {
        let fp = fill_fingerprint(
            &format!("ord{}", i),
            1_700_000_000 + i as i64,
            dec!(50000),
            dec!(1),
        );
        seen_set.insert(fp.clone());
        seen_queue.push_back(fp);
    }

    let mut next_id: usize = DEDUP_CAP;
    c.bench_function("dedup_check_at_capacity", |b| {
        b.iter(|| {
            let fp = fill_fingerprint(
                black_box(&format!("ord{}", next_id)),
                black_box(1_700_000_000 + next_id as i64),
                black_box(dec!(50000)),
                black_box(dec!(1)),
            );
            let inserted = seen_set.insert(fp.clone());
            black_box(inserted);
            seen_queue.push_back(fp);
            while seen_queue.len() > DEDUP_CAP
                && let Some(old) = seen_queue.pop_front()
            {
                seen_set.remove(&old);
            }
            next_id += 1;
        })
    });
}

fn bench_dedup_check_duplicate(c: &mut Criterion) {
    // Measures the fast path where the incoming fill was already seen —
    // HashSet::insert returns false and we short-circuit.
    let mut seen_set: HashSet<String> = HashSet::with_capacity(DEDUP_CAP);
    seen_set.insert(fill_fingerprint(
        "ord0",
        1_700_000_000,
        dec!(50000),
        dec!(1),
    ));
    for i in 1..DEDUP_CAP {
        let fp = fill_fingerprint(
            &format!("ord{}", i),
            1_700_000_000 + i as i64,
            dec!(50000),
            dec!(1),
        );
        seen_set.insert(fp);
    }

    c.bench_function("dedup_check_duplicate", |b| {
        b.iter(|| {
            let fp = fill_fingerprint(
                black_box("ord0"),
                black_box(1_700_000_000),
                black_box(dec!(50000)),
                black_box(dec!(1)),
            );
            // Match engine hot path: insert(fp.clone()) returning false for a duplicate.
            black_box(seen_set.insert(fp))
        })
    });
}

fn bench_intended_fills_lookup(c: &mut Criterion) {
    // Mirrors engine's intended_fills HashMap<String, IntendedFill> used to
    // compute signed slippage on each fill. Size reflects roughly the number
    // of unacked orders — benchmark at 256 live entries.
    const LIVE: usize = 256;
    let keys: Vec<String> = (0..LIVE).map(|i| format!("ord{}", i)).collect();
    let mut map: HashMap<String, (Side, Decimal)> = HashMap::with_capacity(LIVE);
    for k in &keys {
        map.insert(k.clone(), (Side::Buy, dec!(50000)));
    }

    let mut idx: usize = 0;
    c.bench_function("intended_fills_remove", |b| {
        b.iter(|| {
            let key = &keys[idx % LIVE];
            if let Some(val) = map.remove(black_box(key)) {
                map.insert(key.clone(), val);
            }
            idx += 1;
        })
    });
}

/// E2E critical path: quote→book_update→strategy_evaluate→risk_check→compute_orders.
/// Measures the combined latency of the entire hot path the engine traverses
/// per quote, excluding network I/O (which dominates in production).
fn bench_e2e_quote_to_orders(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let instrument_spot = make_instrument(InstrumentType::Spot);
    let strategy = CrossExchangeStrategy {
        venue_a: Venue::Binance,
        venue_b: Venue::Bybit,
        instrument_a: instrument_spot.clone(),
        instrument_b: instrument_spot.clone(),
        min_net_profit_bps: dec!(1),
        max_quantity: dec!(1),
        fee_a: FeeSchedule::new(Venue::Binance, dec!(0.0001), dec!(0.0001)),
        fee_b: FeeSchedule::new(Venue::Bybit, dec!(0.0001), dec!(0.0001)),
        max_quote_age_ms: 5000,
        tick_size_a: dec!(0.01),
        tick_size_b: dec!(0.01),
        lot_size_a: dec!(0.001),
        lot_size_b: dec!(0.001),
        max_book_depth: 5,
    };

    let risk_limits: Vec<Box<dyn arbx_core::risk::limits::RiskLimit>> = vec![
        Box::new(MaxPositionSize {
            max_quantity: dec!(100),
        }),
        Box::new(MaxDailyLoss {
            max_loss: dec!(100000),
        }),
        Box::new(MaxNotionalExposure {
            max_notional: dec!(10000000),
        }),
    ];
    let risk_manager = RiskManager::new(risk_limits);

    let mut books = BookMap::default();
    books.insert(
        book_key(Venue::Binance, &instrument_spot),
        make_orderbook(Venue::Binance, &instrument_spot, dec!(49900), dec!(50000)),
    );
    books.insert(
        book_key(Venue::Bybit, &instrument_spot),
        make_orderbook(Venue::Bybit, &instrument_spot, dec!(50200), dec!(50300)),
    );

    let portfolios: HashMap<String, PortfolioSnapshot> = HashMap::new();

    c.bench_function("e2e_quote_to_orders", |b| {
        b.iter(|| {
            // 1. Update book from a fresh quote
            let quote = Quote {
                venue: Venue::Binance,
                instrument: instrument_spot.clone(),
                bid: dec!(49900),
                ask: dec!(50000),
                bid_size: dec!(10),
                ask_size: dec!(10),
                timestamp: Utc::now(),
            };
            let key = book_key(quote.venue, &quote.instrument);
            books
                .entry(key)
                .and_modify(|b| b.update_from_quote(&quote))
                .or_insert_with(|| {
                    make_orderbook(quote.venue, &quote.instrument, quote.bid, quote.ask)
                });

            // 2. Strategy evaluate
            let opp = rt.block_on(strategy.evaluate(&books, &portfolios, Utc::now()));

            // 3. Risk check + compute orders
            if let Some(ref opp) = opp {
                let orders = strategy.compute_hedge_orders(opp);
                for req in &orders {
                    let order = req.clone().into_order();
                    let portfolio = PortfolioSnapshot::default();
                    let _verdict = risk_manager.check_pre_trade(
                        &OrderRequest {
                            venue: order.venue,
                            instrument: order.instrument.clone(),
                            side: order.side,
                            order_type: order.order_type,
                            time_in_force: order.time_in_force,
                            price: order.price,
                            quantity: order.quantity,
                        },
                        &portfolio,
                    );
                }
            }

            black_box(opp)
        })
    });
}

criterion_group!(
    benches,
    bench_strategy_evaluate,
    bench_risk_check,
    bench_flatbuf_encode_quote,
    bench_flatbuf_decode_quote,
    bench_book_key,
    bench_orderbook_mid_price,
    bench_orderbook_spread_bps,
    bench_bookmap_insert_lookup,
    bench_dedup_check,
    bench_dedup_check_duplicate,
    bench_intended_fills_lookup,
    bench_e2e_quote_to_orders,
);
criterion_main!(benches);
