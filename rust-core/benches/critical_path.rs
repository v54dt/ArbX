use criterion::{Criterion, black_box, criterion_group, criterion_main};

use std::collections::HashMap;

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
            rt.block_on(strategy.evaluate(black_box(&books), black_box(&portfolios)))
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
);
criterion_main!(benches);
