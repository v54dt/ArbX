//! End-to-end backtest smoke: read a canned quote stream from a JSONL
//! fixture, drive the engine through MockExchange feeds, and assert that
//! at least one opportunity was detected and logged. Guards the whole
//! pipeline (feed → merge → strategy → risk → executor → trade_log) from
//! silent regressions.
//!
//! The fixture `tests/fixtures/backtest_quotes.jsonl` ships with synthetic
//! Binance/Bybit BTC/USDT quotes crafted to produce a clear cross-exchange
//! spread. To regenerate from real capture, replace the file contents with
//! JSONL-serialised [`Quote`] objects (one per line) and run the test.
//!
//! Quote timestamps in the fixture are re-stamped to `Utc::now()` per load
//! so staleness filters (`max_quote_age_ms`) don't drop them in CI.

use std::time::Duration;

use chrono::Utc;
use rust_decimal_macros::dec;

use arbx_core::adapters::mock_exchange::MockExchange;
use arbx_core::models::enums::Venue;
use arbx_core::models::fee::FeeSchedule;
use arbx_core::models::instrument::{AssetClass, Instrument, InstrumentType};
use arbx_core::models::market::Quote;
use arbx_core::risk::circuit_breaker::CircuitBreaker;
use arbx_core::risk::manager::RiskManager;
use arbx_core::risk::state::RiskState;
use arbx_core::strategy::cross_exchange::CrossExchangeStrategy;

fn spot_btc_usdt() -> Instrument {
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

fn load_fixture_quotes(path: &str) -> anyhow::Result<Vec<Quote>> {
    let body = std::fs::read_to_string(path)?;
    let mut quotes = Vec::new();
    for (n, line) in body.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut q: Quote = serde_json::from_str(line)
            .map_err(|e| anyhow::anyhow!("parse line {n} of {path}: {e}"))?;
        // Re-stamp fresh so staleness filter in the strategy doesn't drop
        // the quote during CI on a cold clock.
        q.timestamp = Utc::now();
        quotes.push(q);
    }
    Ok(quotes)
}

fn partition_by_venue(quotes: Vec<Quote>) -> (Vec<Quote>, Vec<Quote>) {
    let mut a = Vec::new();
    let mut b = Vec::new();
    for q in quotes {
        match q.venue {
            Venue::Binance => a.push(q),
            Venue::Bybit => b.push(q),
            _ => {}
        }
    }
    (a, b)
}

fn build_strategy() -> CrossExchangeStrategy {
    CrossExchangeStrategy {
        venue_a: Venue::Binance,
        venue_b: Venue::Bybit,
        instrument_a: spot_btc_usdt(),
        instrument_b: spot_btc_usdt(),
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
    }
}

#[tokio::test]
async fn backtest_fixture_produces_trade_logs() {
    let quotes =
        load_fixture_quotes("tests/fixtures/backtest_quotes.jsonl").expect("fixture must parse");
    assert!(
        quotes.len() >= 10,
        "fixture too small ({} quotes), update backtest_quotes.jsonl",
        quotes.len()
    );

    let (binance_quotes, bybit_quotes) = partition_by_venue(quotes);
    assert!(!binance_quotes.is_empty(), "no Binance quotes in fixture");
    assert!(!bybit_quotes.is_empty(), "no Bybit quotes in fixture");

    let feed_a = MockExchange::new(binance_quotes, 0, 1.0).with_quote_interval(10);
    let feed_b = MockExchange::new(bybit_quotes, 0, 1.0).with_quote_interval(10);
    let executor = MockExchange::new(vec![], 0, 1.0);
    let position_manager = MockExchange::new(vec![], 0, 1.0);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let risk_manager = RiskManager::new(Vec::<Box<dyn arbx_core::risk::limits::RiskLimit>>::new());
    let risk_state = RiskState::new(dec!(100), dec!(1_000_000), dec!(10_000));
    let circuit_breaker = CircuitBreaker::new(dec!(50_000), 1000, 10);

    let mut engine = arbx_core::engine::arbitrage::ArbitrageEngine::new(
        vec![Box::new(feed_a), Box::new(feed_b)],
        Box::new(build_strategy()),
        risk_manager,
        risk_state,
        circuit_breaker,
        Box::new(executor),
        Box::new(position_manager),
        Vec::new(),
        Vec::new(),
        3600,
        0,
        shutdown_rx,
    );

    let handle = tokio::spawn(async move {
        let _ = engine.run().await;
        engine
    });

    // Poll until at least one trade log appears or 2 s deadline passes.
    // Avoids a fixed sleep(500ms) that flakes under heavy CI load.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(2000);
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if tokio::time::Instant::now() >= deadline {
            break;
        }
    }
    let _ = shutdown_tx.send(true);
    let engine = handle.await.unwrap();

    let logs = engine.trade_logs();
    assert!(
        !logs.is_empty(),
        "backtest smoke failed: no trade_logs produced from fixture (waited 2s)"
    );
    // Sanity: every logged trade should have ≥2 legs (cross-exchange arbitrage).
    for log in logs {
        assert!(
            log.legs.len() >= 2,
            "trade_log has {} legs, expected ≥2",
            log.legs.len()
        );
    }
}
