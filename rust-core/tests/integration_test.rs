use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::mpsc;

use arbx_core::adapters::mock_exchange::MockExchange;
use arbx_core::adapters::private_stream::{PrivateStream, PrivateStreamReceivers};
use arbx_core::models::enums::{Side, Venue};
use arbx_core::models::fee::FeeSchedule;
use arbx_core::models::instrument::{AssetClass, Instrument, InstrumentType};
use arbx_core::models::market::Quote;
use arbx_core::models::order::Fill;
use arbx_core::models::trade_log::TradeOutcome;
use arbx_core::risk::circuit_breaker::CircuitBreaker;
use arbx_core::risk::limits::MaxPositionSize;
use arbx_core::risk::manager::RiskManager;
use arbx_core::risk::state::RiskState;
use arbx_core::strategy::cross_exchange::CrossExchangeStrategy;

fn spot_instrument() -> Instrument {
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

fn make_quote(venue: Venue, bid: Decimal, ask: Decimal) -> Quote {
    Quote {
        venue,
        instrument: spot_instrument(),
        bid,
        ask,
        bid_size: dec!(10),
        ask_size: dec!(10),
        timestamp: Utc::now(),
    }
}

fn profitable_quotes() -> (Vec<Quote>, Vec<Quote>) {
    let spot_quotes: Vec<Quote> = (0..5)
        .map(|_| make_quote(Venue::Binance, dec!(49900), dec!(50000)))
        .collect();
    let perp_quotes: Vec<Quote> = (0..5)
        .map(|_| make_quote(Venue::Bybit, dec!(50200), dec!(50300)))
        .collect();
    (spot_quotes, perp_quotes)
}

fn build_strategy() -> CrossExchangeStrategy {
    CrossExchangeStrategy {
        venue_a: Venue::Binance,
        venue_b: Venue::Bybit,
        instrument_a: spot_instrument(),
        instrument_b: spot_instrument(),
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

#[allow(clippy::too_many_arguments)]
fn build_engine(
    feed_a: MockExchange,
    feed_b: MockExchange,
    executor: MockExchange,
    position_manager: MockExchange,
    strategy: CrossExchangeStrategy,
    risk_limits: Vec<Box<dyn arbx_core::risk::limits::RiskLimit>>,
    max_position_size: Decimal,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> arbx_core::engine::arbitrage::ArbitrageEngine {
    let risk_manager = RiskManager::new(risk_limits);
    let risk_state = RiskState::new(max_position_size, dec!(1000000), dec!(10000));
    let circuit_breaker = CircuitBreaker::new(dec!(50000), 1000, 10);

    arbx_core::engine::arbitrage::ArbitrageEngine::new(
        vec![Box::new(feed_a), Box::new(feed_b)],
        Box::new(strategy),
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
    )
}

#[tokio::test]
async fn engine_detects_opportunity_and_submits_orders() {
    let (spot_quotes, perp_quotes) = profitable_quotes();

    let feed_a = MockExchange::new(spot_quotes, 0, 1.0).with_quote_interval(10);
    let feed_b = MockExchange::new(perp_quotes, 0, 1.0).with_quote_interval(10);
    let executor = MockExchange::new(vec![], 0, 1.0);
    let position_manager = MockExchange::new(vec![], 0, 1.0);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut engine = build_engine(
        feed_a,
        feed_b,
        executor,
        position_manager,
        build_strategy(),
        vec![],
        dec!(100),
        shutdown_rx,
    );

    let handle = tokio::spawn(async move {
        let _ = engine.run().await;
        engine
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);

    let engine = handle.await.unwrap();
    let logs = engine.trade_logs();
    assert!(!logs.is_empty(), "expected at least one trade log");
    assert_eq!(logs[0].outcome, TradeOutcome::AllSubmitted);
    assert!(logs[0].legs.len() >= 2);
    for leg in logs[0].legs.iter() {
        assert!(leg.order_id.is_some(), "expected order_id on submitted leg");
    }
}

#[tokio::test]
async fn engine_respects_risk_limits() {
    let (spot_quotes, perp_quotes) = profitable_quotes();

    let feed_a = MockExchange::new(spot_quotes, 0, 1.0).with_quote_interval(10);
    let feed_b = MockExchange::new(perp_quotes, 0, 1.0).with_quote_interval(10);
    let executor = MockExchange::new(vec![], 0, 1.0);
    let position_manager = MockExchange::new(vec![], 0, 1.0);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let tight_limit: Vec<Box<dyn arbx_core::risk::limits::RiskLimit>> =
        vec![Box::new(MaxPositionSize {
            max_quantity: dec!(0),
        })];

    let mut engine = build_engine(
        feed_a,
        feed_b,
        executor,
        position_manager,
        build_strategy(),
        tight_limit,
        dec!(0),
        shutdown_rx,
    );

    let handle = tokio::spawn(async move {
        let _ = engine.run().await;
        engine
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);

    let engine = handle.await.unwrap();
    let logs = engine.trade_logs();
    assert!(!logs.is_empty(), "expected trade logs even when rejected");
    for log in logs {
        assert_eq!(
            log.outcome,
            TradeOutcome::RiskRejected,
            "all trades should be risk-rejected"
        );
    }
}

struct PrecannedPrivateStream {
    fill: Option<Fill>,
    extra_fills: Vec<Fill>,
}

impl PrecannedPrivateStream {
    fn single(fill: Fill) -> Self {
        Self {
            fill: Some(fill),
            extra_fills: Vec::new(),
        }
    }

    fn many(fills: Vec<Fill>) -> Self {
        Self {
            fill: None,
            extra_fills: fills,
        }
    }
}

#[async_trait]
impl PrivateStream for PrecannedPrivateStream {
    async fn connect(&mut self) -> anyhow::Result<PrivateStreamReceivers> {
        let (fill_tx, fill_rx) = mpsc::unbounded_channel();
        let (_updates_tx, order_rx) = mpsc::unbounded_channel();
        let mut fills: Vec<Fill> = self.fill.take().into_iter().collect();
        fills.extend(std::mem::take(&mut self.extra_fills));
        if !fills.is_empty() {
            tokio::spawn(async move {
                for f in fills {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _ = fill_tx.send(f);
                }
            });
        }
        Ok(PrivateStreamReceivers {
            fills: fill_rx,
            order_updates: order_rx,
        })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn private_stream_fill_updates_position_manager() {
    let (spot_quotes, perp_quotes) = profitable_quotes();

    let feed_a = MockExchange::new(spot_quotes, 0, 1.0).with_quote_interval(50);
    let feed_b = MockExchange::new(perp_quotes, 0, 1.0).with_quote_interval(50);
    let executor = MockExchange::new(vec![], 0, 0.0);
    let position_manager = MockExchange::new(vec![], 0, 1.0);
    let positions_handle = position_manager.positions_handle();

    let fill = Fill {
        order_id: "external-exchange-fill-1".into(),
        venue: Venue::Binance,
        instrument: spot_instrument(),
        side: Side::Buy,
        price: dec!(50000),
        quantity: dec!(0.123),
        fee: Decimal::ZERO,
        fee_currency: "USDT".into(),
        filled_at: Utc::now(),
    };
    let private_stream: Box<dyn PrivateStream> =
        Box::new(PrecannedPrivateStream::single(fill.clone()));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let risk_manager = RiskManager::new(Vec::<Box<dyn arbx_core::risk::limits::RiskLimit>>::new());
    let risk_state = RiskState::new(dec!(100), dec!(1_000_000), dec!(10_000));
    let circuit_breaker = CircuitBreaker::new(dec!(50000), 1000, 10);

    let mut engine = arbx_core::engine::arbitrage::ArbitrageEngine::new(
        vec![Box::new(feed_a), Box::new(feed_b)],
        Box::new(build_strategy()),
        risk_manager,
        risk_state,
        circuit_breaker,
        Box::new(executor),
        Box::new(position_manager),
        vec![private_stream],
        Vec::new(),
        3600,
        0,
        shutdown_rx,
    );

    let handle = tokio::spawn(async move { engine.run().await });

    tokio::time::sleep(Duration::from_millis(400)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    let positions = positions_handle.lock().await;
    let key = format!("{:?}:{}", fill.venue, fill.instrument.base).to_lowercase();
    let pos = positions
        .get(&key)
        .expect("private stream fill should have triggered apply_fill on position manager");
    assert_eq!(pos.quantity, fill.quantity);
    assert_eq!(pos.average_cost, fill.price);
}

#[tokio::test]
async fn engine_handles_graceful_shutdown() {
    let (spot_quotes, perp_quotes) = profitable_quotes();

    let feed_a = MockExchange::new(spot_quotes, 0, 1.0).with_quote_interval(50);
    let feed_b = MockExchange::new(perp_quotes, 0, 1.0).with_quote_interval(50);
    let executor = MockExchange::new(vec![], 0, 1.0);
    let position_manager = MockExchange::new(vec![], 0, 1.0);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut engine = build_engine(
        feed_a,
        feed_b,
        executor,
        position_manager,
        build_strategy(),
        vec![],
        dec!(100),
        shutdown_rx,
    );

    let handle = tokio::spawn(async move {
        let result = engine.run().await;
        (engine, result)
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(true);

    let (engine, result) = handle.await.unwrap();
    assert!(result.is_ok(), "engine should exit cleanly");
    let _ = engine.trade_logs();
}

#[tokio::test]
async fn duplicate_fill_is_deduplicated_by_fingerprint() {
    // The same Fill (identical order_id + filled_at + price + qty) should only
    // apply to positions once — regression guard for WS reconnect replay.
    let (spot_quotes, perp_quotes) = profitable_quotes();

    let feed_a = MockExchange::new(spot_quotes, 0, 1.0).with_quote_interval(50);
    let feed_b = MockExchange::new(perp_quotes, 0, 1.0).with_quote_interval(50);
    let executor = MockExchange::new(vec![], 0, 0.0);
    let position_manager = MockExchange::new(vec![], 0, 1.0);
    let positions_handle = position_manager.positions_handle();

    let same_ts = Utc::now();
    let fill = Fill {
        order_id: "dup-order-1".into(),
        venue: Venue::Binance,
        instrument: spot_instrument(),
        side: Side::Buy,
        price: dec!(50000),
        quantity: dec!(0.2),
        fee: Decimal::ZERO,
        fee_currency: "USDT".into(),
        filled_at: same_ts,
    };
    // Same fingerprint — fill arrives twice (e.g., WS reconnect replays history).
    let private_stream: Box<dyn PrivateStream> = Box::new(PrecannedPrivateStream::many(vec![
        fill.clone(),
        fill.clone(),
    ]));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let risk_manager = RiskManager::new(Vec::<Box<dyn arbx_core::risk::limits::RiskLimit>>::new());
    let risk_state = RiskState::new(dec!(100), dec!(1_000_000), dec!(10_000));
    let circuit_breaker = CircuitBreaker::new(dec!(50000), 1000, 10);

    let mut engine = arbx_core::engine::arbitrage::ArbitrageEngine::new(
        vec![Box::new(feed_a), Box::new(feed_b)],
        Box::new(build_strategy()),
        risk_manager,
        risk_state,
        circuit_breaker,
        Box::new(executor),
        Box::new(position_manager),
        vec![private_stream],
        Vec::new(),
        3600,
        0,
        shutdown_rx,
    );

    let handle = tokio::spawn(async move { engine.run().await });

    tokio::time::sleep(Duration::from_millis(400)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    let positions = positions_handle.lock().await;
    let key = format!("{:?}:{}", fill.venue, fill.instrument.base).to_lowercase();
    let pos = positions
        .get(&key)
        .expect("apply_fill must have fired once");
    // Quantity equals the single fill, not 2× — dedup held.
    assert_eq!(pos.quantity, fill.quantity);
}

#[tokio::test]
async fn unfilled_orders_are_cancelled_after_ttl() {
    // Engine with order_ttl_secs=1 and executor fill_rate=0 (orders submit but
    // never fill). After the TTL elapses + a 500ms cancel tick, the engine
    // should call executor.cancel_order for each live order.
    let (spot_quotes, perp_quotes) = profitable_quotes();

    let feed_a = MockExchange::new(spot_quotes, 0, 1.0).with_quote_interval(50);
    let feed_b = MockExchange::new(perp_quotes, 0, 1.0).with_quote_interval(50);
    let executor = MockExchange::new(vec![], 0, 0.0); // fill_rate=0 → no fills
    let cancels_handle = executor.cancels_handle();
    let position_manager = MockExchange::new(vec![], 0, 1.0);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let risk_manager = RiskManager::new(Vec::<Box<dyn arbx_core::risk::limits::RiskLimit>>::new());
    let risk_state = RiskState::new(dec!(100), dec!(1_000_000), dec!(10_000));
    let circuit_breaker = CircuitBreaker::new(dec!(50000), 1000, 10);

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
        1, // order_ttl_secs = 1
        shutdown_rx,
    );

    let handle = tokio::spawn(async move { engine.run().await });

    // Orders go out within ~200ms; TTL elapses at +1s; cancel_check ticks at +500ms
    // — so ~2s is enough margin to see the cancel fire.
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    let cancels = cancels_handle.lock().await;
    assert!(
        !cancels.is_empty(),
        "expected at least one cancel_order call after TTL (got 0)"
    );
    for id in cancels.iter() {
        assert!(
            id.starts_with("mock-"),
            "cancel id should be a mock order id, got '{id}'"
        );
    }
}
