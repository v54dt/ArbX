//! Live Binance testnet smoke test — guarded by `#[ignore]` so `cargo test`
//! doesn't hit the network in CI. Run locally with:
//!
//!     BINANCE_TESTNET_API_KEY=...  BINANCE_TESTNET_API_SECRET=...  \
//!         cargo test --test binance_testnet_smoke -- --ignored --nocapture
//!
//! Obtain test credentials from <https://testnet.binance.vision/>.
//!
//! The test submits a conservative LIMIT BUY at a price far below market
//! (`$10` on BTCUSDT) so the order never fills, verifies the REST submit
//! path returns an `orderId`, then cancels the order. Testnet-only — no
//! production keys accepted.

use std::time::Duration;

use rust_decimal_macros::dec;

use arbx_core::adapters::binance::market_data::BinanceMarket;
use arbx_core::adapters::binance::order_executor::BinanceOrderExecutor;
use arbx_core::adapters::order_executor::OrderExecutor;
use arbx_core::models::enums::{OrderType, Side, TimeInForce, Venue};
use arbx_core::models::instrument::{AssetClass, Instrument, InstrumentType};
use arbx_core::models::order::Order;

fn testnet_creds() -> Option<(String, String)> {
    let key = std::env::var("BINANCE_TESTNET_API_KEY").ok()?;
    let secret = std::env::var("BINANCE_TESTNET_API_SECRET").ok()?;
    Some((key, secret))
}

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

/// Submits a LIMIT BUY at $10/BTC (far below any realistic market price) so
/// it rests in the book, then cancels it. Exercises the full REST submit +
/// cancel round-trip against the real testnet.
#[tokio::test]
#[ignore = "live Binance testnet; requires BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET"]
async fn binance_testnet_submit_then_cancel_roundtrip() {
    let (key, secret) = testnet_creds()
        .expect("set BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET to run this test");

    let mut executor = BinanceOrderExecutor::with_testnet(BinanceMarket::Spot, key, secret, true)
        .expect("executor construction");

    let _receivers = executor.connect().await.expect("executor connect");

    let order = Order {
        id: "smoketest-submit-1".into(),
        client_order_id: uuid::Uuid::new_v4().to_string(),
        venue: Venue::Binance,
        instrument: spot_btc_usdt(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(dec!(10)),
        quantity: dec!(0.001),
        time_in_force: Some(TimeInForce::Rod),
        created_at: chrono::Utc::now(),
    };

    let order_id = executor
        .submit_order(&order)
        .await
        .expect("testnet submit succeeded");
    assert!(!order_id.is_empty(), "submit must return non-empty orderId");
    eprintln!("testnet submit orderId = {order_id}");

    // Give the matching engine a moment to register the resting order before
    // we cancel it — observed flakiness under 200ms.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cancelled = executor
        .cancel_order(&order_id)
        .await
        .expect("testnet cancel succeeded");
    assert!(cancelled, "cancel_order must return true on success");
}
