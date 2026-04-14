//! Run manually with a Media Driver: `cargo test --test aeron_ipc_roundtrip -- --ignored`.
//! Start a driver via `aeronmd` (C), Aeron Cookbook, or a one-shot container
//! (`docker run --rm -p 40123:40123 shazam42/aeron-driver`).

use std::time::Duration;

use arbx_core::ipc::aeron::{AeronPublisher, AeronSubscriber};
use arbx_core::ipc::flatbuf_codec::{decode_quote, encode_quote};
use arbx_core::ipc::{IpcPublisher, IpcSubscriber};
use arbx_core::models::enums::Venue;
use arbx_core::models::instrument::{AssetClass, Instrument, InstrumentType};
use arbx_core::models::market::Quote;
use chrono::Utc;
use rust_decimal_macros::dec;

fn test_quote() -> Quote {
    Quote {
        venue: Venue::Binance,
        instrument: Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        },
        bid: dec!(50000),
        ask: dec!(50010),
        bid_size: dec!(1.5),
        ask_size: dec!(2.0),
        timestamp: Utc::now(),
    }
}

#[tokio::test]
#[ignore = "requires a running Aeron Media Driver; run with --ignored"]
async fn aeron_ipc_roundtrip_quote() {
    const STREAM_ID: i32 = 9999;

    let publisher = AeronPublisher::new(STREAM_ID).expect("construct AeronPublisher");
    let mut subscriber = AeronSubscriber::new(STREAM_ID).expect("construct AeronSubscriber");

    let quote = test_quote();
    let encoded = encode_quote(&quote);
    publisher.publish(&encoded).await.expect("publish quote");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let received = loop {
        if let Some(bytes) = subscriber.poll().await.expect("poll subscriber") {
            break bytes;
        }
        if std::time::Instant::now() > deadline {
            panic!("timed out waiting for subscriber to receive Quote");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let decoded = decode_quote(&received).expect("decode FlatBuffers Quote");
    assert_eq!(decoded.venue, quote.venue);
    assert_eq!(decoded.bid, quote.bid);
    assert_eq!(decoded.ask, quote.ask);
    assert_eq!(decoded.instrument.base, "BTC");
    assert_eq!(decoded.instrument.quote, "USDT");
}
