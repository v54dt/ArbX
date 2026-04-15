//! Loopback test: publish tagged Quote/OrderBook bytes via AeronPublisher,
//! verify AeronMarketDataFeed routes them to the right receiver channel.
//! Run manually with: `cargo test --test aeron_loopback_feed -- --ignored`.

use std::time::Duration;

use arbx_core::adapters::aeron_feed::AeronMarketDataFeed;
use arbx_core::adapters::market_data::MarketDataFeed;
use arbx_core::ipc::IpcPublisher;
use arbx_core::ipc::aeron::AeronPublisher;
use arbx_core::ipc::flatbuf_codec::{
    MSG_TAG_ORDER_BOOK, MSG_TAG_QUOTE, encode_order_book, encode_quote,
};
use arbx_core::models::enums::Venue;
use arbx_core::models::instrument::{AssetClass, Instrument, InstrumentType};
use arbx_core::models::market::{OrderBook, OrderBookLevel, Quote};
use chrono::Utc;
use rust_decimal_macros::dec;

const STREAM_ID: i32 = 9998;

fn instrument() -> Instrument {
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

fn tagged(tag: u8, payload: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 1);
    out.push(tag);
    out.extend_from_slice(&payload);
    out
}

#[tokio::test]
#[ignore = "requires a running Aeron Media Driver"]
async fn aeron_loopback_routes_quote_and_book_to_correct_channel() {
    let publisher = AeronPublisher::new(STREAM_ID).expect("AeronPublisher");
    let mut feed = AeronMarketDataFeed::new(STREAM_ID);
    let mut receivers = feed.connect().await.expect("connect feed");

    let quote = Quote {
        venue: Venue::Binance,
        instrument: instrument(),
        bid: dec!(50000),
        ask: dec!(50010),
        bid_size: dec!(1.5),
        ask_size: dec!(2.0),
        timestamp: Utc::now(),
    };
    let book = OrderBook {
        venue: Venue::Binance,
        instrument: instrument(),
        bids: smallvec::smallvec![OrderBookLevel {
            price: dec!(49999),
            size: dec!(0.5)
        }],
        asks: smallvec::smallvec![OrderBookLevel {
            price: dec!(50011),
            size: dec!(0.7)
        }],
        timestamp: Utc::now(),
        local_timestamp: Utc::now(),
    };

    publisher
        .publish(&tagged(MSG_TAG_QUOTE, encode_quote(&quote)))
        .await
        .unwrap();
    publisher
        .publish(&tagged(MSG_TAG_ORDER_BOOK, encode_order_book(&book)))
        .await
        .unwrap();

    let q_received = tokio::time::timeout(Duration::from_secs(3), receivers.quotes.recv())
        .await
        .expect("quote receive timeout")
        .expect("quote channel closed");
    assert_eq!(q_received.bid, dec!(50000));

    let b_received = tokio::time::timeout(Duration::from_secs(3), receivers.order_books.recv())
        .await
        .expect("book receive timeout")
        .expect("book channel closed");
    assert_eq!(b_received.bids.len(), 1);
    assert_eq!(b_received.bids[0].price, dec!(49999));

    feed.disconnect().await.unwrap();
}
