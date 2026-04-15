//! Python → Rust FlatBuffers cross-compatibility guard.
//!
//! The fixture `tests/fixtures/python_quote.bin` is produced by
//! `tools/gen_flatbuf_fixtures.py` using `python-sidecar/src/ipc/flatbuf_codec.py`.
//! This test reads that fixture, strips the 1-byte MSG_TAG_QUOTE prefix
//! (matching `AeronMarketDataFeed`'s demux), and decodes via the Rust
//! `decode_quote`. It fails if the two encoders diverge on any field.
//!
//! Regenerate after touching either codec:
//!     python-sidecar/.venv/bin/python tools/gen_flatbuf_fixtures.py

use std::path::PathBuf;

use arbx_core::ipc::flatbuf_codec::{MSG_TAG_QUOTE, decode_quote};
use arbx_core::models::enums::Venue;
use arbx_core::models::instrument::InstrumentType;
use rust_decimal_macros::dec;

fn fixture_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p.push(name);
    p
}

#[test]
fn python_encoded_quote_decodes_in_rust() {
    let bytes = std::fs::read(fixture_path("python_quote.bin"))
        .expect("fixture python_quote.bin — regenerate via tools/gen_flatbuf_fixtures.py");

    // Aeron wire format: first byte is the MSG_TAG_* discriminator.
    assert!(!bytes.is_empty(), "fixture is empty");
    assert_eq!(bytes[0], MSG_TAG_QUOTE, "first byte must be MSG_TAG_QUOTE");

    let quote = decode_quote(&bytes[1..]).expect("decode");

    // Must match the Python sample_quote() in tools/gen_flatbuf_fixtures.py
    assert_eq!(quote.venue, Venue::Binance);
    assert_eq!(quote.instrument.base, "BTC");
    assert_eq!(quote.instrument.quote, "USDT");
    assert_eq!(quote.instrument.instrument_type, InstrumentType::Spot);
    assert_eq!(quote.bid, dec!(50000.5));
    assert_eq!(quote.ask, dec!(50010.25));
    assert_eq!(quote.bid_size, dec!(1.5));
    assert_eq!(quote.ask_size, dec!(2));
    assert_eq!(quote.timestamp.timestamp_millis(), 1_700_000_000_000);
}
