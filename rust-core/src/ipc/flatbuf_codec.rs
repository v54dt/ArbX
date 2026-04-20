use std::str::FromStr as _;

use anyhow::{Context as _, bail};
use chrono::{TimeZone as _, Utc};
use flatbuffers::{FlatBufferBuilder, Follow, ForwardsUOffset, Table, Vector, WIPOffset};
use rust_decimal::Decimal;

use crate::models::enums::{
    OrderType as DomainOrderType, Side as DomainSide, TimeInForce as DomainTimeInForce,
    Venue as DomainVenue,
};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::market::{OrderBook, OrderBookLevel, Quote};
use crate::models::order::{Fill, OrderRequest};

/// 1-byte type tag prefix for IPC payloads — disambiguates which decoder to call.
/// FlatBuffer decoders cannot reject a wrong table type, so without a tag a
/// Quote payload would silently parse as a (garbage) OrderBook.
pub const MSG_TAG_QUOTE: u8 = 1;
pub const MSG_TAG_ORDER_BOOK: u8 = 2;
pub const MSG_TAG_FILL: u8 = 3;
pub const MSG_TAG_ORDER_REQUEST: u8 = 4;

// Must match schemas/messages.fbs.
mod vt {
    pub const QUOTE_VENUE: u16 = 4;
    pub const QUOTE_BASE: u16 = 6;
    pub const QUOTE_QUOTE_CURRENCY: u16 = 8;
    pub const QUOTE_INSTRUMENT_TYPE: u16 = 10;
    pub const QUOTE_BID: u16 = 12;
    pub const QUOTE_ASK: u16 = 14;
    pub const QUOTE_BID_SIZE: u16 = 16;
    pub const QUOTE_ASK_SIZE: u16 = 18;
    pub const QUOTE_TIMESTAMP_MS: u16 = 20;

    pub const OR_VENUE: u16 = 4;
    pub const OR_BASE: u16 = 6;
    pub const OR_QUOTE_CURRENCY: u16 = 8;
    pub const OR_INSTRUMENT_TYPE: u16 = 10;
    pub const OR_SIDE: u16 = 12;
    pub const OR_ORDER_TYPE: u16 = 14;
    pub const OR_TIME_IN_FORCE: u16 = 16;
    pub const OR_PRICE: u16 = 18;
    pub const OR_QUANTITY: u16 = 20;

    pub const FILL_ORDER_ID: u16 = 4;
    pub const FILL_VENUE: u16 = 6;
    pub const FILL_BASE: u16 = 8;
    pub const FILL_QUOTE_CURRENCY: u16 = 10;
    pub const FILL_SIDE: u16 = 12;
    pub const FILL_PRICE: u16 = 14;
    pub const FILL_QUANTITY: u16 = 16;
    pub const FILL_FEE: u16 = 18;
    pub const FILL_FEE_CURRENCY: u16 = 20;
    pub const FILL_TIMESTAMP_MS: u16 = 22;
    pub const FILL_INSTRUMENT_TYPE: u16 = 24;

    pub const OB_VENUE: u16 = 4;
    pub const OB_BASE: u16 = 6;
    pub const OB_QUOTE_CURRENCY: u16 = 8;
    pub const OB_INSTRUMENT_TYPE: u16 = 10;
    pub const OB_BID_PRICES: u16 = 12;
    pub const OB_BID_SIZES: u16 = 14;
    pub const OB_ASK_PRICES: u16 = 16;
    pub const OB_ASK_SIZES: u16 = 18;
    pub const OB_TIMESTAMP_MS: u16 = 20;
}

fn venue_to_i8(v: DomainVenue) -> i8 {
    match v {
        DomainVenue::Binance => 0,
        DomainVenue::Okx => 1,
        DomainVenue::Bybit => 2,
        DomainVenue::Shioaji => 3,
        DomainVenue::Fubon => 4,
    }
}

fn venue_from_i8(v: i8) -> anyhow::Result<DomainVenue> {
    match v {
        0 => Ok(DomainVenue::Binance),
        1 => Ok(DomainVenue::Okx),
        2 => Ok(DomainVenue::Bybit),
        3 => Ok(DomainVenue::Shioaji),
        4 => Ok(DomainVenue::Fubon),
        _ => bail!("unknown venue: {}", v),
    }
}

fn side_to_i8(s: DomainSide) -> i8 {
    match s {
        DomainSide::Buy => 0,
        DomainSide::Sell => 1,
    }
}

fn side_from_i8(v: i8) -> anyhow::Result<DomainSide> {
    match v {
        0 => Ok(DomainSide::Buy),
        1 => Ok(DomainSide::Sell),
        _ => bail!("unknown side: {}", v),
    }
}

fn order_type_to_i8(ot: DomainOrderType) -> i8 {
    match ot {
        DomainOrderType::Limit => 0,
        DomainOrderType::Market => 1,
    }
}

fn order_type_from_i8(v: i8) -> anyhow::Result<DomainOrderType> {
    match v {
        0 => Ok(DomainOrderType::Limit),
        1 => Ok(DomainOrderType::Market),
        _ => bail!("unknown order type: {}", v),
    }
}

fn tif_to_i8(tif: DomainTimeInForce) -> i8 {
    match tif {
        DomainTimeInForce::Rod => 0,
        DomainTimeInForce::Ioc => 1,
        DomainTimeInForce::Fok => 2,
    }
}

fn tif_from_i8(v: i8) -> anyhow::Result<DomainTimeInForce> {
    match v {
        0 => Ok(DomainTimeInForce::Rod),
        1 => Ok(DomainTimeInForce::Ioc),
        2 => Ok(DomainTimeInForce::Fok),
        _ => bail!("unknown time_in_force: {}", v),
    }
}

/// Decimals over IPC are encoded as their canonical string representation —
/// avoids the f64 round-trip precision loss (e.g. `dec!(0.1)` → `0.1f64` →
/// `Decimal::try_from(0.1)` would reintroduce floating noise).
fn dec_to_string(d: Decimal) -> String {
    d.to_string()
}

fn parse_decimal_str(s: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(s).context("string -> Decimal")
}

fn parse_optional_decimal(s: &str) -> anyhow::Result<Option<Decimal>> {
    if s.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parse_decimal_str(s)?))
    }
}

fn instrument_type_str(it: InstrumentType) -> &'static str {
    match it {
        InstrumentType::Spot => "spot",
        InstrumentType::Futures => "futures",
        InstrumentType::Option => "option",
        InstrumentType::Swap => "swap",
    }
}

fn parse_instrument_type(s: &str) -> anyhow::Result<InstrumentType> {
    match s {
        "spot" => Ok(InstrumentType::Spot),
        "futures" => Ok(InstrumentType::Futures),
        "option" => Ok(InstrumentType::Option),
        "swap" => Ok(InstrumentType::Swap),
        other => bail!("unknown instrument type: {}", other),
    }
}

fn make_instrument(
    base: &str,
    quote_currency: &str,
    instrument_type: &str,
) -> anyhow::Result<Instrument> {
    Ok(Instrument {
        asset_class: AssetClass::Crypto,
        instrument_type: parse_instrument_type(instrument_type)?,
        base: base.to_string(),
        quote: quote_currency.to_string(),
        settle_currency: None,
        expiry: None,
        last_trade_time: None,
        settlement_time: None,
    })
}

const MIN_FLATBUF_SIZE: usize = 8; // root offset (4) + vtable offset (4)

fn checked_root_as_table<'a>(data: &'a [u8], label: &str) -> anyhow::Result<Table<'a>> {
    if data.len() < MIN_FLATBUF_SIZE {
        bail!(
            "flatbuffer too short for {label}: {} bytes (min {MIN_FLATBUF_SIZE})",
            data.len()
        );
    }
    Ok(unsafe { flatbuffers::root_unchecked::<Table>(data) })
}

unsafe fn get_field<'a, T: Follow<'a> + 'a>(
    table: &Table<'a>,
    vt: u16,
    default: T::Inner,
) -> T::Inner
where
    T::Inner: PartialEq + Copy,
{
    unsafe { table.get::<T>(vt, Some(default)).unwrap_or(default) }
}

unsafe fn get_str<'a>(table: &Table<'a>, vt: u16) -> Option<&'a str> {
    unsafe { table.get::<ForwardsUOffset<&str>>(vt, None) }
}

pub fn encode_quote(quote: &Quote) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(256);
    let base = fbb.create_string(&quote.instrument.base);
    let quote_cur = fbb.create_string(&quote.instrument.quote);
    let inst_type = fbb.create_string(instrument_type_str(quote.instrument.instrument_type));
    let bid_s = fbb.create_string(&dec_to_string(quote.bid));
    let ask_s = fbb.create_string(&dec_to_string(quote.ask));
    let bid_size_s = fbb.create_string(&dec_to_string(quote.bid_size));
    let ask_size_s = fbb.create_string(&dec_to_string(quote.ask_size));

    let start = fbb.start_table();
    fbb.push_slot::<i8>(vt::QUOTE_VENUE, venue_to_i8(quote.venue), 0);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_BASE, base);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_QUOTE_CURRENCY, quote_cur);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_INSTRUMENT_TYPE, inst_type);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_BID, bid_s);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_ASK, ask_s);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_BID_SIZE, bid_size_s);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_ASK_SIZE, ask_size_s);
    fbb.push_slot::<i64>(
        vt::QUOTE_TIMESTAMP_MS,
        quote.timestamp.timestamp_millis(),
        0,
    );
    let root = fbb.end_table(start);
    fbb.finish(root, None);
    fbb.finished_data().to_vec()
}

pub fn decode_quote(data: &[u8]) -> anyhow::Result<Quote> {
    let table = checked_root_as_table(data, "quote")?;
    let venue_i8 = unsafe { get_field::<i8>(&table, vt::QUOTE_VENUE, 0) };
    let base = unsafe { get_str(&table, vt::QUOTE_BASE) }.unwrap_or("");
    let quote_cur = unsafe { get_str(&table, vt::QUOTE_QUOTE_CURRENCY) }.unwrap_or("");
    let inst_type = unsafe { get_str(&table, vt::QUOTE_INSTRUMENT_TYPE) }.unwrap_or("spot");
    let bid = unsafe { get_str(&table, vt::QUOTE_BID) }.unwrap_or("0");
    let ask = unsafe { get_str(&table, vt::QUOTE_ASK) }.unwrap_or("0");
    let bid_size = unsafe { get_str(&table, vt::QUOTE_BID_SIZE) }.unwrap_or("0");
    let ask_size = unsafe { get_str(&table, vt::QUOTE_ASK_SIZE) }.unwrap_or("0");
    let timestamp_ms = unsafe { get_field::<i64>(&table, vt::QUOTE_TIMESTAMP_MS, 0) };

    let ts = Utc
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .context("invalid timestamp")?;

    Ok(Quote {
        venue: venue_from_i8(venue_i8)?,
        instrument: make_instrument(base, quote_cur, inst_type)?,
        bid: parse_decimal_str(bid)?,
        ask: parse_decimal_str(ask)?,
        bid_size: parse_decimal_str(bid_size)?,
        ask_size: parse_decimal_str(ask_size)?,
        timestamp: ts,
    })
}

pub fn encode_order_request(req: &OrderRequest) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(256);
    let base = fbb.create_string(&req.instrument.base);
    let quote_cur = fbb.create_string(&req.instrument.quote);
    let inst_type = fbb.create_string(instrument_type_str(req.instrument.instrument_type));
    // Empty string means None for optional Decimal fields (price).
    let price_str = req.price.map(dec_to_string).unwrap_or_default();
    let price_off = fbb.create_string(&price_str);
    let qty_off = fbb.create_string(&dec_to_string(req.quantity));

    let start = fbb.start_table();
    fbb.push_slot::<i8>(vt::OR_VENUE, venue_to_i8(req.venue), 0);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OR_BASE, base);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OR_QUOTE_CURRENCY, quote_cur);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OR_INSTRUMENT_TYPE, inst_type);
    fbb.push_slot::<i8>(vt::OR_SIDE, side_to_i8(req.side), 0);
    fbb.push_slot::<i8>(vt::OR_ORDER_TYPE, order_type_to_i8(req.order_type), 0);
    fbb.push_slot::<i8>(
        vt::OR_TIME_IN_FORCE,
        req.time_in_force.map(tif_to_i8).unwrap_or(0),
        0,
    );
    fbb.push_slot_always::<WIPOffset<_>>(vt::OR_PRICE, price_off);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OR_QUANTITY, qty_off);
    let root = fbb.end_table(start);
    fbb.finish(root, None);
    fbb.finished_data().to_vec()
}

pub fn decode_order_request(data: &[u8]) -> anyhow::Result<OrderRequest> {
    let table = checked_root_as_table(data, "order_request")?;
    let venue_i8 = unsafe { get_field::<i8>(&table, vt::OR_VENUE, 0) };
    let base = unsafe { get_str(&table, vt::OR_BASE) }.unwrap_or("");
    let quote_cur = unsafe { get_str(&table, vt::OR_QUOTE_CURRENCY) }.unwrap_or("");
    let inst_type = unsafe { get_str(&table, vt::OR_INSTRUMENT_TYPE) }.unwrap_or("spot");
    let side_i8 = unsafe { get_field::<i8>(&table, vt::OR_SIDE, 0) };
    let ot_i8 = unsafe { get_field::<i8>(&table, vt::OR_ORDER_TYPE, 0) };
    let tif_i8 = unsafe { get_field::<i8>(&table, vt::OR_TIME_IN_FORCE, 0) };
    let price_str = unsafe { get_str(&table, vt::OR_PRICE) }.unwrap_or("");
    let qty_str = unsafe { get_str(&table, vt::OR_QUANTITY) }.unwrap_or("0");

    Ok(OrderRequest {
        venue: venue_from_i8(venue_i8)?,
        instrument: make_instrument(base, quote_cur, inst_type)?,
        side: side_from_i8(side_i8)?,
        order_type: order_type_from_i8(ot_i8)?,
        // Lossy: encode maps None → 0 (Rod), so decode always returns Some.
        time_in_force: Some(tif_from_i8(tif_i8)?),
        price: parse_optional_decimal(price_str)?,
        quantity: parse_decimal_str(qty_str)?,
        estimated_notional: None,
    })
}

pub fn encode_fill(fill: &Fill) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(256);
    let order_id = fbb.create_string(&fill.order_id);
    let base = fbb.create_string(&fill.instrument.base);
    let quote_cur = fbb.create_string(&fill.instrument.quote);
    let inst_type = fbb.create_string(instrument_type_str(fill.instrument.instrument_type));
    let fee_cur = fbb.create_string(&fill.fee_currency);
    let price_off = fbb.create_string(&dec_to_string(fill.price));
    let qty_off = fbb.create_string(&dec_to_string(fill.quantity));
    let fee_off = fbb.create_string(&dec_to_string(fill.fee));

    let start = fbb.start_table();
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_ORDER_ID, order_id);
    fbb.push_slot::<i8>(vt::FILL_VENUE, venue_to_i8(fill.venue), 0);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_BASE, base);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_QUOTE_CURRENCY, quote_cur);
    fbb.push_slot::<i8>(vt::FILL_SIDE, side_to_i8(fill.side), 0);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_PRICE, price_off);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_QUANTITY, qty_off);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_FEE, fee_off);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_FEE_CURRENCY, fee_cur);
    fbb.push_slot::<i64>(vt::FILL_TIMESTAMP_MS, fill.filled_at.timestamp_millis(), 0);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_INSTRUMENT_TYPE, inst_type);
    let root = fbb.end_table(start);
    fbb.finish(root, None);
    fbb.finished_data().to_vec()
}

pub fn decode_fill(data: &[u8]) -> anyhow::Result<Fill> {
    let table = checked_root_as_table(data, "fill")?;
    let order_id = unsafe { get_str(&table, vt::FILL_ORDER_ID) }.unwrap_or("");
    let venue_i8 = unsafe { get_field::<i8>(&table, vt::FILL_VENUE, 0) };
    let base = unsafe { get_str(&table, vt::FILL_BASE) }.unwrap_or("");
    let quote_cur = unsafe { get_str(&table, vt::FILL_QUOTE_CURRENCY) }.unwrap_or("");
    let inst_type = unsafe { get_str(&table, vt::FILL_INSTRUMENT_TYPE) }.unwrap_or("spot");
    let side_i8 = unsafe { get_field::<i8>(&table, vt::FILL_SIDE, 0) };
    let price_str = unsafe { get_str(&table, vt::FILL_PRICE) }.unwrap_or("0");
    let qty_str = unsafe { get_str(&table, vt::FILL_QUANTITY) }.unwrap_or("0");
    let fee_str = unsafe { get_str(&table, vt::FILL_FEE) }.unwrap_or("0");
    let fee_cur = unsafe { get_str(&table, vt::FILL_FEE_CURRENCY) }.unwrap_or("");
    let timestamp_ms = unsafe { get_field::<i64>(&table, vt::FILL_TIMESTAMP_MS, 0) };

    let ts = Utc
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .context("invalid timestamp")?;

    Ok(Fill {
        order_id: order_id.to_string(),
        client_order_id: None,
        venue: venue_from_i8(venue_i8)?,
        instrument: make_instrument(base, quote_cur, inst_type)?,
        side: side_from_i8(side_i8)?,
        price: parse_decimal_str(price_str)?,
        quantity: parse_decimal_str(qty_str)?,
        fee: parse_decimal_str(fee_str)?,
        fee_currency: fee_cur.to_string(),
        filled_at: ts,
    })
}

pub fn encode_order_book(book: &OrderBook) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(512);
    let base = fbb.create_string(&book.instrument.base);
    let quote_cur = fbb.create_string(&book.instrument.quote);
    let inst_type = fbb.create_string(instrument_type_str(book.instrument.instrument_type));

    let bid_price_strs: Vec<String> = book.bids.iter().map(|l| dec_to_string(l.price)).collect();
    let bid_size_strs: Vec<String> = book.bids.iter().map(|l| dec_to_string(l.size)).collect();
    let ask_price_strs: Vec<String> = book.asks.iter().map(|l| dec_to_string(l.price)).collect();
    let ask_size_strs: Vec<String> = book.asks.iter().map(|l| dec_to_string(l.size)).collect();

    // Inlined builds (the closure form trips the borrow checker on
    // FlatBufferBuilder's invariant lifetime).
    let bid_price_offsets: Vec<WIPOffset<&str>> = bid_price_strs
        .iter()
        .map(|s| fbb.create_string(s))
        .collect();
    let bid_prices_off = fbb.create_vector(&bid_price_offsets);
    let bid_size_offsets: Vec<WIPOffset<&str>> =
        bid_size_strs.iter().map(|s| fbb.create_string(s)).collect();
    let bid_sizes_off = fbb.create_vector(&bid_size_offsets);
    let ask_price_offsets: Vec<WIPOffset<&str>> = ask_price_strs
        .iter()
        .map(|s| fbb.create_string(s))
        .collect();
    let ask_prices_off = fbb.create_vector(&ask_price_offsets);
    let ask_size_offsets: Vec<WIPOffset<&str>> =
        ask_size_strs.iter().map(|s| fbb.create_string(s)).collect();
    let ask_sizes_off = fbb.create_vector(&ask_size_offsets);

    let start = fbb.start_table();
    fbb.push_slot::<i8>(vt::OB_VENUE, venue_to_i8(book.venue), 0);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OB_BASE, base);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OB_QUOTE_CURRENCY, quote_cur);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OB_INSTRUMENT_TYPE, inst_type);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OB_BID_PRICES, bid_prices_off);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OB_BID_SIZES, bid_sizes_off);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OB_ASK_PRICES, ask_prices_off);
    fbb.push_slot_always::<WIPOffset<_>>(vt::OB_ASK_SIZES, ask_sizes_off);
    fbb.push_slot::<i64>(vt::OB_TIMESTAMP_MS, book.timestamp.timestamp_millis(), 0);
    let root = fbb.end_table(start);
    fbb.finish(root, None);
    fbb.finished_data().to_vec()
}

pub fn decode_order_book(data: &[u8]) -> anyhow::Result<OrderBook> {
    let table = checked_root_as_table(data, "order_book")?;
    let venue_i8 = unsafe { get_field::<i8>(&table, vt::OB_VENUE, 0) };
    let base = unsafe { get_str(&table, vt::OB_BASE) }.unwrap_or("");
    let quote_cur = unsafe { get_str(&table, vt::OB_QUOTE_CURRENCY) }.unwrap_or("");
    let inst_type = unsafe { get_str(&table, vt::OB_INSTRUMENT_TYPE) }.unwrap_or("spot");
    let timestamp_ms = unsafe { get_field::<i64>(&table, vt::OB_TIMESTAMP_MS, 0) };

    let read_str_vec = |vt_offset: u16| -> Vec<String> {
        let opt: Option<Vector<'_, ForwardsUOffset<&str>>> =
            unsafe { table.get::<ForwardsUOffset<Vector<ForwardsUOffset<&str>>>>(vt_offset, None) };
        opt.map(|v| v.iter().map(|s| s.to_string()).collect())
            .unwrap_or_default()
    };
    let bid_prices = read_str_vec(vt::OB_BID_PRICES);
    let bid_sizes = read_str_vec(vt::OB_BID_SIZES);
    let ask_prices = read_str_vec(vt::OB_ASK_PRICES);
    let ask_sizes = read_str_vec(vt::OB_ASK_SIZES);

    if bid_prices.len() != bid_sizes.len() {
        bail!(
            "bid prices/sizes length mismatch: {} vs {}",
            bid_prices.len(),
            bid_sizes.len()
        );
    }
    if ask_prices.len() != ask_sizes.len() {
        bail!(
            "ask prices/sizes length mismatch: {} vs {}",
            ask_prices.len(),
            ask_sizes.len()
        );
    }

    let to_levels = |prices: &[String],
                     sizes: &[String]|
     -> anyhow::Result<smallvec::SmallVec<[OrderBookLevel; 20]>> {
        prices
            .iter()
            .zip(sizes.iter())
            .map(|(p, s)| {
                Ok(OrderBookLevel {
                    price: parse_decimal_str(p)?,
                    size: parse_decimal_str(s)?,
                })
            })
            .collect()
    };

    let ts = Utc
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .context("invalid timestamp")?;

    Ok(OrderBook {
        venue: venue_from_i8(venue_i8)?,
        instrument: make_instrument(base, quote_cur, inst_type)?,
        bids: to_levels(&bid_prices, &bid_sizes)?,
        asks: to_levels(&ask_prices, &ask_sizes)?,
        timestamp: ts,
        local_timestamp: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample_instrument() -> Instrument {
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

    fn assert_dec_approx(a: Decimal, b: Decimal, label: &str) {
        let diff = (a - b).abs();
        assert!(
            diff < dec!(0.000001),
            "{}: {} vs {} (diff={})",
            label,
            a,
            b,
            diff
        );
    }

    #[test]
    fn encode_decode_quote_roundtrip() {
        let ts = Utc::now();
        let original = Quote {
            venue: DomainVenue::Binance,
            instrument: sample_instrument(),
            bid: dec!(50123.45),
            ask: dec!(50124.67),
            bid_size: dec!(1.5),
            ask_size: dec!(2.3),
            timestamp: ts,
        };

        let bytes = encode_quote(&original);
        let decoded = decode_quote(&bytes).unwrap();

        assert_eq!(decoded.venue, original.venue);
        assert_eq!(decoded.instrument.base, original.instrument.base);
        assert_eq!(decoded.instrument.quote, original.instrument.quote);
        assert_eq!(
            decoded.instrument.instrument_type,
            original.instrument.instrument_type
        );
        assert_dec_approx(decoded.bid, original.bid, "bid");
        assert_dec_approx(decoded.ask, original.ask, "ask");
        assert_dec_approx(decoded.bid_size, original.bid_size, "bid_size");
        assert_dec_approx(decoded.ask_size, original.ask_size, "ask_size");
        assert_eq!(
            decoded.timestamp.timestamp_millis(),
            original.timestamp.timestamp_millis()
        );
    }

    #[test]
    fn encode_decode_order_request_roundtrip() {
        let original = OrderRequest {
            venue: DomainVenue::Okx,
            instrument: sample_instrument(),
            side: DomainSide::Sell,
            order_type: DomainOrderType::Limit,
            time_in_force: Some(DomainTimeInForce::Ioc),
            price: Some(dec!(49999.99)),
            quantity: dec!(0.5),
            estimated_notional: None,
        };

        let bytes = encode_order_request(&original);
        let decoded = decode_order_request(&bytes).unwrap();

        assert_eq!(decoded.venue, original.venue);
        assert_eq!(decoded.instrument.base, original.instrument.base);
        assert_eq!(decoded.side, original.side);
        assert_eq!(decoded.order_type, original.order_type);
        assert_eq!(decoded.time_in_force, original.time_in_force);
        assert_dec_approx(decoded.price.unwrap(), original.price.unwrap(), "price");
        assert_dec_approx(decoded.quantity, original.quantity, "quantity");
    }

    #[test]
    fn encode_decode_fill_roundtrip() {
        let ts = Utc::now();
        let original = Fill {
            order_id: "ord-123".into(),
            client_order_id: None,
            venue: DomainVenue::Bybit,
            instrument: sample_instrument(),
            side: DomainSide::Buy,
            price: dec!(50100.0),
            quantity: dec!(0.25),
            fee: dec!(0.001),
            fee_currency: "USDT".into(),
            filled_at: ts,
        };

        let bytes = encode_fill(&original);
        let decoded = decode_fill(&bytes).unwrap();

        assert_eq!(decoded.order_id, original.order_id);
        assert_eq!(decoded.venue, original.venue);
        assert_eq!(decoded.instrument.base, original.instrument.base);
        assert_eq!(decoded.side, original.side);
        assert_dec_approx(decoded.price, original.price, "price");
        assert_dec_approx(decoded.quantity, original.quantity, "quantity");
        assert_dec_approx(decoded.fee, original.fee, "fee");
        assert_eq!(decoded.fee_currency, original.fee_currency);
        assert_eq!(
            decoded.filled_at.timestamp_millis(),
            original.filled_at.timestamp_millis()
        );
    }

    #[test]
    fn encode_decode_order_book_roundtrip() {
        let ts = Utc::now();
        let original = OrderBook {
            venue: DomainVenue::Binance,
            instrument: sample_instrument(),
            bids: smallvec::smallvec![
                OrderBookLevel {
                    price: dec!(50000),
                    size: dec!(1.5)
                },
                OrderBookLevel {
                    price: dec!(49999),
                    size: dec!(2.0)
                },
                OrderBookLevel {
                    price: dec!(49998),
                    size: dec!(0.5)
                },
            ],
            asks: smallvec::smallvec![
                OrderBookLevel {
                    price: dec!(50010),
                    size: dec!(1.0)
                },
                OrderBookLevel {
                    price: dec!(50011),
                    size: dec!(2.5)
                },
            ],
            timestamp: ts,
            local_timestamp: ts,
        };

        let bytes = encode_order_book(&original);
        let decoded = decode_order_book(&bytes).unwrap();

        assert_eq!(decoded.venue, original.venue);
        assert_eq!(decoded.instrument.base, original.instrument.base);
        assert_eq!(decoded.bids.len(), 3);
        assert_eq!(decoded.asks.len(), 2);
        assert_dec_approx(decoded.bids[0].price, dec!(50000), "bid0 price");
        assert_dec_approx(decoded.bids[2].size, dec!(0.5), "bid2 size");
        assert_dec_approx(decoded.asks[1].price, dec!(50011), "ask1 price");
        assert_eq!(
            decoded.timestamp.timestamp_millis(),
            original.timestamp.timestamp_millis()
        );
    }

    #[test]
    fn decode_order_book_empty_levels() {
        let original = OrderBook {
            venue: DomainVenue::Okx,
            instrument: sample_instrument(),
            bids: smallvec::smallvec![],
            asks: smallvec::smallvec![],
            timestamp: Utc::now(),
            local_timestamp: Utc::now(),
        };
        let bytes = encode_order_book(&original);
        let decoded = decode_order_book(&bytes).unwrap();
        assert!(decoded.bids.is_empty());
        assert!(decoded.asks.is_empty());
    }
}
