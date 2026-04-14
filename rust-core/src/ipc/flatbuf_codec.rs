use anyhow::{Context as _, bail};
use chrono::{TimeZone as _, Utc};
use flatbuffers::{FlatBufferBuilder, Follow, ForwardsUOffset, Table, WIPOffset};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::models::enums::{
    OrderType as DomainOrderType, Side as DomainSide, TimeInForce as DomainTimeInForce,
    Venue as DomainVenue,
};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::market::Quote;
use crate::models::order::{Fill, OrderRequest};

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

fn dec_to_f64(d: Decimal) -> f64 {
    d.to_f64().unwrap_or_else(|| {
        tracing::warn!(decimal = %d, "Decimal->f64 overflow, falling back to 0.0");
        0.0
    })
}

fn f64_to_dec(v: f64) -> anyhow::Result<Decimal> {
    Decimal::try_from(v).context("f64 -> Decimal")
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

// Helper to read a field from a FlatBuffer table.
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

    let start = fbb.start_table();
    fbb.push_slot::<i8>(vt::QUOTE_VENUE, venue_to_i8(quote.venue), 0);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_BASE, base);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_QUOTE_CURRENCY, quote_cur);
    fbb.push_slot_always::<WIPOffset<_>>(vt::QUOTE_INSTRUMENT_TYPE, inst_type);
    fbb.push_slot::<f64>(vt::QUOTE_BID, dec_to_f64(quote.bid), 0.0);
    fbb.push_slot::<f64>(vt::QUOTE_ASK, dec_to_f64(quote.ask), 0.0);
    fbb.push_slot::<f64>(vt::QUOTE_BID_SIZE, dec_to_f64(quote.bid_size), 0.0);
    fbb.push_slot::<f64>(vt::QUOTE_ASK_SIZE, dec_to_f64(quote.ask_size), 0.0);
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
    let bid = unsafe { get_field::<f64>(&table, vt::QUOTE_BID, 0.0) };
    let ask = unsafe { get_field::<f64>(&table, vt::QUOTE_ASK, 0.0) };
    let bid_size = unsafe { get_field::<f64>(&table, vt::QUOTE_BID_SIZE, 0.0) };
    let ask_size = unsafe { get_field::<f64>(&table, vt::QUOTE_ASK_SIZE, 0.0) };
    let timestamp_ms = unsafe { get_field::<i64>(&table, vt::QUOTE_TIMESTAMP_MS, 0) };

    let ts = Utc
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .context("invalid timestamp")?;

    Ok(Quote {
        venue: venue_from_i8(venue_i8)?,
        instrument: make_instrument(base, quote_cur, inst_type)?,
        bid: f64_to_dec(bid)?,
        ask: f64_to_dec(ask)?,
        bid_size: f64_to_dec(bid_size)?,
        ask_size: f64_to_dec(ask_size)?,
        timestamp: ts,
    })
}

pub fn encode_order_request(req: &OrderRequest) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(256);
    let base = fbb.create_string(&req.instrument.base);
    let quote_cur = fbb.create_string(&req.instrument.quote);
    let inst_type = fbb.create_string(instrument_type_str(req.instrument.instrument_type));

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
    fbb.push_slot::<f64>(vt::OR_PRICE, req.price.map(dec_to_f64).unwrap_or(0.0), 0.0);
    fbb.push_slot::<f64>(vt::OR_QUANTITY, dec_to_f64(req.quantity), 0.0);
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
    let price_f = unsafe { get_field::<f64>(&table, vt::OR_PRICE, 0.0) };
    let quantity_f = unsafe { get_field::<f64>(&table, vt::OR_QUANTITY, 0.0) };

    let price = if price_f == 0.0 {
        None
    } else {
        Some(f64_to_dec(price_f)?)
    };

    Ok(OrderRequest {
        venue: venue_from_i8(venue_i8)?,
        instrument: make_instrument(base, quote_cur, inst_type)?,
        side: side_from_i8(side_i8)?,
        order_type: order_type_from_i8(ot_i8)?,
        // Lossy: encode maps None → 0 (Rod), so decode always returns Some.
        time_in_force: Some(tif_from_i8(tif_i8)?),
        price,
        quantity: f64_to_dec(quantity_f)?,
    })
}

pub fn encode_fill(fill: &Fill) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(256);
    let order_id = fbb.create_string(&fill.order_id);
    let base = fbb.create_string(&fill.instrument.base);
    let quote_cur = fbb.create_string(&fill.instrument.quote);
    let fee_cur = fbb.create_string(&fill.fee_currency);

    let start = fbb.start_table();
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_ORDER_ID, order_id);
    fbb.push_slot::<i8>(vt::FILL_VENUE, venue_to_i8(fill.venue), 0);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_BASE, base);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_QUOTE_CURRENCY, quote_cur);
    fbb.push_slot::<i8>(vt::FILL_SIDE, side_to_i8(fill.side), 0);
    fbb.push_slot::<f64>(vt::FILL_PRICE, dec_to_f64(fill.price), 0.0);
    fbb.push_slot::<f64>(vt::FILL_QUANTITY, dec_to_f64(fill.quantity), 0.0);
    fbb.push_slot::<f64>(vt::FILL_FEE, dec_to_f64(fill.fee), 0.0);
    fbb.push_slot_always::<WIPOffset<_>>(vt::FILL_FEE_CURRENCY, fee_cur);
    fbb.push_slot::<i64>(vt::FILL_TIMESTAMP_MS, fill.filled_at.timestamp_millis(), 0);
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
    let side_i8 = unsafe { get_field::<i8>(&table, vt::FILL_SIDE, 0) };
    let price_f = unsafe { get_field::<f64>(&table, vt::FILL_PRICE, 0.0) };
    let quantity_f = unsafe { get_field::<f64>(&table, vt::FILL_QUANTITY, 0.0) };
    let fee_f = unsafe { get_field::<f64>(&table, vt::FILL_FEE, 0.0) };
    let fee_cur = unsafe { get_str(&table, vt::FILL_FEE_CURRENCY) }.unwrap_or("");
    let timestamp_ms = unsafe { get_field::<i64>(&table, vt::FILL_TIMESTAMP_MS, 0) };

    let ts = Utc
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .context("invalid timestamp")?;

    Ok(Fill {
        order_id: order_id.to_string(),
        venue: venue_from_i8(venue_i8)?,
        instrument: make_instrument(base, quote_cur, "spot")?,
        side: side_from_i8(side_i8)?,
        price: f64_to_dec(price_f)?,
        quantity: f64_to_dec(quantity_f)?,
        fee: f64_to_dec(fee_f)?,
        fee_currency: fee_cur.to_string(),
        filled_at: ts,
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
}
