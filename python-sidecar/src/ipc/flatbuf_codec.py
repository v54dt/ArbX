"""Hand-rolled FlatBuffers encoders matching rust-core/src/ipc/flatbuf_codec.rs.

Without flatc codegen, slot indices must stay in sync with the Rust side's
vtable offsets:
    vtable_offset = slot_index * 2 + 4
i.e. Rust's QUOTE_VENUE=4 is slot 0, QUOTE_BASE=6 is slot 1, etc.

Each emitted payload is prefixed with a 1-byte MSG_TAG_* matching the
constants in rust-core/src/ipc/flatbuf_codec.rs, since FlatBuffers
itself cannot tell which table a buffer holds.
"""

from flatbuffers import Builder

from src.models.messages import Quote, Venue

MSG_TAG_QUOTE = 1
MSG_TAG_ORDER_BOOK = 2
MSG_TAG_FILL = 3
MSG_TAG_ORDER_REQUEST = 4

_VENUE_INT = {
    Venue.BINANCE: 0,
    Venue.OKX: 1,
    Venue.BYBIT: 2,
    Venue.SHIOAJI: 3,
    Venue.FUBON: 4,
}


def encode_quote(quote: Quote) -> bytes:
    builder = Builder(256)
    base = builder.CreateString(quote.base)
    quote_cur = builder.CreateString(quote.quote_currency)
    inst_type = builder.CreateString(quote.instrument_type)

    builder.StartObject(9)
    builder.PrependInt8Slot(0, _VENUE_INT[quote.venue], 0)
    builder.PrependUOffsetTRelativeSlot(1, base, 0)
    builder.PrependUOffsetTRelativeSlot(2, quote_cur, 0)
    builder.PrependUOffsetTRelativeSlot(3, inst_type, 0)
    builder.PrependFloat64Slot(4, float(quote.bid), 0.0)
    builder.PrependFloat64Slot(5, float(quote.ask), 0.0)
    builder.PrependFloat64Slot(6, float(quote.bid_size), 0.0)
    builder.PrependFloat64Slot(7, float(quote.ask_size), 0.0)
    builder.PrependInt64Slot(8, int(quote.timestamp_ms), 0)
    end = builder.EndObject()
    builder.Finish(end)
    return bytes([MSG_TAG_QUOTE]) + bytes(builder.Output())
