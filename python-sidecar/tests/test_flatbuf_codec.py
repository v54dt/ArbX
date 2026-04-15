from src.ipc.flatbuf_codec import encode_quote
from src.models.messages import Quote, Venue


def sample_quote() -> Quote:
    return Quote(
        venue=Venue.BINANCE,
        base="BTC",
        quote_currency="USDT",
        instrument_type="spot",
        bid=50000.5,
        ask=50001.0,
        bid_size=1.5,
        ask_size=2.0,
        timestamp_ms=1_700_000_000_000,
    )


def test_encode_quote_returns_bytes():
    out = encode_quote(sample_quote())
    assert isinstance(out, bytes)
    assert len(out) >= 16  # root offset + vtable + fields


def test_encode_quote_changes_with_input():
    a = encode_quote(sample_quote())
    q2 = sample_quote()
    q2.bid = 49999.0
    b = encode_quote(q2)
    assert a != b
