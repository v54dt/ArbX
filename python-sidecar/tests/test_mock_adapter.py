import asyncio

import pytest

from src.adapters.mock_adapter import MockAdapter, _split_symbol
from src.models.messages import Venue


def test_split_symbol_hyphen():
    assert _split_symbol("BTC-USDT") == ("BTC", "USDT")


def test_split_symbol_suffix():
    assert _split_symbol("ETHUSDC") == ("ETH", "USDC")
    assert _split_symbol("SOLBTC") == ("SOL", "BTC")


def test_split_symbol_fallback():
    # Unknown suffix falls back to USDT default for the quote side.
    assert _split_symbol("WEIRD") == ("WEIRD", "USDT")


@pytest.mark.asyncio
async def test_mock_adapter_emits_quotes():
    adapter = MockAdapter(venue=Venue.BINANCE, interval_ms=20, start_bid=100.0, spread=2.0)
    await adapter.connect()

    received = []
    await adapter.subscribe_quotes(["BTC-USDT"], received.append)

    # Let a few intervals pass.
    await asyncio.sleep(0.1)
    await adapter.disconnect()

    assert len(received) >= 2, f"expected >=2 quotes, got {len(received)}"
    q = received[0]
    assert q.venue == Venue.BINANCE
    assert q.base == "BTC"
    assert q.quote_currency == "USDT"
    assert q.instrument_type == "spot"
    # ask > bid with our spread.
    assert q.ask > q.bid
    assert q.bid_size == 1.0
    assert q.ask_size == 1.0


@pytest.mark.asyncio
async def test_disconnect_cancels_emit_tasks():
    adapter = MockAdapter(interval_ms=50)
    await adapter.connect()
    await adapter.subscribe_quotes(["BTC-USDT"], lambda _: None)
    assert len(adapter._tasks) == 1
    await adapter.disconnect()
    assert len(adapter._tasks) == 0
