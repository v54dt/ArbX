"""Mock adapter that emits synthetic Quote events on a timer.

Purpose: smoke-test the Aeron forwarding pipeline end-to-end without
pulling in shioaji / fubon_neo / ccxt. Config maps a venue="mock" to
this adapter; each configured symbol gets one Quote per interval_ms
with a tiny random-walk around a configured starting bid/ask.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable

from src.adapters.base import BaseAdapter
from src.models.messages import OrderRequest, OrderResponse, Quote, Venue

logger = logging.getLogger(__name__)


class MockAdapter(BaseAdapter):
    def __init__(
        self,
        venue: Venue = Venue.BINANCE,
        interval_ms: int = 100,
        start_bid: float = 50_000.0,
        spread: float = 10.0,
    ):
        self.venue = venue
        self.interval_ms = interval_ms
        self.start_bid = start_bid
        self.spread = spread
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def connect(self) -> None:
        self._running = True
        logger.info(
            "MockAdapter connected venue=%s interval=%dms",
            self.venue.value, self.interval_ms,
        )

    async def disconnect(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        logger.info("MockAdapter disconnected")

    async def subscribe_quotes(
        self, symbols: list[str], callback: Callable[[Quote], None]
    ) -> None:
        for sym in symbols:
            task = asyncio.create_task(self._emit_loop(sym, callback))
            self._tasks.append(task)

    async def _emit_loop(self, symbol: str, callback: Callable[[Quote], None]) -> None:
        base, quote_cur = _split_symbol(symbol)
        mid = self.start_bid
        sleep_s = self.interval_ms / 1000.0
        while self._running:
            mid += random.uniform(-0.5, 0.5)
            bid = mid - self.spread / 2
            ask = mid + self.spread / 2
            q = Quote(
                venue=self.venue,
                base=base,
                quote_currency=quote_cur,
                instrument_type="spot",
                bid=bid,
                ask=ask,
                bid_size=1.0,
                ask_size=1.0,
                timestamp_ms=int(time.time() * 1000),
            )
            try:
                callback(q)
            except Exception:
                logger.exception("mock quote callback raised")
            await asyncio.sleep(sleep_s)

    # Not used in quote-only smoke flow — stubbed to satisfy BaseAdapter.
    async def place_order(self, request: OrderRequest) -> OrderResponse:
        raise NotImplementedError("MockAdapter does not place orders")

    async def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError("MockAdapter does not cancel orders")

    async def get_positions(self) -> list[dict]:
        return []


def _split_symbol(symbol: str) -> tuple[str, str]:
    """Accept 'BTC-USDT' or 'BTCUSDT' (common quote suffixes)."""
    if "-" in symbol:
        base, quote = symbol.split("-", 1)
        return base, quote
    for q in ("USDT", "USDC", "BUSD", "BTC", "ETH"):
        if symbol.endswith(q):
            return symbol[: -len(q)], q
    return symbol, "USDT"
