import asyncio
import logging
import time
from collections.abc import Callable

from src.adapters.base import BaseAdapter
from src.models.messages import (
    OrderRequest,
    OrderResponse,
    Quote,
    Side,
    Venue,
)

logger = logging.getLogger(__name__)


class FubonAdapter(BaseAdapter):
    """Fubon (富邦) adapter using fubon_neo SDK.

    fubon_neo is not on PyPI. Install from local .whl:
        pip install fubon_neo-X.Y.Z-cp3xx-cp3xx-linux_x86_64.whl
    """

    def __init__(self, user_id: str, password: str, pfx_path: str, pfx_password: str = ""):
        self._user_id = user_id
        self._password = password
        self._pfx_path = pfx_path
        self._pfx_password = pfx_password
        self._sdk = None
        self._accounts = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscribed_symbols: set[str] = set()

    async def connect(self) -> None:
        from fubon_neo.sdk import FubonSDK

        self._loop = asyncio.get_running_loop()
        self._sdk = FubonSDK()
        try:
            self._accounts = self._sdk.login(
                self._user_id, self._password, self._pfx_path, self._pfx_password
            )
        except Exception:
            logger.exception("Fubon login failed")
            raise
        try:
            self._sdk.init_realtime()
        except Exception:
            logger.exception("Fubon init_realtime failed")
            raise
        logger.info("Fubon connected, accounts: %d", len(self._accounts.data))

    async def disconnect(self) -> None:
        self._sdk = None
        logger.info("Fubon disconnected")

    async def subscribe_quotes(
        self, symbols: list[str], callback: Callable[[Quote], None]
    ) -> None:
        ws_client = self._sdk.marketdata.websocket_client.stock

        def _on_message(message):
            if message.get("event") != "data":
                return
            data = message.get("data", {})
            bids = data.get("bids", [{}])
            asks = data.get("asks", [{}])
            best_bid = bids[0] if bids else {}
            best_ask = asks[0] if asks else {}
            quote = Quote(
                venue=Venue.FUBON,
                base=data.get("symbol", ""),
                quote_currency="TWD",
                instrument_type="stock",
                bid=float(best_bid.get("price", 0)),
                ask=float(best_ask.get("price", 0)),
                bid_size=float(best_bid.get("volume", 0)),
                ask_size=float(best_ask.get("volume", 0)),
                timestamp_ms=int(time.time() * 1000),
            )
            if self._loop:
                self._loop.call_soon_threadsafe(callback, quote)

        ws_client.on("message", _on_message)
        ws_client.connect()

        for symbol in symbols:
            if symbol in self._subscribed_symbols:
                logger.debug("Fubon already subscribed to %s, skipping", symbol)
                continue
            ws_client.subscribe({"channel": "books", "symbol": symbol})
            self._subscribed_symbols.add(symbol)
            logger.info("Fubon subscribed to %s", symbol)

    async def unsubscribe_quotes(self, symbols: list[str]) -> None:
        ws_client = self._sdk.marketdata.websocket_client.stock
        for symbol in symbols:
            if symbol not in self._subscribed_symbols:
                continue
            ws_client.unsubscribe({"channel": "books", "symbol": symbol})
            self._subscribed_symbols.discard(symbol)
            logger.info("Fubon unsubscribed from %s", symbol)

    async def place_order(self, request: OrderRequest) -> OrderResponse:
        from fubon_neo.sdk import Order

        account = self._accounts.data[0]
        action = Order.Buy if request.side == Side.BUY else Order.Sell
        order = Order(
            buy_sell=action,
            symbol=request.base,
            price=str(request.price),
            quantity=int(request.quantity),
            price_flag=Order.LMT,
            time_in_force=Order.ROD,
        )
        result = self._sdk.stock.place_order(account, order)
        return OrderResponse(
            order_id=result.data.get("ord_no", "") if result.data else "",
            status="submitted" if result.is_success else "failed",
        )

    async def cancel_order(self, order_id: str) -> bool:
        result = self._sdk.stock.cancel_order(self._accounts.data[0], order_id)
        return result.is_success if result else False

    async def get_positions(self) -> list[dict]:
        result = self._sdk.stock.get_inventories(self._accounts.data[0])
        if not result.is_success:
            return []
        return [
            {
                "symbol": inv.get("stk_no", ""),
                "quantity": inv.get("qty", 0),
                "avg_price": inv.get("cost_price", 0),
                "pnl": inv.get("unrealized_pnl", 0),
            }
            for inv in (result.data or [])
        ]
