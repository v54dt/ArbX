import asyncio
import logging
import time
from collections.abc import Callable

import shioaji
from shioaji.constant import Action, OrderType, StockPriceType

from src.adapters.base import BaseAdapter
from src.models.messages import (
    Fill,
    OrderRequest,
    OrderResponse,
    Quote,
    Side,
    Venue,
)

logger = logging.getLogger(__name__)


class ShioajiAdapter(BaseAdapter):
    def __init__(self, api_key: str, secret_key: str, ca_path: str, ca_password: str = ""):
        self._api = shioaji.Shioaji()
        self._api_key = api_key
        self._secret_key = secret_key
        self._ca_path = ca_path
        self._ca_password = ca_password
        self._quote_queue: asyncio.Queue[Quote] = asyncio.Queue()
        self._fill_queue: asyncio.Queue[Fill] = asyncio.Queue()
        self._loop: asyncio.AbstractEventLoop | None = None

    async def connect(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._api.login(
            api_key=self._api_key,
            secret_key=self._secret_key,
        )
        self._api.activate_ca(
            ca_path=self._ca_path,
            ca_passwd=self._ca_password,
        )
        self._api.set_order_callback(self._on_order_event)
        logger.info("Shioaji connected and CA activated")

    async def disconnect(self) -> None:
        self._api.logout()
        logger.info("Shioaji disconnected")

    async def subscribe_quotes(
        self, symbols: list[str], callback: Callable[[Quote], None]
    ) -> None:
        @self._api.on_bidask_stk_v1()
        def _on_bidask_stk(_, bid_ask):
            quote = Quote(
                venue=Venue.SHIOAJI,
                base=bid_ask.code,
                quote_currency="TWD",
                instrument_type="stock",
                bid=bid_ask.bid_price[0],
                ask=bid_ask.ask_price[0],
                bid_size=bid_ask.bid_volume[0],
                ask_size=bid_ask.ask_volume[0],
                timestamp_ms=int(time.time() * 1000),
            )
            if self._loop:
                self._loop.call_soon_threadsafe(callback, quote)

        @self._api.on_bidask_fop_v1()
        def _on_bidask_fop(_, bid_ask):
            quote = Quote(
                venue=Venue.SHIOAJI,
                base=bid_ask.code,
                quote_currency="TWD",
                instrument_type="futures",
                bid=bid_ask.bid_price[0],
                ask=bid_ask.ask_price[0],
                bid_size=bid_ask.bid_volume[0],
                ask_size=bid_ask.ask_volume[0],
                timestamp_ms=int(time.time() * 1000),
            )
            if self._loop:
                self._loop.call_soon_threadsafe(callback, quote)

        for symbol in symbols:
            contract = self._api.Contracts.Stocks.get(symbol)
            if contract is None:
                contract = self._api.Contracts.Futures.get(symbol)
            if contract is None:
                logger.warning("Contract not found: %s", symbol)
                continue
            self._api.quote.subscribe(contract, quote_type=shioaji.constant.QuoteType.BidAsk)
            logger.info("Subscribed to %s", symbol)

    async def place_order(self, request: OrderRequest) -> OrderResponse:
        contract = self._api.Contracts.Stocks.get(request.base)
        if contract is None:
            contract = self._api.Contracts.Futures.get(request.base)
        if contract is None:
            return OrderResponse(order_id="", status="contract_not_found")

        price_type_map = {
            "limit": StockPriceType.LMT,
            "market": StockPriceType.MKT,
        }
        tif_map = {
            "rod": OrderType.ROD,
            "ioc": OrderType.IOC,
            "fok": OrderType.FOK,
        }
        action = Action.Buy if request.side == Side.BUY else Action.Sell
        price_type = price_type_map.get(request.order_type, StockPriceType.LMT)
        order_type = tif_map.get(request.time_in_force, OrderType.ROD)
        order = self._api.Order(
            price=request.price,
            quantity=int(request.quantity),
            action=action,
            price_type=price_type,
            order_type=order_type,
        )
        trade = self._api.place_order(contract, order)
        return OrderResponse(
            order_id=trade.status.id if trade.status else "",
            status=trade.status.status if trade.status else "submitted",
        )

    async def cancel_order(self, order_id: str) -> bool:
        trades = self._api.list_trades()
        for trade in trades:
            if trade.status and trade.status.id == order_id:
                self._api.cancel_order(trade)
                return True
        return False

    async def get_positions(self) -> list[dict]:
        positions = self._api.list_positions()
        return [
            {
                "symbol": p.code,
                "quantity": p.quantity,
                "avg_price": p.price,
                "pnl": p.pnl,
            }
            for p in positions
        ]

    def _on_order_event(self, stat, msg):
        logger.info("Order event: stat=%s msg=%s", stat, msg)
