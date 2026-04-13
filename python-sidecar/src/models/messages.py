from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Side(Enum):
    BUY = "buy"
    SELL = "sell"


class Venue(Enum):
    BINANCE = "binance"
    OKX = "okx"
    BYBIT = "bybit"
    SHIOAJI = "shioaji"
    FUBON = "fubon"


@dataclass
class Quote:
    venue: Venue
    base: str
    quote_currency: str
    instrument_type: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    timestamp_ms: int


@dataclass
class OrderRequest:
    venue: Venue
    base: str
    quote_currency: str
    instrument_type: str
    side: Side
    order_type: str
    time_in_force: str
    price: float
    quantity: float


@dataclass
class OrderResponse:
    order_id: str
    status: str


@dataclass
class Fill:
    order_id: str
    venue: Venue
    base: str
    quote_currency: str
    side: Side
    price: float
    quantity: float
    fee: float
    fee_currency: str
    timestamp_ms: int
