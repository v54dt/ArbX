"""Base class for event/signal sources."""
from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RawSignal:
    instrument_key: str  # e.g. "binance:btc-usdt:swap" or "*" for global
    signal_id: str  # e.g. "news_sentiment", "whale_alert"
    value: float  # signal value (e.g. sentiment -1.0 to 1.0)
    confidence: float  # 0.0 to 1.0
    timestamp_ms: int  # epoch ms


class SignalSource:
    """Abstract base for polling signal sources."""

    def name(self) -> str:
        raise NotImplementedError

    async def poll(self) -> list[RawSignal]:
        """Poll the source for new signals. Called periodically."""
        raise NotImplementedError
