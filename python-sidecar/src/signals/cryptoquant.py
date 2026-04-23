"""CryptoQuant on-chain data source.

Tracks exchange net flow (inflow - outflow) as a directional signal.
Large net inflow to exchanges = potential sell pressure (bearish).
Large net outflow = accumulation (bullish).

Requires CryptoQuant API key ($30+/mo).
Docs: https://cryptoquant.com/docs
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request

from .base import RawSignal, SignalSource

logger = logging.getLogger(__name__)


class CryptoQuantSource(SignalSource):
    """Polls CryptoQuant exchange flow data."""

    BASE_URL = "https://api.cryptoquant.com/v1"

    def __init__(self, api_key: str, assets: tuple[str, ...] = ("btc", "eth")):
        self._api_key = api_key
        self._assets = assets
        self._prev_netflow: dict[str, float] = {}

    def name(self) -> str:
        return "cryptoquant"

    async def poll(self) -> list[RawSignal]:
        signals: list[RawSignal] = []
        now_ms = int(time.time() * 1000)

        for asset in self._assets:
            try:
                netflow = self._fetch_exchange_netflow(asset)
                if netflow is None:
                    continue

                prev = self._prev_netflow.get(asset)
                self._prev_netflow[asset] = netflow

                # Normalize: negative netflow (outflow > inflow) = bullish signal
                # Scale by a rough factor so signal is in [-1, 1] range
                # Typical BTC daily netflow is -5000 to +5000 BTC
                scale = 5000.0 if asset == "btc" else 50000.0
                normalized = max(-1.0, min(1.0, -netflow / scale))

                signals.append(RawSignal(
                    instrument_key=f"*:{asset}-usdt:*",
                    signal_id="exchange_flow",
                    value=normalized,
                    confidence=0.7 if prev is not None else 0.3,
                    timestamp_ms=now_ms,
                ))

                if prev is not None:
                    delta = netflow - prev
                    if abs(delta) > scale * 0.1:
                        logger.info(
                            "CryptoQuant %s: netflow=%.1f (delta=%.1f, signal=%.3f)",
                            asset, netflow, delta, normalized,
                        )
            except Exception:
                logger.exception("CryptoQuant poll failed for %s", asset)

        return signals

    def _fetch_exchange_netflow(self, asset: str) -> float | None:
        """Fetch latest exchange netflow (inflow - outflow) for an asset."""
        url = (
            f"{self.BASE_URL}/btc/exchange-flows/netflow"
            f"?window=day&limit=1"
        ) if asset == "btc" else (
            f"{self.BASE_URL}/{asset}/exchange-flows/netflow"
            f"?window=day&limit=1"
        )
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {self._api_key}")

        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        results = data.get("result", {}).get("data", [])
        if not results:
            return None
        return float(results[0].get("netflow", 0))
