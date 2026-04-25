"""FRED API macro economic data source (free)."""
import logging
import time

import aiohttp

from .base import RawSignal, SignalSource

logger = logging.getLogger(__name__)

# Key macro series and their typical market impact
SERIES_MAP = {
    "CPIAUCSL": ("cpi", "macro_event"),
    "UNRATE": ("unemployment", "macro_event"),
    "FEDFUNDS": ("fed_funds", "macro_event"),
}


class FredSource(SignalSource):
    """Polls FRED for latest macro economic releases."""

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._last_values: dict[str, float] = {}

    def name(self) -> str:
        return "fred"

    async def poll(self) -> list[RawSignal]:
        signals = []
        now_ms = int(time.time() * 1000)

        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for series_id, (label, signal_id) in SERIES_MAP.items():
                url = (
                    f"https://api.stlouisfed.org/fred/series/observations"
                    f"?series_id={series_id}&api_key={self._api_key}"
                    f"&sort_order=desc&limit=1&file_type=json"
                )
                try:
                    async with session.get(url) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                    obs = data.get("observations", [])
                    if not obs:
                        continue
                    value = float(obs[0]["value"])
                    prev = self._last_values.get(series_id)
                    self._last_values[series_id] = value

                    if prev is not None and prev != value:
                        delta = value - prev
                        signals.append(
                            RawSignal(
                                instrument_key="*",
                                signal_id=signal_id,
                                value=delta,
                                confidence=1.0,
                                timestamp_ms=now_ms,
                            )
                        )
                        logger.info(
                            "FRED %s: %.4f -> %.4f (delta=%.4f)", label, prev, value, delta
                        )
                except Exception:
                    logger.exception("FRED poll failed for %s", series_id)

        return signals
