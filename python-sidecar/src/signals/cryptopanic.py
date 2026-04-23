"""CryptoPanic news sentiment source (free tier)."""
import json
import logging
import time
import urllib.request

from .base import RawSignal, SignalSource

logger = logging.getLogger(__name__)

# Map CryptoPanic vote sentiment to a numeric score
SENTIMENT_MAP = {
    "positive": 0.5,
    "negative": -0.5,
    "important": 0.3,
    "lol": 0.0,
    "toxic": -0.3,
    "saved": 0.1,
    "liked": 0.2,
}


class CryptoPanicSource(SignalSource):
    """Polls CryptoPanic /api/v1/posts/ for recent crypto news."""

    def __init__(self, api_key: str = "", currencies: str = "BTC,ETH"):
        self._api_key = api_key
        self._currencies = currencies
        self._last_poll_ts: int = 0

    def name(self) -> str:
        return "cryptopanic"

    async def poll(self) -> list[RawSignal]:
        base = "https://cryptopanic.com/api/v1/posts/"
        if self._api_key:
            params = (
                f"?auth_token={self._api_key}"
                f"&currencies={self._currencies}&filter=hot&public=true"
            )
        else:
            params = f"?currencies={self._currencies}&filter=hot&public=true"
        url = base + params

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ArbX/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: ASYNC210, ASYNC230
                data = json.loads(resp.read())
        except Exception:
            logger.exception("CryptoPanic poll failed")
            return []

        signals = []
        now_ms = int(time.time() * 1000)

        for post in data.get("results", [])[:10]:
            votes = post.get("votes", {})
            total_votes = sum(votes.values()) if votes else 0
            if total_votes == 0:
                continue

            sentiment = sum(
                SENTIMENT_MAP.get(k, 0.0) * v for k, v in votes.items()
            ) / total_votes

            confidence = min(total_votes / 50.0, 1.0)

            for currency in post.get("currencies", []):
                code = currency.get("code", "").upper()
                if code:
                    signals.append(
                        RawSignal(
                            instrument_key=f"*:{code.lower()}-usdt:*",
                            signal_id="news_sentiment",
                            value=sentiment,
                            confidence=confidence,
                            timestamp_ms=now_ms,
                        )
                    )

        self._last_poll_ts = now_ms
        return signals
