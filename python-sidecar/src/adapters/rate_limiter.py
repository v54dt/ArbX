import asyncio
import time


class TokenBucket:
    def __init__(self, max_tokens: int, refill_period: float):
        self.max_tokens = max_tokens
        self.tokens = max_tokens
        self.refill_period = refill_period
        self._last_refill = time.monotonic()

    async def acquire(self):
        while True:
            now = time.monotonic()
            elapsed = now - self._last_refill
            if elapsed >= self.refill_period:
                self.tokens = self.max_tokens
                self._last_refill = now
            if self.tokens > 0:
                self.tokens -= 1
                return
            wait = self.refill_period - elapsed
            await asyncio.sleep(wait)
