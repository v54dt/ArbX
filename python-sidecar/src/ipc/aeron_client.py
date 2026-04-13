import logging

logger = logging.getLogger(__name__)


class AeronClient:
    def __init__(self, channel: str = "aeron:ipc", stream_id: int = 1001):
        self.channel = channel
        self.stream_id = stream_id

    async def connect(self) -> None:
        logger.info("AeronClient connect (stub): channel=%s stream=%d", self.channel, self.stream_id)

    async def disconnect(self) -> None:
        logger.info("AeronClient disconnect (stub)")

    async def publish(self, data: bytes) -> None:
        pass  # TODO: real Aeron publish

    async def poll(self) -> bytes | None:
        return None  # TODO: real Aeron poll
