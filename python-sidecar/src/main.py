import asyncio
import logging
import sys

from src.adapters.base import BaseAdapter
from src.adapters.fubon_adapter import FubonAdapter
from src.adapters.shioaji_adapter import ShioajiAdapter
from src.config import load_config
from src.ipc.aeron_client import AeronClient
from src.ipc.flatbuf_codec import encode_quote
from src.models.messages import Quote

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_adapter(venue_cfg: dict) -> BaseAdapter:
    venue = venue_cfg["venue"]
    if venue == "shioaji":
        return ShioajiAdapter(
            api_key=venue_cfg["api_key"],
            secret_key=venue_cfg["secret_key"],
            ca_path=venue_cfg.get("ca_path", ""),
            ca_password=venue_cfg.get("ca_password", ""),
        )
    if venue == "fubon":
        return FubonAdapter(
            user_id=venue_cfg["user_id"],
            password=venue_cfg["password"],
            pfx_path=venue_cfg.get("pfx_path", ""),
            pfx_password=venue_cfg.get("pfx_password", ""),
        )
    raise ValueError(f"Unknown venue: {venue}")


async def run(config_path: str) -> None:
    cfg = load_config(config_path)
    aeron = AeronClient(
        channel=cfg.get("ipc", {}).get("channel", "aeron:ipc"),
        stream_id=cfg.get("ipc", {}).get("stream_id", 1001),
    )
    await aeron.connect()

    adapters: list[BaseAdapter] = []
    for venue_cfg in cfg.get("venues", []):
        adapter = build_adapter(venue_cfg)
        await adapter.connect()
        adapters.append(adapter)

    quote_queue: asyncio.Queue[Quote] = asyncio.Queue()
    loop = asyncio.get_running_loop()

    def on_quote(quote: Quote) -> None:
        loop.call_soon_threadsafe(quote_queue.put_nowait, quote)

    for adapter, venue_cfg in zip(adapters, cfg.get("venues", [])):
        symbols = venue_cfg.get("symbols", [])
        if symbols:
            await adapter.subscribe_quotes(symbols, on_quote)

    logger.info("Sidecar running, forwarding quotes to Rust engine via Aeron")
    try:
        while True:
            quote = await quote_queue.get()
            logger.debug("Quote: %s %s bid=%s ask=%s", quote.venue, quote.base, quote.bid, quote.ask)
            payload = encode_quote(quote)
            await aeron.publish(payload)
    except asyncio.CancelledError:
        pass
    finally:
        for adapter in adapters:
            await adapter.disconnect()
        await aeron.disconnect()


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/sidecar.yaml"
    asyncio.run(run(config_path))


if __name__ == "__main__":
    main()
