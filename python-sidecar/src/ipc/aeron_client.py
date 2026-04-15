"""AeronClient — Python → Aeron via a Rust helper subprocess.

Rather than depend on a Python Aeron binding (all of which have spotty
cross-version support), this client spawns the Rust `aeron_pub` binary
from the workspace and writes length-prefixed frames to its stdin.
Wire format matches the helper:

    [ 4-byte length BE ][ payload ]

The Rust helper forwards each payload verbatim to an Aeron publication
on the configured stream id. Call-site encoders (e.g. encode_quote) are
still responsible for prepending the 1-byte MSG_TAG_* discriminator;
this client is agnostic to payload shape.

This is a publisher-only client. Subscribe is owned by the Rust engine
via AeronMarketDataFeed.
"""

from __future__ import annotations

import asyncio
import logging
import struct
from pathlib import Path

logger = logging.getLogger(__name__)


def _default_bin_path() -> Path:
    """Look up the aeron_pub binary produced by `cargo build`.

    Walks up from this file to find the repo root, then checks the
    usual target/{debug,release} locations.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "Cargo.toml").exists() or (parent / "rust-core" / "Cargo.toml").exists():
            for profile in ("release", "debug"):
                candidate = parent / "target" / profile / "aeron_pub"
                if candidate.exists():
                    return candidate
            break
    # Last resort: hope it's on PATH.
    return Path("aeron_pub")


class AeronClient:
    def __init__(
        self,
        stream_id: int = 1001,
        bin_path: Path | None = None,
        log_level: str = "info",
    ):
        self.stream_id = stream_id
        self.bin_path = bin_path or _default_bin_path()
        self.log_level = log_level
        self._proc: asyncio.subprocess.Process | None = None

    async def connect(self) -> None:
        if self._proc is not None:
            return
        logger.info(
            "AeronClient spawn %s --stream-id %d", self.bin_path, self.stream_id
        )
        self._proc = await asyncio.create_subprocess_exec(
            str(self.bin_path),
            "--stream-id",
            str(self.stream_id),
            "--log-level",
            self.log_level,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=None,  # inherit, so driver errors are visible
        )

    async def publish(self, data: bytes) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise RuntimeError("AeronClient not connected")
        frame = struct.pack(">I", len(data)) + data
        self._proc.stdin.write(frame)
        await self._proc.stdin.drain()

    async def disconnect(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        try:
            if proc.stdin is not None:
                proc.stdin.close()
        except Exception:
            logger.debug("stdin close raised", exc_info=True)
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except TimeoutError:
            logger.warning("aeron_pub did not exit within 5s; killing")
            proc.kill()
            await proc.wait()
        logger.info("AeronClient disconnected, helper exit=%s", proc.returncode)
