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
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Maximum number of consecutive respawn attempts before giving up. Each spawn
# takes a few hundred ms even when failing, so a tight respawn loop on an
# unhealthy host can exhaust file descriptors fast.
MAX_RESPAWN_ATTEMPTS = 8
# Initial / maximum wait between respawn attempts (exponential backoff).
RESPAWN_BACKOFF_INITIAL_S = 0.5
RESPAWN_BACKOFF_MAX_S = 30.0
# Window after which the respawn counter resets — if a spawn was healthy
# this long, treat the next failure as a fresh incident.
RESPAWN_RESET_WINDOW_S = 60.0


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
        # Respawn budget tracking — see review §2.8.
        self._respawn_attempts = 0
        self._last_spawn_ts = 0.0

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
        self._last_spawn_ts = time.monotonic()

    async def publish(self, data: bytes) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise RuntimeError("AeronClient not connected")
        frame = struct.pack(">I", len(data)) + data
        try:
            self._proc.stdin.write(frame)
            await self._proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            logger.warning("aeron_pub subprocess pipe broken, respawning")
            await self._respawn()

    async def _respawn(self) -> None:
        """Kill the dead subprocess and reconnect with exponential backoff.

        Without backoff, a permanently-broken aeron_pub (driver down,
        binary missing) loops `spawn → die → respawn` until the host runs
        out of file descriptors. Cap at MAX_RESPAWN_ATTEMPTS within the
        reset window; after that, raise so the caller can surface the
        outage upstream and let systemd restart the whole sidecar.
        """
        # Reset the attempt counter if we had a long-enough healthy run.
        if (time.monotonic() - self._last_spawn_ts) > RESPAWN_RESET_WINDOW_S:
            self._respawn_attempts = 0

        old = self._proc
        self._proc = None
        if old is not None:
            try:
                old.kill()
                await old.wait()
            except Exception:
                pass

        if self._respawn_attempts >= MAX_RESPAWN_ATTEMPTS:
            logger.error(
                "AeronClient: %d respawn attempts in %ds — giving up",
                self._respawn_attempts, int(RESPAWN_RESET_WINDOW_S),
            )
            raise RuntimeError(
                f"aeron_pub respawn budget exhausted ({self._respawn_attempts} attempts)"
            )

        backoff = min(
            RESPAWN_BACKOFF_INITIAL_S * (2 ** self._respawn_attempts),
            RESPAWN_BACKOFF_MAX_S,
        )
        self._respawn_attempts += 1
        logger.warning(
            "AeronClient respawn attempt %d (sleeping %.2fs)",
            self._respawn_attempts, backoff,
        )
        await asyncio.sleep(backoff)
        await self.connect()

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
