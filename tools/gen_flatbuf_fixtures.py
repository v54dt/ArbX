"""Regenerate FlatBuffers fixture files consumed by the Rust cross-compat
tests. Keeps Rust and Python sides in lockstep without pulling Python into
the Rust CI.

Usage:
    python-sidecar/.venv/bin/python tools/gen_flatbuf_fixtures.py

Outputs:
    rust-core/tests/fixtures/python_quote.bin  — tagged Quote payload

The fixture is a full on-the-wire Aeron payload: 1-byte MSG_TAG_QUOTE
prefix + FlatBuffers-encoded Quote. The Rust test strips the tag byte
before calling decode_quote (matching AeronMarketDataFeed's behaviour).
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "python-sidecar"))

from src.ipc.flatbuf_codec import encode_quote  # noqa: E402
from src.models.messages import Quote, Venue  # noqa: E402


def sample_quote() -> Quote:
    return Quote(
        venue=Venue.BINANCE,
        base="BTC",
        quote_currency="USDT",
        instrument_type="spot",
        bid=50000.5,
        ask=50010.25,
        bid_size=1.5,
        ask_size=2.0,
        timestamp_ms=1_700_000_000_000,
    )


def main() -> None:
    out_dir = REPO_ROOT / "rust-core" / "tests" / "fixtures"
    out_dir.mkdir(parents=True, exist_ok=True)

    quote_bytes = encode_quote(sample_quote())
    out_path = out_dir / "python_quote.bin"
    out_path.write_bytes(quote_bytes)
    print(f"wrote {out_path} ({len(quote_bytes)} bytes)")


if __name__ == "__main__":
    main()
