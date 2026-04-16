# schemas/

This directory used to hold a `messages.fbs` FlatBuffers schema file. It was
**unused** — neither side of the IPC ran `flatc` codegen against it. Both
encoders are hand-rolled to match each other's vtable slots.

## Authoritative codecs (the source of truth)

- **Rust:** `rust-core/src/ipc/flatbuf_codec.rs`
  - `MSG_TAG_QUOTE` / `MSG_TAG_ORDER_BOOK` / `MSG_TAG_FILL` / `MSG_TAG_ORDER_REQUEST` (1-byte payload prefix)
  - `vt::*` constants pin the slot indices for each table
  - `encode_quote` / `decode_quote` / `encode_order_book` / `decode_order_book` /
    `encode_fill` / `decode_fill` / `encode_order_request` / `decode_order_request`

- **Python:** `python-sidecar/src/ipc/flatbuf_codec.py`
  - Same `MSG_TAG_*` constants
  - `encode_quote` and friends mirror the Rust slot layout

## Cross-language verification

`rust-core/tests/flatbuf_python_compat.rs` decodes a Python-encoded fixture
(`rust-core/tests/fixtures/python_quote.bin`) and asserts every field matches
what the Rust encoder would have produced. Regenerate the fixture after
touching either codec:

```bash
python-sidecar/.venv/bin/python tools/gen_flatbuf_fixtures.py
```

## Why no `flatc`

We picked hand-rolled slot encoding to avoid the build-time dependency on
`flatc` and to keep the wire format pinned to a 1-byte type tag + vtable —
trivially auditable. If/when this gets large enough to warrant proper
codegen, restore an `.fbs` schema and wire `flatc` into both build pipelines
(Rust `build.rs` + Python `setup.py`/pre-commit hook).
