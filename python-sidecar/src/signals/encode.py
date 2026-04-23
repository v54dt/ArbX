"""Encode signal for Aeron IPC (matches Rust ipc/flatbuf_codec.rs MSG_TAG_SIGNAL).

Vtable slot mapping (must stay in sync with Rust SIG_* constants):
    slot 0 (vt offset 4)  -> instrument_key  (string)
    slot 1 (vt offset 6)  -> signal_id        (string)
    slot 2 (vt offset 8)  -> value            (string, decimal repr)
    slot 3 (vt offset 10) -> confidence        (string, decimal repr)
    slot 4 (vt offset 12) -> timestamp_ms      (int64)
"""

from flatbuffers import Builder

MSG_TAG_SIGNAL = 0x05


def encode_signal(
    instrument_key: str,
    signal_id: str,
    value: float,
    confidence: float,
    timestamp_ms: int,
) -> bytes:
    """Encode a signal as FlatBuffers with MSG_TAG_SIGNAL prefix.

    Uses the same hand-rolled FlatBuffer pattern as encode_quote in
    src/ipc/flatbuf_codec.py. Value and confidence are encoded as
    decimal strings to match the Rust Decimal::from_str parse path.
    """
    builder = Builder(256)
    key_off = builder.CreateString(instrument_key)
    sid_off = builder.CreateString(signal_id)
    val_off = builder.CreateString(str(value))
    conf_off = builder.CreateString(str(confidence))

    builder.StartObject(5)
    builder.PrependUOffsetTRelativeSlot(0, key_off, 0)
    builder.PrependUOffsetTRelativeSlot(1, sid_off, 0)
    builder.PrependUOffsetTRelativeSlot(2, val_off, 0)
    builder.PrependUOffsetTRelativeSlot(3, conf_off, 0)
    builder.PrependInt64Slot(4, int(timestamp_ms), 0)
    end = builder.EndObject()
    builder.Finish(end)
    return bytes([MSG_TAG_SIGNAL]) + bytes(builder.Output())
