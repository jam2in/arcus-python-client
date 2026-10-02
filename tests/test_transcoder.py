import datetime
import struct

import pytest

from arcus import ArcusTranscoder


@pytest.mark.parametrize("value", [-(2**63), -256, -1, 0, 127, 128, 255, 2**63 - 1])
def test_signed_integer_round_trip_preserves_wire_encoding(value):
    codec = ArcusTranscoder()
    flags, size, payload = codec.encode(value)
    assert (flags, size, payload) == (codec.FLAG_LONG, 8, struct.pack(">q", value))
    assert codec.decode(flags, payload) == value


@pytest.mark.parametrize(
    ("flag", "payload", "expected"),
    [
        (ArcusTranscoder.FLAG_INTEGER, b"\xff\xff\xff\xff", -1),
        (ArcusTranscoder.FLAG_INTEGER, b"\x80\x00\x00\x00", -(2**31)),
        (ArcusTranscoder.FLAG_INTEGER, b"\xff", 255),
        (ArcusTranscoder.FLAG_INTEGER, b"", 0),
        (ArcusTranscoder.FLAG_LONG, b"\xff", 255),
        (ArcusTranscoder.FLAG_LONG, b"\x80", 128),
        (ArcusTranscoder.FLAG_LONG, b"", 0),
        (ArcusTranscoder.FLAG_BYTE, b"\xff", -1),
        (ArcusTranscoder.FLAG_BYTE, b"\x80", -128),
        (ArcusTranscoder.FLAG_BYTE, b"\x7f", 127),
    ],
)
def test_java_integer_encoding_with_leading_zero_bytes_removed(flag, payload, expected):
    # Java TranscoderUtils removes only leading zero bytes, not sign bytes.
    assert ArcusTranscoder().decode(flag, payload) == expected


@pytest.mark.parametrize("timestamp", [-1, 0, 1])
def test_dates_on_both_sides_of_epoch(timestamp):
    codec = ArcusTranscoder()
    value = datetime.datetime.fromtimestamp(timestamp)
    flags, _, payload = codec.encode(value)
    assert codec.decode(flags, payload) == value


def test_java_compact_date_is_positive():
    codec = ArcusTranscoder()
    assert codec.decode(codec.FLAG_DATE, b"\xff") == datetime.datetime.fromtimestamp(
        0.255
    )
