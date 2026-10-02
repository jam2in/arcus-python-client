"""Protocol handlers run against bytes without a node, worker, or socket."""

from io import BytesIO

import pytest

from arcus import (
    ArcusNodeConnectionException,
    ArcusNodeSocketException,
    ArcusProtocolException,
    ArcusTranscoder,
    CollectionExist,
    CollectionIndex,
    CollectionOverflow,
    CollectionType,
    CollectionUnreadable,
)
from arcus.protocol.responses import ResponseHandlers


class ByteConnection:
    def __init__(self, response):
        self.stream = BytesIO(response)

    def readline(self):
        line = self.stream.readline()
        if not line:
            raise ArcusNodeConnectionException("end of response")
        assert line.endswith(b"\r\n"), "test response has incomplete framing"
        return line[:-2]

    def recv(self, size):
        return self.stream.read(size)


def handlers(response):
    return ResponseHandlers(ByteConnection(response), ArcusTranscoder())


@pytest.mark.parametrize("method", ["value", "cas_value"])
def test_kv_miss(method):
    assert getattr(handlers(b"END\r\n").kv, method)() is None


def test_kv_value_uses_payload_length_and_leaves_next_response():
    response = handlers(b"VALUE key 0 4\r\na\r\nb\r\nEND\r\nSTORED\r\n")
    assert response.kv.value() == "a\r\nb"
    assert response.status.stored() is True


def test_cas_retains_bytes_token_and_bytes_payload():
    response = handlers(b"VALUE key 2048 3 42\r\n\x00\xffx\r\nEND\r\n")
    assert response.kv.cas_value() == (b"\x00\xffx", b"42")


@pytest.mark.parametrize(
    "wire",
    [
        b"SERVER_ERROR failed\r\n",
        b"VALUE key 0 -1\r\n",
        b"VALUE key 0 invalid\r\n",
        b"VALUE key invalid 1\r\n",
        b"VALUE key 0 1 unexpected\r\n",
        b"VALUE key 0 1\r\nxXXEND\r\n",
        b"VALUE key 0 1\r\nx\r\nSTORED\r\n",
    ],
)
def test_kv_malformed_response_is_not_a_miss(wire):
    with pytest.raises(ArcusProtocolException):
        handlers(wire).kv.value()


def test_cas_requires_a_numeric_token():
    with pytest.raises(ArcusProtocolException):
        handlers(b"VALUE key 0 1 invalid\r\n").kv.cas_value()


def test_reader_rejects_short_payload():
    with pytest.raises(ArcusNodeSocketException):
        handlers(b"VALUE key 0 3\r\nab").kv.value()


@pytest.mark.parametrize(
    ("family", "method", "wire", "expected"),
    [
        ("status", "stored", b"STORED", True),
        ("status", "stored", b"NOT_FOUND", False),
        ("status", "stored", b"EXISTS", False),
        ("status", "stored", b"12", 12),
        ("status", "deleted", b"DELETED", True),
        ("status", "deleted", b"NOT_FOUND", True),
        ("status", "deleted", b"DELETED_DROPPED", True),
        ("collection", "created", b"CREATED", True),
        ("collection", "created", b"ERROR", False),
        ("collection", "stored", b"STORED", True),
        ("collection", "stored", b"NOT_FOUND", False),
        ("collection", "stored", b"12", False),
        ("set", "exist", b"EXIST", True),
        ("set", "exist", b"NOT_EXIST", False),
        ("admin", "ok", b"OK", True),
        ("admin", "ok", b"ERROR", False),
    ],
)
def test_status_results(family, method, wire, expected):
    actual = getattr(getattr(handlers(wire + b"\r\n"), family), method)()
    assert actual == expected
    assert type(actual) is type(expected)


@pytest.mark.parametrize(
    ("family", "method"),
    [("status", "stored"), ("status", "deleted"), ("collection", "stored")],
)
def test_pipeline_preserves_status_strings_and_next_response(family, method):
    response = handlers(b"RESPONSE 2\r\nSTORED\r\nNOT_FOUND\r\nEND\r\nOK\r\n")
    assert getattr(getattr(response, family), method)() == ["STORED", "NOT_FOUND"]
    assert response.admin.ok() is True


@pytest.mark.parametrize(
    ("family", "method", "wire", "error"),
    [
        ("status", "stored", b"TYPE_MISMATCH", CollectionType),
        ("status", "stored", b"OVERFLOWED", CollectionOverflow),
        ("status", "stored", b"OUT_OF_RANGE", CollectionIndex),
        ("status", "deleted", b"TYPE_MISMATCH", CollectionType),
        ("status", "deleted", b"OVERFLOWED", CollectionOverflow),
        ("status", "deleted", b"OUT_OF_RANGE", CollectionIndex),
        ("status", "deleted", b"NOT_FOUND_ELEMENT", CollectionIndex),
        ("collection", "created", b"EXISTS", CollectionExist),
        ("collection", "stored", b"TYPE_MISMATCH", CollectionType),
        ("collection", "stored", b"OVERFLOWED", CollectionOverflow),
        ("collection", "stored", b"OUT_OF_RANGE", CollectionIndex),
    ],
)
def test_status_errors(family, method, wire, error):
    with pytest.raises(error):
        getattr(getattr(handlers(wire + b"\r\n"), family), method)()


@pytest.mark.parametrize(
    ("family", "expected"),
    [("list", ["one", "two", "one"]), ("set", {"one", "two"})],
)
def test_list_and_set_control_order_and_uniqueness(family, expected):
    response = handlers(b"VALUE 0 3\r\n3 one\r\n3 two\r\n3 one\r\nEND\r\n")
    actual = getattr(response, family).get()
    assert actual == expected
    assert type(actual) is type(expected)


@pytest.mark.parametrize("family", ["list", "set"])
def test_collection_values_use_transcoder_flags(family):
    response = handlers(b"VALUE 2048 1\r\n2 \x00\xff\r\nEND\r\n")
    assert list(getattr(response, family).get()) == [b"\x00\xff"]


def test_btree_preserves_bkeys_element_flags_and_last_value():
    response = handlers(
        b"VALUE 0 3\r\n1 3 one\r\n0x02 0x0A 3 two\r\n1 5 newer\r\nEND\r\n"
    )
    assert response.btree.get() == {1: (None, "newer"), "0x02": ("0x0A", "two")}


def test_btree_count():
    assert handlers(b"COUNT=7\r\n").btree.get() == 7


@pytest.mark.parametrize(
    ("family", "empty"), [("list", []), ("set", set()), ("btree", {})]
)
@pytest.mark.parametrize("wire", [b"OUT_OF_RANGE", b"NOT_FOUND_ELEMENT"])
def test_collection_empty_result_types(family, empty, wire):
    actual = getattr(handlers(wire + b"\r\n"), family).get()
    assert actual == empty
    assert type(actual) is type(empty)


@pytest.mark.parametrize("family", ["list", "set", "btree"])
def test_collection_missing_key(family):
    assert getattr(handlers(b"NOT_FOUND\r\n"), family).get() is None


@pytest.mark.parametrize("family", ["list", "set", "btree"])
@pytest.mark.parametrize(
    ("wire", "error"),
    [(b"TYPE_MISMATCH", CollectionType), (b"UNREADABLE", CollectionUnreadable)],
)
def test_collection_errors(family, wire, error):
    with pytest.raises(error):
        getattr(handlers(wire + b"\r\n"), family).get()


def test_btree_mget_groups_elements_and_records_both_miss_formats():
    response = handlers(
        b"VALUE first OK 0 2\r\nELEMENT 1 3 one\r\nELEMENT 0x02 0x0A 3 two\r\n"
        b"VALUE second NOT_FOUND\r\nMISSED_KEYS 1\r\nthird\r\nEND\r\nOK\r\n"
    )
    assert response.btree.mget() == (
        {"first": {1: (None, "one"), "0x02": ("0x0A", "two")}},
        ["second", "third"],
    )
    assert response.admin.ok() is True


def test_btree_smget_preserves_sorted_order_and_per_key_flags():
    response = handlers(
        b"VALUE 2\r\nfirst 0 1 3 one\r\nsecond 2048 0x02 0x0A 2 \x00\xff\r\n"
        b"MISSED_KEYS 1\r\nthird\r\nEND\r\n"
    )
    assert response.btree.smget() == (
        [(1, "first", None, "one"), ("0x02", "second", "0x0A", b"\x00\xff")],
        ["third"],
    )


@pytest.mark.parametrize("method", ["mget", "smget"])
def test_btree_multikey_miss(method):
    assert getattr(handlers(b"NOT_FOUND\r\n").btree, method)() is None


@pytest.mark.parametrize("method", ["mget", "smget"])
@pytest.mark.parametrize(
    ("wire", "error"),
    [
        (b"TYPE_MISMATCH", CollectionType),
        (b"UNREADABLE", CollectionUnreadable),
        (b"OUT_OF_RANGE", CollectionIndex),
        (b"NOT_FOUND_ELEMENT", CollectionIndex),
    ],
)
def test_btree_multikey_errors(method, wire, error):
    with pytest.raises(error):
        getattr(handlers(wire + b"\r\n").btree, method)()


def test_stats_reads_every_line_and_leaves_next_response():
    response = handlers(b"STAT pid 123\r\nSTAT description two words\r\nEND\r\nOK\r\n")
    assert response.admin.stats() == {"pid": "123", "description": "two words"}
    assert response.admin.ok() is True


def test_empty_stats():
    assert handlers(b"END\r\n").admin.stats() == {}


@pytest.mark.parametrize("wire", [b"ERROR", b"STAT pid", b"END extra"])
def test_stats_rejects_malformed_response(wire):
    with pytest.raises(ArcusProtocolException):
        handlers(wire + b"\r\n").admin.stats()
