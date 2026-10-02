"""Collection framing must preserve both payload bytes and response ownership."""

import queue
import socket
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from arcus import (
    ArcusNodeConnectionException,
    ArcusProtocolException,
    ArcusTranscoder,
    CollectionType,
)
from arcus.protocol.node import ArcusMCNode


def make_node(wire, chunk_size=None):
    sock = Mock(spec=socket.socket)
    size = chunk_size or max(len(wire), 1)
    sock.recv.side_effect = [
        *(wire[index : index + size] for index in range(0, len(wire), size)),
        b"",
    ]
    allocator = SimpleNamespace(
        shutdown=False,
        operation_timeout=2,
        worker=SimpleNamespace(q=queue.Queue(), poll=Mock(), register_node=Mock()),
    )
    with patch("arcus.protocol.connection.socket.socket", return_value=sock):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)
    return node, sock


def collection_operation(node, family):
    if family == "list":
        return node.lop_get("collection", (0, -1))
    if family == "set":
        return node.sop_get("collection")
    if family == "btree":
        return node.bop_get("collection", (0, 10))
    return getattr(node, "bop_" + family)(["collection"], (0, 10))


def collection_wire(family, flags, payload):
    length = str(len(payload)).encode()
    if family in ("list", "set"):
        header = b"VALUE %d 1\r\n" % flags
        prefix = length + b" "
    elif family == "btree":
        header = b"VALUE %d 1\r\n" % flags
        prefix = b"1 0x0A " + length + b" "
    elif family == "mget":
        header = b"VALUE collection OK %d 1\r\n" % flags
        prefix = b"ELEMENT 1 0x0A " + length + b" "
    else:
        header = b"VALUE 1\r\n"
        prefix = b"collection %d 1 0x0A " % flags + length + b" "
    return header + prefix + payload + b"\r\nEND\r\n"


def expected_collection(family, value):
    return {
        "list": [value],
        "set": {value},
        "btree": {1: ("0x0A", value)},
        "mget": ({"collection": {1: ("0x0A", value)}}, []),
        "smget": ([(1, "collection", "0x0A", value)], []),
    }[family]


FAMILIES = ("list", "set", "btree", "mget", "smget")
NEXT_RESPONSES = b"VALUE present 0 2\r\nok\r\nEND\r\nEND\r\n"


def send_and_receive(node, operations):
    for operation in operations:
        node.process_operation(operation)
    for _ in operations:
        if all(operation.has_result() for operation in operations):
            break
        node.do_op()


@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize("chunk_size", [None, 1])
@pytest.mark.parametrize(
    ("flags", "payload", "value"),
    [(0, b"a\r\nb", "a\r\nb"), (2048, b"\x00\r\n\xff", b"\x00\r\n\xff"), (0, b"", "")],
)
def test_collection_payload_preserves_bytes_and_next_requests(
    family, chunk_size, flags, payload, value
):
    node, sock = make_node(
        collection_wire(family, flags, payload) + NEXT_RESPONSES, chunk_size
    )
    first = collection_operation(node, family)
    present, missing = node.get("present"), node.get("missing")
    send_and_receive(node, [first, present, missing])
    assert first.get_result(timeout=0.1) == expected_collection(family, value)
    assert present.get_result(timeout=0.1) == "ok"
    assert missing.get_result(timeout=0.1) is None
    assert not node.handle.disconnected()
    sock.close.assert_not_called()


@pytest.mark.parametrize("family", FAMILIES)
def test_mid_collection_codec_error_invalidates_unread_responses(family):
    node, sock = make_node(collection_wire(family, 0, b"\xff") + NEXT_RESPONSES)
    first = collection_operation(node, family)
    present, missing = node.get("present"), node.get("missing")
    send_and_receive(node, [first, present, missing])
    with pytest.raises(UnicodeDecodeError):
        first.get_result(timeout=0.1)
    for operation in (present, missing):
        with pytest.raises(ArcusNodeConnectionException):
            operation.get_result(timeout=0.1)
    assert node.handle.disconnected()
    assert not node._pending
    sock.close.assert_called_once_with()


@pytest.mark.parametrize("family", FAMILIES)
def test_complete_domain_error_preserves_next_request(family):
    node, sock = make_node(b"TYPE_MISMATCH\r\n" + NEXT_RESPONSES)
    first = collection_operation(node, family)
    present, missing = node.get("present"), node.get("missing")
    send_and_receive(node, [first, present, missing])
    with pytest.raises(CollectionType):
        first.get_result(timeout=0.1)
    assert present.get_result(timeout=0.1) == "ok"
    assert missing.get_result(timeout=0.1) is None
    sock.close.assert_not_called()


@pytest.mark.parametrize("method", ["bop_get", "bop_count", "bop_smget"])
def test_complete_bkey_mismatch_preserves_next_request(method):
    node, sock = make_node(b"BKEY_MISMATCH\r\n" + NEXT_RESPONSES)
    if method == "bop_count":
        first = node.bop_count("collection", (0, 10), None)
    elif method == "bop_smget":
        first = node.bop_smget(["collection"], (0, 10))
    else:
        first = node.bop_get("collection", (0, 10))
    present, missing = node.get("present"), node.get("missing")
    send_and_receive(node, [first, present, missing])
    with pytest.raises(CollectionType, match="bkey type mismatch"):
        first.get_result(timeout=0.1)
    assert present.get_result(timeout=0.1) == "ok"
    assert missing.get_result(timeout=0.1) is None
    sock.close.assert_not_called()


def test_complete_kv_codec_error_preserves_next_request():
    node, sock = make_node(b"VALUE bad 0 1\r\n\xff\r\nEND\r\n" + NEXT_RESPONSES)
    first, present, missing = node.get("bad"), node.get("present"), node.get("missing")
    send_and_receive(node, [first, present, missing])
    with pytest.raises(UnicodeDecodeError):
        first.get_result(timeout=0.1)
    assert present.get_result(timeout=0.1) == "ok"
    assert missing.get_result(timeout=0.1) is None
    sock.close.assert_not_called()


def test_previous_complete_response_does_not_make_custom_parser_failure_safe():
    node, sock = make_node(b"END\r\nVALUE 0 1\r\n1 x\r\nEND\r\n" + NEXT_RESPONSES)

    def incomplete_callback():
        node.handle.readline()
        raise ValueError("custom parser failed before the payload")

    first = node.get("missing")
    failed = node.add_op("lop get", b"lop get collection 0", incomplete_callback)
    pending = node.get("present")
    send_and_receive(node, [first, failed, pending])
    assert first.get_result(timeout=0.1) is None
    with pytest.raises(ValueError, match="custom parser"):
        failed.get_result(timeout=0.1)
    with pytest.raises(ArcusNodeConnectionException):
        pending.get_result(timeout=0.1)
    sock.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("family", "wire"),
    [
        ("list", b"VALUE 0 -1\r\nEND\r\n"),
        ("list", b"VALUE invalid 1\r\n1 x\r\nEND\r\n"),
        ("list", b"VALUE 0 1\r\n-1 x\r\nEND\r\n"),
        ("list", b"VALUE 0 1\r\nx x\r\nEND\r\n"),
        ("set", b"VALUE 0 1\r\n2 x\r\nEND\r\n"),
        ("btree", b"VALUE 0 1\r\n1 2 x\r\nEND\r\n"),
        ("btree", b"VALUE 0 1\r\n1 0x0A -1 x\r\nEND\r\n"),
        ("btree", b"VALUE 0 1\r\ninvalid 1 x\r\nEND\r\n"),
        ("mget", b"VALUE collection OK 0 1\r\nINVALID 1 1 x\r\nEND\r\n"),
        ("mget", b"VALUE collection OK 0 -1\r\nEND\r\n"),
        ("smget", b"VALUE -1\r\nEND\r\n"),
        ("smget", b"VALUE 1\r\ncollection 0 1 -1 x\r\nEND\r\n"),
        *((family, b"SERVER_ERROR failed\r\n") for family in FAMILIES),
        *((family, b"VALUE 0 1\r\n1 x\r\nWRONG\r\n") for family in ("list", "set")),
    ],
)
def test_malformed_collection_response_invalidates_pending_requests(family, wire):
    node, sock = make_node(wire + NEXT_RESPONSES)
    first, pending = collection_operation(node, family), node.get("present")
    send_and_receive(node, [first, pending])
    with pytest.raises(ArcusProtocolException):
        first.get_result(timeout=0.1)
    with pytest.raises(ArcusNodeConnectionException):
        pending.get_result(timeout=0.1)
    sock.close.assert_called_once_with()


@pytest.mark.parametrize("family", FAMILIES)
def test_collection_eof_invalidates_pending_requests(family):
    node, sock = make_node(collection_wire(family, 0, b"abc")[:-8])
    first, pending = collection_operation(node, family), node.get("present")
    send_and_receive(node, [first, pending])
    with pytest.raises(ArcusNodeConnectionException):
        first.get_result(timeout=0.1)
    with pytest.raises(ArcusNodeConnectionException):
        pending.get_result(timeout=0.1)
    sock.close.assert_called_once_with()


def test_set_existence_pipeline_consumes_all_results_and_preserves_next_request():
    node, sock = make_node(
        b"RESPONSE 2\r\nEXIST\r\nNOT_EXIST\r\nEND\r\n" + NEXT_RESPONSES
    )
    piped = node.sop_exist("set", "first", pipe=True)
    final = node.sop_exist("set", "second")
    present, missing = node.get("present"), node.get("missing")
    send_and_receive(node, [piped, final, present, missing])
    assert piped.get_result(timeout=0.1) is True
    assert final.get_result(timeout=0.1) == ["EXIST", "NOT_EXIST"]
    assert present.get_result(timeout=0.1) == "ok"
    assert missing.get_result(timeout=0.1) is None
    sock.close.assert_not_called()


@pytest.mark.parametrize(
    "wire",
    [
        b"RESPONSE -1\r\nEND\r\n",
        b"RESPONSE invalid\r\nEND\r\n",
        b"RESPONSE 1\r\nSTORED\r\nWRONG\r\n",
        b"RESPONSE 1\r\nSTORED\r\nPIPE_ERROR command overflow\r\n",
        b"RESPONSE 1\r\nSTORED\r\nPIPE_ERROR memory overflow\r\n",
        b"RESPONSE 1\r\nSTORED\r\nPIPE_ERROR bad error\r\n",
    ],
)
def test_pipeline_failure_invalidates_remaining_operations(wire):
    node, sock = make_node(wire + NEXT_RESPONSES)
    first, pending = node.lop_insert("list", 0, "x"), node.get("present")
    send_and_receive(node, [first, pending])
    with pytest.raises(ArcusProtocolException):
        first.get_result(timeout=0.1)
    with pytest.raises(ArcusNodeConnectionException):
        pending.get_result(timeout=0.1)
    sock.close.assert_called_once_with()
