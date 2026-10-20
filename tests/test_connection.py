import errno
import socket
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from arcus import ArcusNodeConnectionException, ArcusProtocolException, ArcusTranscoder
from arcus.operation import ArcusOperation
from arcus.protocol import ArcusMCNode, Connection


def make_connection(*chunks):
    sock = Mock(spec=socket.socket)
    sock.recv.side_effect = [*chunks, AssertionError("unexpected socket read")]
    with patch("arcus.protocol.connection.socket.socket", return_value=sock):
        connection = Connection("127.0.0.1:11211")
    return connection, sock


def make_node(*chunks):
    connection, sock = make_connection(*chunks)
    node = ArcusMCNode.__new__(ArcusMCNode)
    node.handle = connection
    node.transcoder = ArcusTranscoder()
    node.lock = threading.Lock()
    node._io_lock = threading.RLock()
    node._read_lock = threading.RLock()
    node._read_failed = False
    node._pending = set()
    node.node_allocator = SimpleNamespace(worker=Mock())
    node.ops = []
    node.addr = "127.0.0.1:11211"
    return node, sock


def test_reconnect_closes_previous_socket_and_discards_buffer():
    connection, original = make_connection()
    connection.buffer = b"stale response\r\n"
    replacement = Mock(spec=socket.socket)
    with patch("arcus.protocol.connection.socket.socket", return_value=replacement):
        assert connection.connect() is replacement
    original.close.assert_called_once_with()
    replacement.connect.assert_called_once_with(("127.0.0.1", 11211))
    assert connection.buffer == b""


def test_failed_reconnect_leaves_connection_disconnected():
    connection, original = make_connection()
    replacement = Mock(spec=socket.socket)
    replacement.connect.side_effect = OSError("connection refused")
    with patch("arcus.protocol.connection.socket.socket", return_value=replacement):
        assert connection.connect() is None
    assert connection.disconnected()
    original.close.assert_called_once_with()
    replacement.close.assert_called_once_with()


def test_sasl_handshake_preserves_its_timeout_then_restores_blocking_io():
    sock = Mock(spec=socket.socket)
    sock.recv.return_value = b"SASL_OK\r\n"

    def authenticate(connection):
        assert sock.settimeout.call_args.args == (Connection.sasl_timeout,)
        connection.send_request(b"sasl mech")
        assert connection.readline() == b"SASL_OK"

    with (
        patch("arcus.protocol.connection.socket.socket", return_value=sock),
        patch.object(Connection, "_sasl_auth", authenticate),
    ):
        connection = Connection("127.0.0.1:11211", "user", "password")
    assert sock.settimeout.call_args.args == (None,)
    connection.disconnect()


def test_disconnect_is_idempotent_and_discards_buffer():
    connection, sock = make_connection()
    connection.buffer = b"stale response\r\n"
    connection.disconnect()
    connection.disconnect()
    assert connection.disconnected()
    assert connection.buffer == b""
    sock.close.assert_called_once_with()


def test_readline_combines_partial_header_and_keeps_following_data():
    connection, sock = make_connection(b"VAL", b"UE key 0 3\r", b"\nabc\r\nEND\r\n")
    assert connection.readline() == b"VALUE key 0 3"
    assert connection.recv(5) == b"abc\r\n"
    assert connection.readline() == b"END"
    assert connection.buffer == b""
    assert sock.recv.call_count == 3


def test_readline_uses_buffer_without_socket_read():
    connection, sock = make_connection(b"first\r\nsecond\r\n")
    assert connection.readline() == b"first"
    assert connection.hasline()
    assert connection.readline() == b"second"
    assert not connection.hasline()
    sock.recv.assert_called_once_with(4096)


def test_recv_combines_partial_payload_and_keeps_next_response():
    connection, sock = make_connection(b"cd", b"ef\r\nEND\r\n")
    connection.buffer = b"ab"
    assert connection.recv(8) == b"abcdef\r\n"
    assert connection.readline() == b"END"
    assert sock.recv.call_count == 2


def test_zero_length_recv_preserves_buffer():
    connection, sock = make_connection()
    connection.buffer = b"next response\r\n"
    assert connection.recv(0) == b""
    assert connection.buffer == b"next response\r\n"
    sock.recv.assert_not_called()


def test_readline_eof_discards_partial_header_and_disconnects():
    connection, sock = make_connection(b"VALUE key", b"")
    with pytest.raises(ArcusNodeConnectionException):
        connection.readline()
    assert connection.disconnected()
    assert connection.buffer == b""
    sock.close.assert_called_once_with()


def test_recv_eof_discards_partial_payload_and_disconnects():
    connection, sock = make_connection(b"ab", b"")
    with pytest.raises(ArcusNodeConnectionException):
        connection.recv(5)
    assert connection.disconnected()
    assert connection.buffer == b""
    sock.close.assert_called_once_with()


def test_disconnected_io_raises_connection_error():
    connection, sock = make_connection()
    connection.disconnect()
    for operation in (
        connection.readline,
        lambda: connection.recv(1),
        lambda: connection.send_request(b"get key"),
    ):
        with pytest.raises(ArcusNodeConnectionException):
            operation()
    sock.recv.assert_not_called()
    sock.sendall.assert_not_called()


def test_negative_recv_length_is_rejected():
    connection, sock = make_connection()
    with pytest.raises(ArcusProtocolException):
        connection.recv(-1)
    sock.recv.assert_not_called()


def test_get_and_gets_preserve_normal_miss():
    for method in ("_recv_value", "_recv_cas_value"):
        node, sock = make_node(b"END\r\n")
        assert getattr(node, method)() is None
        assert not node.handle.disconnected()
        sock.close.assert_not_called()


def test_get_decodes_partial_payload_with_embedded_crlf():
    node, sock = make_node(b"VALUE key 0 4\r\na\r", b"\nb\r", b"\nEND\r\n")
    assert node._recv_value() == "a\r\nb"
    assert node.handle.buffer == b""
    assert sock.recv.call_count == 3


def test_gets_preserves_cas_token_and_following_response():
    node, sock = make_node(b"VALUE key 0 3 42\r\nabc\r\nEND\r\nEND\r\n")
    assert node._recv_cas_value() == ("abc", b"42")
    assert node._recv_value() is None
    assert sock.recv.call_count == 1


def test_empty_value_is_distinct_from_miss():
    node, _ = make_node(b"VALUE key 0 0\r\n\r\nEND\r\n")
    assert node._recv_value() == ""


def test_unexpected_response_is_not_a_miss():
    for response in (b"ERROR", b"SERVER_ERROR unavailable", b"END extra", b""):
        for method in ("_recv_value", "_recv_cas_value"):
            node, _ = make_node(response + b"\r\n")
            with pytest.raises(ArcusProtocolException):
                getattr(node, method)()


def test_malformed_get_headers_raise_protocol_error():
    for header in (
        b"VALUE key 0",
        b"VALUE key 0 3 extra",
        b"VALUEextra key 0 3",
        b"VALUE key invalid 3",
        b"VALUE key 0 invalid",
        b"VALUE key -1 3",
        b"VALUE key 0 -1",
    ):
        node, _ = make_node(header + b"\r\n")
        with pytest.raises(ArcusProtocolException):
            node._recv_value()


def test_malformed_gets_headers_raise_protocol_error():
    for header in (b"VALUE key 0 3", b"VALUE key 0 3 invalid", b"VALUE key 0 3 -1"):
        node, _ = make_node(header + b"\r\n")
        with pytest.raises(ArcusProtocolException):
            node._recv_cas_value()


def test_wrong_payload_terminator_is_rejected():
    node, _ = make_node(b"VALUE key 0 3\r\nabcXXEND\r\n")
    with pytest.raises(ArcusProtocolException):
        node._recv_value()


def test_incorrect_payload_length_is_rejected():
    for length in (2, 4):
        node, _ = make_node(b"VALUE key 0 %d\r\nabc\r\nEND\r\n" % length)
        with pytest.raises(ArcusProtocolException):
            node._recv_value()


def test_wrong_response_terminator_is_rejected():
    node, _ = make_node(b"VALUE key 0 3\r\nabc\r\nSTORED\r\n")
    with pytest.raises(ArcusProtocolException):
        node._recv_value()


def test_payload_eof_raises_connection_error():
    node, sock = make_node(b"VALUE key 0 3\r\nab", b"")
    with pytest.raises(ArcusNodeConnectionException):
        node._recv_value()
    assert node.handle.disconnected()
    sock.close.assert_called_once_with()


def test_decode_failure_does_not_leave_end_for_the_next_operation():
    node, _ = make_node(b"VALUE key 0 1\r\n\xff\r\nEND\r\nEND\r\n")
    with pytest.raises(UnicodeDecodeError):
        node._recv_value()
    assert node._recv_value() is None


def make_operations(node):
    operations = [ArcusOperation(node, b"get key", node._recv_value) for _ in range(2)]
    node.ops = operations.copy()
    node._pending.update(operations)
    return operations


def test_buffered_responses_resolve_the_matching_operations():
    node, sock = make_node(
        b"VALUE first 0 3\r\none\r\nEND\r\nVALUE second 0 3\r\ntwo\r\nEND\r\n"
    )
    first, second = make_operations(node)
    node.do_op()
    assert first.get_result(timeout=0) == "one"
    assert second.get_result(timeout=0) == "two"
    assert node.ops == []
    assert not node._pending
    assert sock.recv.call_count == 1


def test_protocol_failure_fails_pending_operations():
    node, sock = make_node(b"VALUE key 0 -1\r\nEND\r\n")
    first, second = make_operations(node)
    node.do_op()
    with pytest.raises(ArcusProtocolException):
        first.get_result(timeout=0)
    with pytest.raises(ArcusNodeConnectionException):
        second.get_result(timeout=0)
    assert node.ops == []
    assert node.handle.disconnected()
    sock.close.assert_called_once_with()


def test_eof_fails_pending_operations():
    node, sock = make_node(b"")
    operations = make_operations(node)
    node.do_op()
    for operation in operations:
        with pytest.raises(ArcusNodeConnectionException):
            operation.get_result(timeout=0)
    sock.close.assert_called_once_with()


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("read timed out"),
        ConnectionResetError("connection reset"),
        OSError(errno.EIO, "read failed"),
    ],
)
def test_socket_errors_fail_pending_operations(error):
    node, sock = make_node(error)
    first, second = make_operations(node)
    node.do_op()
    with pytest.raises(type(error)) as raised:
        first.get_result(timeout=0)
    assert raised.value is error
    with pytest.raises(ArcusNodeConnectionException):
        second.get_result(timeout=0)
    assert node.ops == []
    assert node.handle.disconnected()
    sock.close.assert_called_once_with()


def test_unsolicited_buffered_response_discards_connection():
    node, sock = make_node(b"END\r\nEND\r\n")
    operation = ArcusOperation(node, b"get key", node._recv_value)
    node.ops = [operation]
    node._pending.add(operation)
    node.do_op()
    assert operation.get_result(timeout=0) is None
    assert node.handle.disconnected()
    assert node.handle.buffer == b""
    sock.close.assert_called_once_with()
