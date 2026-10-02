"""Cleanup errors must not strand asynchronous results or hide their failure."""

import queue
import socket
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from arcus import ArcusNodeConnectionException, ArcusProtocolException, ArcusTranscoder
from arcus.protocol.node import ArcusMCNode


def make_node():
    sock = Mock(spec=socket.socket)
    allocator = SimpleNamespace(
        shutdown=False,
        operation_timeout=2,
        worker=SimpleNamespace(q=queue.Queue(), poll=Mock(), register_node=Mock()),
        forget=Mock(),
    )
    with patch("arcus.protocol.connection.socket.socket", return_value=sock):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)
    return node, sock, allocator


def test_disconnect_attempts_all_cleanup_and_invalidates_pending_after_errors():
    node, sock, allocator = make_node()
    unregister_error = OSError("unregister failed")
    allocator.worker.poll.unregister_node.side_effect = unregister_error
    sock.close.side_effect = OSError("socket close failed")
    operations = [node.commands.kv.get("first"), node.commands.kv.get("second")]
    generation = node._generation
    with pytest.raises(OSError) as raised:
        node.disconnect()
    assert raised.value is unregister_error
    sock.close.assert_called_once_with()
    assert node.handle.disconnected()
    assert node._generation == generation + 1
    assert not node.ops
    assert not node._pending
    for operation in operations:
        with pytest.raises(ArcusNodeConnectionException):
            operation.get_result(timeout=0.1)


def test_parse_error_survives_teardown_error_and_completes_every_operation():
    node, sock, allocator = make_node()
    sock.recv.return_value = b"VALUE invalid header\r\n"
    allocator.worker.poll.unregister_node.side_effect = OSError("unregister failed")
    first, second = node.commands.kv.get("first"), node.commands.kv.get("second")
    node.process_operation(first)
    node.process_operation(second)
    node.do_op()
    with pytest.raises(ArcusProtocolException):
        first.get_result(timeout=0.1)
    with pytest.raises(ArcusNodeConnectionException):
        second.get_result(timeout=0.1)
    sock.close.assert_called_once_with()
    assert not node._pending


def test_write_error_survives_teardown_error():
    node, sock, allocator = make_node()
    send_error = BrokenPipeError("write failed")
    sock.sendall.side_effect = send_error
    allocator.worker.poll.unregister_node.side_effect = OSError("unregister failed")
    operation = node.commands.kv.get("first")
    node.process_operation(operation)
    with pytest.raises(BrokenPipeError) as raised:
        operation.get_result(timeout=0.1)
    assert raised.value is send_error
    sock.close.assert_called_once_with()
    assert not node._pending


def test_expired_operations_finish_even_when_disconnect_raises():
    node, sock, allocator = make_node()
    allocator.worker.poll.unregister_node.side_effect = OSError("unregister failed")
    first, second = node.commands.kv.get("first"), node.commands.kv.get("second")
    first.deadline = time.monotonic() - 1
    with pytest.raises(OSError, match="unregister failed"):
        node.expire_operations()
    for operation in (first, second):
        with pytest.raises(socket.timeout):
            operation.get_result(timeout=0.1)
    sock.close.assert_called_once_with()
    assert not node._pending


def test_failed_node_close_retains_allocator_ownership_for_cleanup_retry():
    node, sock, allocator = make_node()
    allocator.worker.poll.unregister_node.side_effect = OSError("unregister failed")
    with pytest.raises(OSError, match="unregister failed"):
        node.close()
    allocator.forget.assert_not_called()
    assert node._closed
    sock.close.assert_called_once_with()
