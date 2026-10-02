"""Existing node statistics commands must return a completed operation."""

import queue
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from arcus import ArcusProtocolException, ArcusTranscoder
from arcus.protocol import ArcusMCNode


@pytest.mark.parametrize("arguments", [None, "items"])
def test_stats_returns_all_lines_and_leaves_the_next_response(arguments):
    sock = Mock()
    sock.recv.return_value = b"STAT hits 3\r\nSTAT misses 2\r\nEND\r\nSTORED\r\n"
    allocator = SimpleNamespace(shutdown=False, worker=Mock(q=queue.Queue()))
    with patch("arcus.protocol.connection.socket.socket", return_value=sock):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)
    stats = node.commands.admin.get_stats(arguments)
    stored = node.commands.kv.set("key", "value")
    node.process_operation(stats)
    node.process_operation(stored)
    node.do_op()
    assert stats.get_result(timeout=0.1) == {"hits": "3", "misses": "2"}
    assert stored.get_result(timeout=0.1) is True
    assert sock.sendall.call_args_list[0].args == (
        b"stats\r\n" if arguments is None else b"stats items\r\n",
    )


def test_malformed_stats_response_closes_the_connection():
    sock = Mock()
    sock.recv.return_value = b"STAT missing-value\r\n"
    allocator = SimpleNamespace(shutdown=False, worker=Mock(q=queue.Queue()))
    with patch("arcus.protocol.connection.socket.socket", return_value=sock):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)
    stats = node.commands.admin.get_stats()
    node.process_operation(stats)
    node.do_op()
    with pytest.raises(ArcusProtocolException):
        stats.get_result(timeout=0.1)
    assert node.handle.disconnected()
