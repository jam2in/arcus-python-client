"""Socket bounds use real loopback sockets; Linux also exercises real epoll."""

import select
import socket
import threading
import time
from contextlib import contextmanager
from unittest.mock import Mock, patch

import pytest

from arcus import ArcusTranscoder
from arcus_mc_node import ArcusMCNodeAllocator, Connection


@contextmanager
def stalled_server(response=b"", *, drip=False):
    stop = threading.Event()
    received = threading.Event()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(2)
    host, port = listener.getsockname()

    def serve():
        try:
            peer, _ = listener.accept()
            with peer:
                peer.settimeout(2)
                peer.recv(4096)
                received.set()
                if drip:
                    while not stop.wait(0.01):
                        peer.sendall(b"x")
                elif response:
                    peer.sendall(response)
                stop.wait(2)
        except OSError:
            pass

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        yield f"{host}:{port}", received
    finally:
        stop.set()
        listener.close()
        thread.join(timeout=3)
        assert not thread.is_alive()


def test_connection_applies_separate_connect_and_io_timeouts():
    sock = Mock(spec=socket.socket)
    with patch("arcus.protocol.connection.socket.socket", return_value=sock):
        connection = Connection("127.0.0.1:11211", connect_timeout=0.2, io_timeout=0.3)
    assert sock.settimeout.call_args_list[0].args == (0.2,)
    assert sock.settimeout.call_args_list[1].args == (0.3,)
    connection.disconnect()


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), None])
def test_invalid_timeouts_do_not_allocate_socket(value):
    with patch("arcus.protocol.connection.socket.socket") as create:
        with pytest.raises((TypeError, ValueError)):
            Connection("127.0.0.1:11211", io_timeout=value)
    create.assert_not_called()


@pytest.mark.parametrize("response", [b"", b"VALUE key", b"VALUE key 0 9\r\nab"])
def test_real_socket_incomplete_response_has_io_bound(response):
    with stalled_server(response) as (address, received):
        connection = Connection(address, io_timeout=0.08)
        try:
            connection.send_request(b"get key")
            assert received.wait(1)
            started = time.monotonic()
            with pytest.raises(socket.timeout):
                if response.startswith(b"VALUE key 0"):
                    assert connection.readline() == b"VALUE key 0 9"
                    connection.recv(11)
                else:
                    connection.readline()
            assert time.monotonic() - started < 1
        finally:
            connection.disconnect()


def test_real_socket_trickling_response_cannot_extend_operation_deadline():
    with stalled_server(drip=True) as (address, received):
        connection = Connection(address, io_timeout=0.3)
        try:
            connection.send_request(b"get key")
            assert received.wait(1)
            connection.deadline = time.monotonic() + 0.12
            started = time.monotonic()
            with pytest.raises(socket.timeout):
                connection.readline()
            assert time.monotonic() - started < 0.8
        finally:
            connection.disconnect()


@pytest.mark.skipif(not hasattr(select, "epoll"), reason="requires Linux epoll")
@pytest.mark.parametrize("response", [b"", b"VALUE key", b"VALUE key 0 9\r\nab"])
def test_linux_epoll_terminates_silent_or_incomplete_operations(response):
    with stalled_server(response) as (address, received):
        allocator = ArcusMCNodeAllocator(
            ArcusTranscoder(), io_timeout=0.08, operation_timeout=0.15
        )
        node = allocator.alloc(address, "deadline")
        try:
            operation = node.get("key")
            assert received.wait(1)
            started = time.monotonic()
            with pytest.raises(socket.timeout):
                operation.get_result(timeout=1)
            assert time.monotonic() - started < 0.8
            assert node.handle.disconnected()
        finally:
            allocator.close()
        assert not allocator.worker.is_alive()
        assert not allocator.worker.poll.is_alive()
        assert allocator.worker.poll.epoll.closed


def test_reconnect_uses_the_remaining_operation_deadline():
    first = Mock(spec=socket.socket)
    replacement = Mock(spec=socket.socket)
    with patch(
        "arcus.protocol.connection.socket.socket", side_effect=[first, replacement]
    ):
        connection = Connection("127.0.0.1:11211", connect_timeout=1)
        connection.deadline = 10.05
        with patch("arcus.protocol.connection.time.monotonic", return_value=10):
            connection.connect()
    assert replacement.settimeout.call_args_list[0].args[0] == pytest.approx(0.05)
    connection.disconnect()


def test_reconnect_consuming_operation_deadline_cannot_send_request():
    first = Mock(spec=socket.socket)
    replacement = Mock(spec=socket.socket)
    with patch(
        "arcus.protocol.connection.socket.socket", side_effect=[first, replacement]
    ):
        connection = Connection("127.0.0.1:11211")
        connection.deadline = 10.05
        with patch("arcus.protocol.connection.time.monotonic", side_effect=[10, 10.06]):
            connection.connect()
            with pytest.raises(socket.timeout):
                connection.send_request(b"incr counter 1")
    replacement.sendall.assert_not_called()
    connection.disconnect()
