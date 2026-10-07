import queue
import socket
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from arcus import (
    ArcusNodeConnectionException,
    ArcusProtocolException,
    ArcusTranscoder,
)
from arcus.protocol import ArcusMCNode
from arcus.protocol.connection import Connection
from arcus.protocol.worker import ArcusMCPoll


def test_selector_drains_buffered_responses_and_fails_pending_requests_on_eof():
    client_socket, server_socket = socket.socketpair()
    allocator = SimpleNamespace(shutdown=False)
    poll = ArcusMCPoll(allocator)
    poll.daemon = True
    allocator.worker = SimpleNamespace(
        q=queue.Queue(), poll=poll, register_node=poll.register_node
    )

    def connect(connection):
        connection.socket = client_socket
        return client_socket

    with patch.object(Connection, "connect", connect):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)

    try:
        operations = [node.get(key) for key in ("first", "second", "third", "fourth")]
        for _ in operations:
            node.process_operation(allocator.worker.q.get_nowait())
        poll.register_node(node)

        # A single response stream contains two results, then ends while two
        # already sent requests are still waiting for their responses.
        server_socket.sendall(
            b"VALUE first 0 3\r\none\r\nEND\r\nVALUE second 0 3\r\ntwo\r\nEND\r\n"
        )
        server_socket.shutdown(socket.SHUT_WR)
        poll.start()

        assert operations[0].get_result(timeout=3) == "one"
        assert operations[1].get_result(timeout=3) == "two"
        for operation in operations[2:]:
            with pytest.raises(ArcusNodeConnectionException):
                operation.get_result(timeout=3)
        assert not node.ops
        assert node.handle.disconnected()
    finally:
        allocator.shutdown = True
        server_socket.close()
        node.disconnect()
        if poll.ident is not None:
            poll.join(timeout=3)
        else:
            poll.selector.close()
        client_socket.close()

    assert not poll.is_alive()


@pytest.mark.parametrize("malformed_response", [False, True])
def test_sender_and_receiver_progress_under_bidirectional_backpressure(
    malformed_response,
):
    client_socket, server_socket = socket.socketpair()
    for sock in (client_socket, server_socket):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)

    sending_request = threading.Event()
    sending_response = threading.Event()
    observed_socket = Mock(wraps=client_socket)

    def sendall(request):
        if request.startswith(b"set second "):
            sending_request.set()
        client_socket.sendall(request)

    observed_socket.sendall.side_effect = sendall
    allocator = SimpleNamespace(shutdown=False)
    poll = ArcusMCPoll(allocator)
    poll.daemon = True
    allocator.worker = SimpleNamespace(
        q=queue.Queue(), poll=poll, register_node=poll.register_node
    )

    def connect(connection):
        connection.socket = observed_socket
        return observed_socket

    with patch.object(Connection, "connect", connect):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)

    payload = b"x" * (1024 * 1024)
    first = node.get("first")
    second = node.set("second", payload)
    server_errors = []

    def receive_request(expected):
        received = b""
        while len(received) < len(expected):
            chunk = server_socket.recv(len(expected) - len(received))
            if not chunk:
                raise ConnectionError("client closed before sending its request")
            received += chunk
        assert received == expected

    def serve():
        try:
            receive_request(first.request + b"\r\n")
            sending_response.set()
            if malformed_response:
                # Leave the large write blocked: detecting a malformed response
                # must interrupt the write before waiting for its lock.
                server_socket.sendall(b"INVALID\r\n")
                return
            server_socket.sendall(
                f"VALUE first {ArcusTranscoder.FLAG_BYTEARRAY} {len(payload)}\r\n".encode()
                + payload
                + b"\r\nEND\r\n"
            )
            # The server finishes the first response before reading the next
            # request. Both payloads exceed the socket buffers.
            receive_request(second.request + b"\r\n")
            server_socket.sendall(b"STORED\r\n")
        except (OSError, AssertionError) as error:
            server_errors.append(error)

    server_thread = threading.Thread(target=serve, daemon=True)
    writer = threading.Thread(
        target=node.process_operation, args=(second,), daemon=True
    )
    try:
        poll.register_node(node)
        node.process_operation(allocator.worker.q.get_nowait())
        assert allocator.worker.q.get_nowait() is second
        server_thread.start()
        assert sending_response.wait(timeout=3)
        writer.start()
        assert sending_request.wait(timeout=3)
        # Start reading only after the writer enters sendall. A shared I/O lock
        # would now prevent either side from draining the other's socket buffer.
        poll.start()

        if malformed_response:
            for operation in (first, second):
                with pytest.raises(
                    (ArcusNodeConnectionException, ArcusProtocolException, OSError)
                ):
                    operation.get_result(timeout=3)
            assert node.handle.disconnected()
        else:
            assert first.get_result(timeout=3) == payload
            assert second.get_result(timeout=3) is True
        server_thread.join(timeout=3)
        writer.join(timeout=3)
        assert not server_thread.is_alive()
        assert not writer.is_alive()
        assert not server_errors
    finally:
        allocator.shutdown = True
        try:
            client_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        server_socket.close()
        for thread in (server_thread, writer, poll):
            if thread.ident is not None:
                thread.join(timeout=3)
        node.disconnect()
        if poll.ident is None:
            poll.selector.close()
        client_socket.close()

    assert not poll.is_alive()


def test_disconnect_interrupts_a_partial_response_read():
    client_socket, server_socket = socket.socketpair()
    observed_socket = Mock(wraps=client_socket)
    waiting_for_more = threading.Event()
    recv_count = 0

    def recv(*args):
        nonlocal recv_count
        recv_count += 1
        if recv_count == 2:
            waiting_for_more.set()
        return client_socket.recv(*args)

    observed_socket.recv.side_effect = recv
    allocator = SimpleNamespace(shutdown=False)
    poll = ArcusMCPoll(allocator)
    poll.daemon = True
    allocator.worker = SimpleNamespace(
        q=queue.Queue(), poll=poll, register_node=poll.register_node
    )

    def connect(connection):
        connection.socket = observed_socket
        return observed_socket

    with patch.object(Connection, "connect", connect):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)

    disconnect_thread = threading.Thread(target=node.disconnect, daemon=True)
    try:
        operation = node.get("partial")
        node.process_operation(allocator.worker.q.get_nowait())
        poll.register_node(node)
        server_socket.sendall(b"VALUE partial 0 5\r\nab")
        poll.start()
        assert waiting_for_more.wait(timeout=3)

        disconnect_thread.start()
        disconnect_thread.join(timeout=3)

        assert not disconnect_thread.is_alive()
        with pytest.raises(ArcusNodeConnectionException):
            operation.get_result(timeout=0)
        assert node.handle.disconnected()
    finally:
        allocator.shutdown = True
        # Release the blocking read even if the regression reappears, so a
        # failing assertion cannot leave either test thread running forever.
        try:
            client_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        server_socket.close()
        if disconnect_thread.ident is not None:
            disconnect_thread.join(timeout=3)
        node.disconnect()
        if poll.ident is not None:
            poll.join(timeout=3)
        else:
            poll.selector.close()
        client_socket.close()

    assert not poll.is_alive()
