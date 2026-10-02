import queue
import socket
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from arcus import ArcusNodeConnectionException, ArcusTranscoder
from arcus.protocol import ArcusMCNode, ArcusMCNodeAllocator, ArcusMCPoll
from arcus.protocol.request import CommandRequest


def make_node(sock=None):
    sock = sock or Mock(spec=socket.socket)
    allocator = SimpleNamespace(
        shutdown=False,
        operation_timeout=0.2,
        worker=SimpleNamespace(q=queue.Queue(), poll=Mock(), register_node=Mock()),
        forget=Mock(),
    )
    with patch("arcus.protocol.connection.socket.socket", return_value=sock):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)
    return node, sock, allocator


def test_partial_send_failure_invalidates_all_pending_and_never_retries():
    node, sock, allocator = make_node()
    sock.sendall.side_effect = socket.timeout("partial write")
    first = node.commands.kv.get("first")
    second = node.commands.kv.get("second")
    node.process_operation(first)
    node.process_operation(second)
    with pytest.raises(socket.timeout):
        first.get_result(timeout=0.1)
    with pytest.raises(ArcusNodeConnectionException):
        second.get_result(timeout=0.1)
    sock.sendall.assert_called_once_with(b"get first\r\n")
    assert node.ops == []
    assert not node._pending
    assert node.handle.disconnected()
    allocator.worker.poll.unregister_node.assert_called_once_with(node)


def test_disconnect_between_dequeue_and_send_cannot_send_stale_operation():
    node, sock, allocator = make_node()
    operation = node.commands.kv.get("stale")
    dequeued = allocator.worker.q.get_nowait()
    started = threading.Event()

    def send():
        started.set()
        node.process_operation(dequeued)

    with node._io_lock:
        sender = threading.Thread(target=send)
        sender.start()
        assert started.wait(1)
        node.disconnect()
    sender.join(timeout=1)
    assert not sender.is_alive()
    sock.sendall.assert_not_called()
    assert operation.invalid


def test_noreply_reports_send_failure_instead_of_premature_success():
    node, sock, _ = make_node()
    sock.sendall.side_effect = BrokenPipeError("closed")
    operation = node.submit(
        CommandRequest("command", b"command noreply", None, noreply=True)
    )
    assert not operation.has_result()
    node.process_operation(operation)
    with pytest.raises(BrokenPipeError):
        operation.get_result(timeout=0.1)


def test_noreply_completes_after_write_without_waiting_for_response():
    node, sock, _ = make_node()
    operation = node.submit(
        CommandRequest("command", b"command noreply", None, noreply=True)
    )
    node.process_operation(operation)
    assert operation.get_result(timeout=0.1) is True
    assert not node._pending
    assert node.ops == []
    sock.recv.assert_not_called()


def test_queue_deadline_expires_before_a_write():
    node, sock, _ = make_node()
    operation = node.commands.kv.get("expired")
    operation.deadline = time.monotonic() - 1
    node.process_operation(operation)
    with pytest.raises(socket.timeout):
        operation.get_result(timeout=0.1)
    sock.sendall.assert_not_called()
    assert node.handle.disconnected()


def test_silent_deadline_invalidates_all_response_slots():
    node, sock, _ = make_node()
    first = node.commands.kv.get("first")
    second = node.commands.kv.get("second")
    node.process_operation(first)
    first.deadline = time.monotonic() - 1
    node.expire_operations()
    for operation in (first, second):
        with pytest.raises(socket.timeout):
            operation.get_result(timeout=0.1)
    node.process_operation(second)
    sock.sendall.assert_called_once_with(b"get first\r\n")
    assert node.ops == []


def test_retired_node_rejects_new_requests_and_releases_tracking():
    node, sock, allocator = make_node()
    pending = node.commands.kv.get("old")
    node.close()
    fresh = node.commands.kv.get("new")
    for operation in (pending, fresh):
        with pytest.raises(ArcusNodeConnectionException):
            operation.get_result(timeout=0.1)
    sock.sendall.assert_not_called()
    allocator.forget.assert_called_once_with(node)


def test_reconnect_unregisters_old_descriptor_and_preserves_fresh_result():
    node, original, allocator = make_node()
    old = node.commands.kv.get("old")
    node.disconnect()
    replacement = Mock(spec=socket.socket)
    replacement.recv.return_value = b"VALUE fresh 0 2\r\nok\r\nEND\r\n"
    fresh = node.commands.kv.get("fresh")
    with patch("arcus.protocol.connection.socket.socket", return_value=replacement):
        node.process_operation(old)
        node.process_operation(fresh)
    node.do_op()
    assert fresh.get_result(timeout=0.1) == "ok"
    original.sendall.assert_not_called()
    replacement.sendall.assert_called_once_with(b"get fresh\r\n")
    allocator.worker.register_node.assert_called_once_with(node)


class FakeEpoll:
    def __init__(self):
        self.closed = False
        self.registered = set()

    def poll(self, timeout):
        time.sleep(min(timeout, 0.005))
        return []

    def register(self, descriptor, mask):
        self.registered.add(descriptor)

    def unregister(self, descriptor):
        self.registered.discard(descriptor)

    def close(self):
        self.closed = True


@pytest.mark.parametrize("allocate_node", [False, True])
def test_allocator_close_stops_threads_closes_epoll_and_allows_restart(allocate_node):
    sock = Mock(spec=socket.socket)
    sock.fileno.return_value = 7
    with (
        patch("arcus.protocol.worker.select.epoll", FakeEpoll, create=True),
        patch.multiple(
            "arcus.protocol.worker.select",
            EPOLLIN=1,
            EPOLLHUP=16,
            EPOLLERR=8,
            create=True,
        ),
        patch("arcus.protocol.connection.socket.socket", return_value=sock),
    ):
        allocator = ArcusMCNodeAllocator(ArcusTranscoder())
        if allocate_node:
            allocator.alloc("127.0.0.1:11211", "test")
        original_worker = allocator.worker
        allocator.close()
        allocator.close()
        assert not original_worker.is_alive()
        assert not original_worker.poll.is_alive()
        assert original_worker.poll.epoll.closed
        assert not original_worker.poll.sock_node_map
        assert not allocator.snapshot_nodes()
        assert original_worker.q.empty()
        with pytest.raises(ArcusNodeConnectionException):
            allocator.alloc("127.0.0.1:11211", "closed")
        allocator.start()
        assert allocator.worker is not original_worker
        assert allocator.worker.is_alive()
        allocator.close()
        assert not allocator.worker.is_alive()


def test_poll_registration_replaces_the_previous_descriptor_for_a_node():
    allocator = SimpleNamespace(shutdown=False)
    with (
        patch("arcus.protocol.worker.select.epoll", FakeEpoll, create=True),
        patch.multiple(
            "arcus.protocol.worker.select",
            EPOLLIN=1,
            EPOLLHUP=16,
            EPOLLERR=8,
            create=True,
        ),
    ):
        poll = ArcusMCPoll(allocator)
        node = Mock()
        node.handle.disconnected.return_value = False
        node.get_fileno.return_value = 7
        poll.register_node(node)
        node.get_fileno.return_value = 9
        poll.register_node(node)
        assert poll.sock_node_map == {9: node}
        assert poll.epoll.registered == {9}
        poll.unregister_node(node)
        assert not poll.sock_node_map
        assert not poll.epoll.registered


@pytest.mark.parametrize("failure_path", ["send", "receive", "expire"])
def test_transport_is_closed_before_publishing_fatal_operation_results(failure_path):
    node, sock, allocator = make_node()
    first = node.commands.kv.get("first")
    second = node.commands.kv.get("second")
    published = []

    def check_publication(operation):
        publish = operation.set_result

        def complete(result):
            assert node.handle.disconnected()
            assert not node.ops
            assert not node._pending
            allocator.worker.poll.unregister_node.assert_called_once_with(node)
            published.append(operation)
            publish(result)

        return complete

    with (
        patch.object(first, "set_result", side_effect=check_publication(first)),
        patch.object(second, "set_result", side_effect=check_publication(second)),
    ):
        if failure_path == "send":
            sock.sendall.side_effect = socket.timeout("partial send")
            node.process_operation(first)
        elif failure_path == "receive":
            sock.recv.side_effect = socket.timeout("partial response")
            node.do_op()
        else:
            first.deadline = time.monotonic() - 1
            node.expire_operations()

    assert first in published
    with pytest.raises(socket.timeout):
        first.get_result(timeout=0.1)
    if failure_path == "expire":
        assert second in published
        with pytest.raises(socket.timeout):
            second.get_result(timeout=0.1)
    else:
        with pytest.raises(ArcusNodeConnectionException):
            second.get_result(timeout=0.1)
