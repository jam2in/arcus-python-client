import queue
import selectors
import socket
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from arcus import ArcusNodeConnectionException, ArcusTranscoder
from arcus.protocol import ArcusMCNode, ArcusMCNodeAllocator, ArcusMCPoll


def make_node(sock=None):
    sock = sock or Mock(spec=socket.socket)
    allocator = SimpleNamespace(
        shutdown=False,
        worker=SimpleNamespace(q=queue.Queue(), poll=Mock(), register_node=Mock()),
    )
    with patch("arcus.protocol.connection.socket.socket", return_value=sock):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)
    return node, sock, allocator


def test_partial_send_failure_invalidates_all_pending_and_never_retries():
    node, sock, allocator = make_node()
    sock.sendall.side_effect = BrokenPipeError("partial write")
    first = node.get("first")
    second = node.get("second")
    node.process_operation(first)
    node.process_operation(second)
    with pytest.raises(BrokenPipeError):
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
    operation = node.get("stale")
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
    with pytest.raises(ArcusNodeConnectionException):
        operation.get_result(timeout=0)


def test_submitting_operation_does_not_wait_for_in_progress_io():
    node, sock, allocator = make_node()
    returned = threading.Event()
    operations = []

    def submit():
        operations.append(node.get("next"))
        returned.set()

    # A sender can hold this lock while waiting for socket capacity.
    with node._io_lock:
        submitter = threading.Thread(target=submit)
        submitter.start()
        returned_before_release = returned.wait(1)
    submitter.join(timeout=1)

    assert not submitter.is_alive()
    assert returned_before_release
    assert allocator.worker.q.get_nowait() is operations[0]
    sock.sendall.assert_not_called()


def test_reader_failure_blocks_queued_sends_until_pending_operations_are_failed():
    node, sock, _ = make_node()
    first = node.get("first")
    stale = node.get("stale")
    node.process_operation(first)
    sock.recv.return_value = b""
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    fail_operation = node._fail_operation

    def delayed_cleanup(operation, error):
        cleanup_started.set()
        assert release_cleanup.wait(2)
        fail_operation(operation, error)

    replacement = Mock(spec=socket.socket)
    with (
        patch.object(node, "_fail_operation", side_effect=delayed_cleanup),
        patch(
            "arcus.protocol.connection.socket.socket", return_value=replacement
        ) as create_socket,
    ):
        reader = threading.Thread(target=node.do_op)
        reader.start()
        try:
            assert cleanup_started.wait(1)
            assert node.handle.disconnected()
            assert node._read_lock.acquire(blocking=False)
            node._read_lock.release()

            node.process_operation(stale)

            create_socket.assert_not_called()
            replacement.sendall.assert_not_called()
            assert not stale.has_result()
        finally:
            release_cleanup.set()
            reader.join(timeout=1)

    assert not reader.is_alive()
    sock.sendall.assert_called_once_with(b"get first\r\n")
    for operation in (first, stale):
        with pytest.raises(ArcusNodeConnectionException):
            operation.get_result(timeout=0)


def test_reconnect_unregisters_old_descriptor_and_preserves_fresh_result():
    node, original, allocator = make_node()
    old = node.get("old")
    node.disconnect()
    replacement = Mock(spec=socket.socket)
    replacement.recv.return_value = b"VALUE fresh 0 2\r\nok\r\nEND\r\n"
    fresh = node.get("fresh")
    with patch("arcus.protocol.connection.socket.socket", return_value=replacement):
        node.process_operation(old)
        node.process_operation(fresh)
    node.do_op()
    assert fresh.get_result(timeout=0.1) == "ok"
    original.sendall.assert_not_called()
    replacement.sendall.assert_called_once_with(b"get fresh\r\n")
    allocator.worker.register_node.assert_called_once_with(node)


class FakeSelector:
    def __init__(self):
        self.closed = False
        self.registered = {}

    def select(self, timeout):
        time.sleep(min(timeout, 0.005))
        return []

    def register(self, fileobj, events, data=None):
        descriptor = fileobj if isinstance(fileobj, int) else fileobj.fileno()
        if descriptor in self.registered:
            raise KeyError(descriptor)
        key = selectors.SelectorKey(fileobj, descriptor, events, data)
        self.registered[descriptor] = key
        return key

    def unregister(self, fileobj):
        descriptor = fileobj if isinstance(fileobj, int) else fileobj.fileno()
        return self.registered.pop(descriptor)

    def get_key(self, fileobj):
        descriptor = fileobj if isinstance(fileobj, int) else fileobj.fileno()
        return self.registered[descriptor]

    def get_map(self):
        return self.registered

    def close(self):
        self.closed = True
        self.registered.clear()


def test_allocator_initializes_shutdown_before_starting_worker_threads():
    worker = Mock()

    def create_worker(allocator):
        # The worker constructor starts the poller, which reads this immediately.
        assert allocator.shutdown is False
        return worker

    with patch("arcus.protocol.allocator.ArcusMCWorker", side_effect=create_worker):
        allocator = ArcusMCNodeAllocator(ArcusTranscoder())

    assert allocator.worker is worker
    worker.start.assert_called_once_with()


def test_poll_registration_replaces_the_previous_descriptor_for_a_node():
    allocator = SimpleNamespace(shutdown=False)
    with patch("arcus.protocol.worker.selectors.DefaultSelector", FakeSelector):
        poll = ArcusMCPoll(allocator)
        sock = Mock(spec=socket.socket)
        sock.fileno.return_value = 7
        node = SimpleNamespace(
            handle=SimpleNamespace(socket=sock), _io_lock=threading.RLock()
        )
        poll.register_node(node)
        sock.fileno.return_value = 9
        poll.register_node(node)
        assert poll.sock_node_map == {9: node}
        assert set(poll.selector.registered) == {9}
        poll.unregister_node(node)
        assert not poll.sock_node_map
        assert not poll.selector.registered
        poll.selector.close()


@pytest.mark.parametrize("failure_path", ["send", "receive"])
def test_transport_is_closed_before_publishing_fatal_operation_results(failure_path):
    node, sock, allocator = make_node()
    first = node.get("first")
    second = node.get("second")
    published = []

    def check_publication(operation):
        publish = operation.set_exception

        def complete(result):
            assert node.handle.disconnected()
            assert not node.ops
            assert not node._pending
            allocator.worker.poll.unregister_node.assert_called_once_with(node)
            published.append(operation)
            publish(result)

        return complete

    with (
        patch.object(first, "set_exception", side_effect=check_publication(first)),
        patch.object(second, "set_exception", side_effect=check_publication(second)),
    ):
        if failure_path == "send":
            sock.sendall.side_effect = ConnectionResetError("partial send")
            node.process_operation(first)
        else:
            sock.recv.side_effect = ConnectionResetError("partial response")
            node.do_op()

    assert first in published
    with pytest.raises(ConnectionResetError):
        first.get_result(timeout=0.1)
    assert second in published
    with pytest.raises(ArcusNodeConnectionException):
        second.get_result(timeout=0.1)
