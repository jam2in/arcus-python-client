import errno
import queue
import selectors
import socket
import threading
from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest

from arcus import (
    ArcusNodeConnectionException,
    ArcusOperation,
    ArcusProtocolException,
    ArcusTranscoder,
)
from arcus.protocol import ArcusMCNode, ArcusMCWorker
from arcus.protocol.worker import ArcusMCPoll


def make_worker():
    worker = ArcusMCWorker.__new__(ArcusMCWorker)
    worker.node_allocator = SimpleNamespace(shutdown=False, worker=worker)
    worker.poll = Mock()
    worker.q = queue.Queue()
    return worker


def drain_worker(worker):
    get = worker.q.get

    def take():
        try:
            return get(block=False)
        except queue.Empty:
            worker.node_allocator.shutdown = True
            return None

    with patch.object(worker.q, "get", side_effect=take):
        worker.run()
    worker.poll.join.assert_called_once_with()


def test_invalidated_queued_request_cannot_supply_a_later_requests_result():
    worker = make_worker()
    original = Mock(spec=socket.socket)
    original.recv.return_value = b"INVALID\r\n"
    replacement = Mock(spec=socket.socket)
    responses = deque()

    def reply_to_get(request):
        command, key = request.strip().split()
        assert command == b"get"
        responses.append(
            b"VALUE "
            + key
            + b" 0 "
            + str(len(key)).encode()
            + b"\r\n"
            + key
            + b"\r\nEND\r\n"
        )

    replacement.sendall.side_effect = reply_to_get
    replacement.recv.side_effect = lambda size: responses.popleft()

    with patch(
        "arcus.protocol.connection.socket.socket", side_effect=[original, replacement]
    ):
        node = ArcusMCNode(
            "127.0.0.1:11211", "test", ArcusTranscoder(), worker.node_allocator
        )
        first = node.get("first")
        stale = node.get("second")
        node.process_operation(worker.q.get_nowait())
        node.do_op()
        with pytest.raises(ArcusProtocolException):
            first.get_result(timeout=0)
        with pytest.raises(ArcusNodeConnectionException):
            stale.get_result(timeout=0)

        fresh = node.get("third")
        process_request = node.process_request

        def process_and_read(request):
            process_request(request)
            node.do_op()

        with patch.object(node, "process_request", side_effect=process_and_read):
            drain_worker(worker)

    assert fresh.get_result(timeout=0.1) == "third"
    replacement.sendall.assert_called_once_with(b"get third\r\n")
    assert not responses


def test_worker_sends_pending_operations_but_skips_failed_and_completed_ones():
    worker = make_worker()
    node = Mock()
    failed = ArcusOperation(node, b"get stale", None)
    failed.set_exception(ArcusNodeConnectionException())
    completed = ArcusOperation(node, b"get completed", None)
    completed.set_result("done")
    pending = ArcusOperation(node, b"get fresh", None)
    for operation in (failed, completed, pending):
        worker.q.put(operation)

    drain_worker(worker)

    assert node.process_operation.call_args_list == [call(pending)]


def make_poll():
    allocator = SimpleNamespace(shutdown=False)
    poll = ArcusMCPoll.__new__(ArcusMCPoll)
    poll.node_allocator = allocator
    poll.selector = Mock()
    poll.lock = threading.Lock()
    node = Mock()
    node._io_lock = threading.Lock()
    node.ops = []
    sock = node.handle.socket
    key = selectors.SelectorKey(sock, 7, selectors.EVENT_READ, node)
    poll.selector.get_key.return_value = key
    poll.sock_node_map = {7: node}
    node.disconnect.side_effect = lambda: poll.unregister_node(node)
    events = [[(key, selectors.EVENT_READ)]]

    def select(timeout):
        if events:
            return events.pop()
        allocator.shutdown = True
        return []

    poll.selector.select.side_effect = select
    return poll, node


@pytest.mark.parametrize(
    "unregister_error",
    [
        KeyError(7),
        ValueError("closed"),
        OSError(errno.EBADF, "closed"),
        OSError(errno.ENOENT, "removed"),
    ],
)
def test_poll_dispatch_can_disconnect_and_tolerates_removed_descriptor(
    unregister_error,
):
    poll, node = make_poll()
    node.do_op.side_effect = lambda expected_socket: node.disconnect()
    poll.selector.unregister.side_effect = unregister_error

    poll.run()

    node.disconnect.assert_called_once_with()
    node.do_op.assert_called_once_with(expected_socket=node.handle.socket)
    assert not poll.sock_node_map
    poll.selector.close.assert_called_once_with()


def test_poll_does_not_dispatch_events_after_shutdown():
    poll, node = make_poll()

    def select(timeout):
        assert timeout == 2
        poll.node_allocator.shutdown = True
        key = poll.selector.get_key.return_value
        return [(key, selectors.EVENT_READ)]

    poll.selector.select.side_effect = select

    poll.run()

    node.do_op.assert_not_called()
    node.disconnect.assert_not_called()
    poll.selector.close.assert_called_once_with()


def test_poll_does_not_hide_unexpected_unregister_errors():
    poll, node = make_poll()
    node.do_op.side_effect = lambda expected_socket: node.disconnect()
    poll.selector.unregister.side_effect = OSError(errno.EIO, "unexpected error")

    with pytest.raises(OSError) as raised:
        poll.run()

    assert raised.value.errno == errno.EIO
    poll.selector.close.assert_called_once_with()


@pytest.mark.parametrize("replacement_node", [False, True])
def test_poll_ignores_events_from_replaced_registration(replacement_node):
    poll, node = make_poll()
    if replacement_node:
        poll.sock_node_map[7] = Mock()
    else:
        poll.selector.get_key.return_value = selectors.SelectorKey(
            node.handle.socket, 7, selectors.EVENT_READ, node
        )

    poll.run()

    node.do_op.assert_not_called()
    node.disconnect.assert_not_called()
    node.handle.socket.recv.assert_not_called()


def test_poll_reads_without_holding_registration_or_write_lock():
    poll, node = make_poll()
    node.ops = [Mock()]

    def read_response(expected_socket):
        assert expected_socket is node.handle.socket
        assert poll.lock.acquire(blocking=False)
        poll.lock.release()
        assert node._io_lock.acquire(blocking=False)
        node._io_lock.release()
        node.disconnect()

    node.do_op.side_effect = read_response

    poll.run()

    node.do_op.assert_called_once_with(expected_socket=node.handle.socket)
    node.handle.socket.recv.assert_not_called()
    poll.selector.unregister.assert_called_once_with(7)


def test_stale_disconnect_does_not_interrupt_a_replacement_socket():
    original = Mock(spec=socket.socket)
    replacement = Mock(spec=socket.socket)

    class ReconnectingHandle:
        def __init__(self):
            self.first_access = True
            self.disconnect = Mock()

        @property
        def socket(self):
            # Simulate replacement immediately after the caller observes the
            # old socket, before it interrupts I/O or acquires the write lock.
            if self.first_access:
                self.first_access = False
                return original
            return replacement

    node = ArcusMCNode.__new__(ArcusMCNode)
    node.handle = ReconnectingHandle()
    node._io_lock = threading.RLock()
    node.node_allocator = SimpleNamespace(worker=SimpleNamespace(poll=Mock()))

    node.disconnect(expected_socket=original)

    original.shutdown.assert_called_once_with(socket.SHUT_RDWR)
    replacement.shutdown.assert_not_called()
    node.handle.disconnect.assert_not_called()
    node.node_allocator.worker.poll.unregister_node.assert_not_called()
