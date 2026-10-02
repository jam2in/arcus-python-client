import queue
import socket
from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from arcus import (
    ArcusNodeConnectionException,
    ArcusOperation,
    ArcusProtocolException,
    ArcusTranscoder,
)
from arcus.protocol import ArcusMCNode, ArcusMCWorker


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
        first = node.commands.kv.get("first")
        stale = node.commands.kv.get("second")
        node.process_request(worker.q.get_nowait().request)
        node.do_op()
        assert isinstance(first.result, ArcusProtocolException)
        assert isinstance(stale.result, ArcusNodeConnectionException)
        assert stale.invalid

        fresh = node.commands.kv.get("third")
        process_request = node.process_request

        def process_and_read(request):
            process_request(request)
            node.do_op()

        with patch.object(node, "process_request", side_effect=process_and_read):
            drain_worker(worker)

    assert fresh.get_result(timeout=0.1) == "third"
    replacement.sendall.assert_called_once_with(b"get third\r\n")
    assert not responses


def test_worker_sends_pending_and_noreply_operations_but_skips_invalidated_ones():
    worker = make_worker()
    node = Mock()
    invalid = ArcusOperation(node, b"get stale", None)
    invalid.set_invalid()
    pending = ArcusOperation(node, b"get fresh", None)
    noreply = ArcusOperation(node, b"set fresh 0 0 1 noreply\r\nx", None)
    noreply.set_result(True)
    for operation in (invalid, pending, noreply):
        worker.q.put(operation)

    drain_worker(worker)

    assert node.process_operation.call_args_list == [
        call(pending),
        call(noreply),
    ]
