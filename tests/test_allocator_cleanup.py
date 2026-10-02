"""Shutdown must finish worker cleanup even when retiring a node fails."""

from unittest.mock import Mock

import pytest

from arcus import ArcusLocator, ArcusOperation, ArcusTranscoder
from arcus.protocol.allocator import ArcusMCNodeAllocator


@pytest.fixture
def allocator(monkeypatch):
    # Exercise the real allocator and worker thread without sockets or epoll.
    monkeypatch.setattr("arcus.protocol.worker.ArcusMCPoll", Mock())
    monkeypatch.setattr(
        "arcus.protocol.node.Connection", Mock(side_effect=lambda *a, **kw: Mock())
    )
    allocator = ArcusMCNodeAllocator(ArcusTranscoder())
    yield allocator
    # A failing pre-fix close must not leave a test thread behind.
    allocator.shutdown = True
    allocator.worker.q.put(None)
    allocator.worker.join(timeout=2)
    assert not allocator.worker.is_alive()


def test_node_cleanup_error_does_not_skip_other_nodes_or_worker_shutdown(allocator):
    for port in (11211, 11212, 11213):
        allocator.alloc(f"127.0.0.1:{port}", "node")
    first, second, third = allocator.snapshot_nodes()
    first_error = OSError("first close failed")
    first.handle.disconnect.side_effect = first_error
    second.handle.disconnect.side_effect = OSError("second close failed")

    with pytest.raises(OSError) as raised:
        allocator.close()

    assert raised.value is first_error
    for node in (first, second, third):
        assert node._closed
        node.handle.disconnect.assert_called_once_with()
    assert not allocator.worker.is_alive()
    assert allocator.worker.q.empty()
    assert allocator.snapshot_nodes() == ()
    # Shutdown remains idempotent after reporting its first cleanup error.
    allocator.close()
    assert allocator.worker.q.empty()


def test_failed_retirement_is_retried_without_leaving_a_worker_alive(allocator):
    locator = ArcusLocator(allocator)
    locator.hash_nodes(["127.0.0.1:11211-a"])
    retired = locator.get_node("key")
    error = OSError("persistent retirement failure")
    retired.handle.disconnect.side_effect = error
    locator.hash_nodes(["127.0.0.1:11212-b"])
    current = locator.get_node("key")
    assert retired in allocator.snapshot_nodes()

    with pytest.raises(OSError) as raised:
        locator.disconnect()

    assert raised.value is error
    assert retired.handle.disconnect.call_count == 2
    current.handle.disconnect.assert_called_once_with()
    assert locator.addr_node_map == {}
    assert allocator.snapshot_nodes() == ()
    assert not allocator.worker.is_alive()
    assert allocator.worker.q.empty()


def test_shutdown_failure_still_resolves_pending_operations(allocator):
    node = allocator.alloc("127.0.0.1:11211", "node")
    operation = ArcusOperation(node, b"get key", None)
    node._pending.add(operation)
    node.ops.append(operation)
    node.handle.disconnect.side_effect = OSError("socket close failed")

    with pytest.raises(OSError, match="socket close failed"):
        allocator.close()

    assert operation.has_result()
    assert operation.invalid
    assert not node._pending and not node.ops
    assert not allocator.worker.is_alive()
