"""Deterministic discovery, lifecycle, and routing regressions."""

import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from kazoo.handlers.threading import SequentialThreadingHandler

import arcus.routing as routing
from arcus import ArcusLocator, ArcusNodeConnectionException, ArcusProtocolException


def make_node(addr, name="cache"):
    return SimpleNamespace(addr=addr, name=name, in_use=False, close=Mock())


@pytest.fixture
def allocator():
    allocator = Mock()
    allocator.alloc.side_effect = make_node
    return allocator


@pytest.fixture
def locator(allocator):
    return ArcusLocator(allocator)


def lock_available(locator):
    acquired = locator.lock.acquire(blocking=False)
    if acquired:
        locator.lock.release()
    return acquired


def test_empty_ring_raises_connection_error_and_releases_lock(locator):
    with pytest.raises(ArcusNodeConnectionException, match="no available"):
        locator.get_node("key")
    assert lock_available(locator)


def test_allocation_failure_preserves_ring_and_releases_lock(locator, allocator):
    locator.hash_nodes(["cache1:11211-a"])
    old_nodes = locator.node_list[:]
    old_map = locator.addr_node_map.copy()
    allocator.alloc.side_effect = RuntimeError("allocation failed")
    with pytest.raises(RuntimeError, match="allocation failed"):
        locator.hash_nodes(["cache2:11211-b"])
    assert locator.node_list == old_nodes
    assert locator.addr_node_map == old_map
    assert lock_available(locator)


def test_failed_rehash_closes_new_allocations(locator, allocator):
    first = make_node("cache1:11211")
    allocator.alloc.side_effect = [first, RuntimeError("second allocation failed")]
    with pytest.raises(RuntimeError):
        locator.hash_nodes(["cache1:11211-a", "cache2:11211-b"])
    first.close.assert_called_once_with()
    assert locator.addr_node_map == {}
    assert locator.node_list == []
    assert lock_available(locator)


def test_malformed_child_does_not_publish_partial_ring(locator):
    locator.hash_nodes(["cache1:11211-a"])
    old_nodes = locator.node_list[:]
    with pytest.raises(ArcusProtocolException, match="invalid cache node"):
        locator.hash_nodes(["invalid"])
    assert locator.node_list == old_nodes
    assert lock_available(locator)


def test_removed_nodes_are_retired_without_reallocating_survivors(locator, allocator):
    locator.hash_nodes(["cache1:11211-a", "cache2:11211-b"])
    first, second = locator.addr_node_map.values()
    locator.hash_nodes(["cache2:11211-b"])
    first.close.assert_called_once_with()
    second.close.assert_not_called()
    assert allocator.alloc.call_count == 2
    assert locator.get_node("key") is second


def test_disconnect_without_connect_stops_allocator(locator, allocator):
    locator.disconnect()
    allocator.close.assert_called_once_with()
    with pytest.raises(ArcusNodeConnectionException):
        locator.get_node("key")


def test_zero_node_disconnect_stops_and_closes_zookeeper(locator, allocator):
    zk = Mock()
    locator.zk = zk
    locator.disconnect()
    zk.stop.assert_called_once_with()
    zk.close.assert_called_once_with()
    allocator.close.assert_called_once_with()
    assert locator.zk is None


def test_failed_connect_closes_zookeeper_and_allocator(locator, allocator, monkeypatch):
    zk = Mock()
    zk.start.side_effect = RuntimeError("unavailable ZooKeeper")
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    with pytest.raises(RuntimeError, match="unavailable ZooKeeper"):
        locator.connect("zookeeper:2181", "service")
    zk.stop.assert_called_once_with()
    zk.close.assert_called_once_with()
    allocator.close.assert_called_once_with()
    assert locator.zk is None


def configured_zk(children):
    zk = Mock()
    zk.get_async.return_value.get.return_value = (b"", None)
    zk.get_children_async.return_value.get.return_value = children
    zk.get.return_value = (b"", None)
    zk.get_children.return_value = children
    return zk


def test_reconnect_restarts_allocator_and_ignores_old_watch(
    locator, allocator, monkeypatch
):
    first_zk = configured_zk(["cache1:11211-a"])
    second_zk = configured_zk(["cache2:11211-b"])
    monkeypatch.setattr(routing, "KazooClient", Mock(side_effect=[first_zk, second_zk]))
    locator.connect("zookeeper:2181", "service")
    first_watch = first_zk.get_children_async.call_args.kwargs["watch"]
    locator.disconnect()
    locator.connect("zookeeper:2181", "service")
    first_watch(SimpleNamespace(path="/arcus/cache_list/service"))
    assert set(locator.addr_node_map) == {"cache2:11211"}
    assert first_zk.get_children_async.call_count == 1
    assert allocator.start.call_count == 2
    locator.disconnect()


def test_watch_after_disconnect_does_not_allocate(locator, allocator, monkeypatch):
    zk = configured_zk(["cache1:11211-a"])
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    locator.connect("zookeeper:2181", "service")
    watch = zk.get_children_async.call_args.kwargs["watch"]
    locator.disconnect()
    watch(SimpleNamespace(path="/arcus/cache_list/service"))
    assert allocator.alloc.call_count == 1
    assert zk.get_children_async.call_count == 1
    assert locator.addr_node_map == {}


def test_stopped_locator_does_not_accept_direct_rehash(locator, allocator):
    locator.disconnect()
    locator.hash_nodes(["cache1:11211-a"])
    allocator.alloc.assert_not_called()


def test_disconnect_cleans_all_resources_if_node_close_fails(locator, allocator):
    locator.hash_nodes(["cache1:11211-a", "cache2:11211-b"])
    first, second = locator.addr_node_map.values()
    first.close.side_effect = RuntimeError("close failed")
    zk = Mock()
    locator.zk = zk
    with pytest.raises(RuntimeError, match="close failed"):
        locator.disconnect()
    second.close.assert_called_once_with()
    zk.stop.assert_called_once_with()
    zk.close.assert_called_once_with()
    allocator.close.assert_called_once_with()
    assert locator.addr_node_map == {}


def test_watch_racing_disconnect_cannot_restore_closed_nodes(
    locator, allocator, monkeypatch
):
    zk = configured_zk(["cache1:11211-a"])
    pending = Mock()
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    locator.connect("zookeeper:2181", "service")
    watch = zk.get_children_async.call_args.kwargs["watch"]
    zk.get_children_async.return_value = pending
    watch(SimpleNamespace(type="CHILD", path="unused"))
    callback = pending.rawlink.call_args.args[0]
    locator.disconnect()
    pending.get.return_value = ["cache2:11211-b"]
    callback(pending)
    assert locator.addr_node_map == {}
    assert allocator.alloc.call_count == 1
    allocator.close.assert_called_once_with()


def test_zookeeper_stop_failure_still_closes_remaining_resources(locator, allocator):
    zk = Mock()
    zk.stop.side_effect = RuntimeError("stop failed")
    locator.zk = zk
    with pytest.raises(RuntimeError, match="stop failed"):
        locator.disconnect()
    zk.close.assert_called_once_with()
    allocator.close.assert_called_once_with()


def test_session_notice_does_not_issue_a_blocking_zookeeper_read(locator, monkeypatch):
    zk = configured_zk(["cache1:11211-a"])
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    locator.connect("zookeeper:2181", "service")
    watch = zk.get_children_async.call_args.kwargs["watch"]
    zk.get_children.side_effect = AssertionError("blocking read during suspension")
    watch(SimpleNamespace(type="NONE", path=None))
    assert locator.get_node("key").addr == "cache1:11211"


def test_reconnection_listener_is_registered(locator, monkeypatch):
    zk = configured_zk(["cache1:11211-a"])
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    locator.connect("zookeeper:2181", "service")
    zk.add_listener.assert_called_once()


def test_pending_discovery_keeps_cache_routing_available(locator, monkeypatch):
    zk = configured_zk(["cache1:11211-a"])
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    locator.connect("zookeeper:2181", "service")
    listener = zk.add_listener.call_args.args[0]
    zk.get_children.side_effect = AssertionError("synchronous read in listener")
    listener("SUSPENDED")
    listener("CONNECTED")
    assert zk.get_children_async.call_count == 2
    assert locator.get_node("key").addr == "cache1:11211"
    assert lock_available(locator)


def test_reconnected_snapshot_updates_membership_and_reinstalls_watch(
    locator, monkeypatch
):
    zk = configured_zk(["cache1:11211-a"])
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    locator.connect("zookeeper:2181", "service")
    listener = zk.add_listener.call_args.args[0]
    listener("LOST")
    listener("CONNECTED")
    result = zk.get_children_async.return_value
    result.get.return_value = ["cache2:11211-b"]
    result.rawlink.call_args.args[0](result)
    assert set(locator.addr_node_map) == {"cache2:11211"}
    assert callable(zk.get_children_async.call_args.kwargs["watch"])


def test_out_of_order_discovery_completion_keeps_newest_snapshot(locator, monkeypatch):
    zk = configured_zk(["cache1:11211-a"])
    first, second = Mock(), Mock()
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    locator.connect("zookeeper:2181", "service")
    watch = zk.get_children_async.call_args.kwargs["watch"]
    zk.get_children_async.side_effect = [first, second]
    watch(SimpleNamespace(type="CHILD", path="unused"))
    watch(SimpleNamespace(type="CHILD", path="unused"))
    first.get.return_value = ["cache2:11211-b"]
    second.get.return_value = ["cache3:11211-c"]
    second.rawlink.call_args.args[0](second)
    first.rawlink.call_args.args[0](first)
    assert set(locator.addr_node_map) == {"cache3:11211"}


def test_reconnection_during_initial_snapshot_registers_a_new_watch(
    locator, monkeypatch
):
    zk = configured_zk(["cache1:11211-a"])
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))

    def initial_children(*args, **kwargs):
        listener = zk.add_listener.call_args.args[0]
        listener("SUSPENDED")
        listener("CONNECTED")
        return ["cache1:11211-a"]

    initial = zk.get_children_async.return_value
    refreshed = Mock()
    initial.get.side_effect = initial_children
    zk.get_children_async.side_effect = [initial, refreshed]
    locator.connect("zookeeper:2181", "service")
    assert zk.get_children_async.call_count == 2
    result = refreshed
    result.get.return_value = ["cache2:11211-b"]
    result.rawlink.call_args.args[0](result)
    assert set(locator.addr_node_map) == {"cache2:11211"}


@pytest.mark.parametrize("stage", ["data", "children"])
def test_incomplete_initial_snapshot_times_out_and_releases_disconnect(
    locator, allocator, monkeypatch, stage
):
    monkeypatch.setattr(routing, "ZOOKEEPER_CONNECT_TIMEOUT", 0.05, raising=False)
    zk = configured_zk([])
    handler = SequentialThreadingHandler()
    pending = handler.async_result()
    entered = threading.Event()
    original_get = pending.get

    def observed_get(*args, **kwargs):
        entered.set()
        return original_get(*args, **kwargs)

    pending.get = observed_get
    if stage == "data":
        zk.get_async.return_value = pending
        zk.get.side_effect = lambda *args, **kwargs: pending.get()
    else:
        zk.get_children_async.return_value = pending
        zk.get_children.side_effect = lambda *args, **kwargs: pending.get()
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    errors = []

    def connect():
        try:
            locator.connect("zookeeper:2181", "service")
        except Exception as error:
            errors.append(error)

    connector = threading.Thread(target=connect)
    disconnector = threading.Thread(target=locator.disconnect)
    try:
        connector.start()
        assert entered.wait(1)
        disconnector.start()
        connector.join(timeout=0.5)
        disconnector.join(timeout=0.5)
        assert not connector.is_alive(), "initial discovery must have a deadline"
        assert not disconnector.is_alive(), "disconnect must not wait indefinitely"
        assert len(errors) == 1
        assert isinstance(errors[0], handler.timeout_exception)
        assert locator.zk is None
        assert not locator.addr_node_map
        zk.stop.assert_called_once_with()
        zk.close.assert_called_once_with()
        assert allocator.close.called
    finally:
        # Release the test even against the old unbounded synchronous implementation.
        pending.set((b"", None) if stage == "data" else [])
        connector.join(timeout=1)
        if disconnector.ident is not None:
            disconnector.join(timeout=1)


def test_initial_connection_and_discovery_share_one_time_budget(locator, monkeypatch):
    zk = configured_zk(["cache1:11211-a"])
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    clock = Mock(side_effect=[100.0, 100.0, 105.0, 113.0])
    monkeypatch.setattr(routing, "time", SimpleNamespace(monotonic=clock))
    locator.connect("zookeeper:2181", "service")
    zk.start.assert_called_once_with(timeout=15.0)
    zk.get_async.return_value.get.assert_called_once_with(timeout=10.0)
    zk.get_children_async.return_value.get.assert_called_once_with(timeout=2.0)
    zk.get.assert_not_called()
    zk.get_children.assert_not_called()


def test_exhausted_initial_connection_budget_cleans_resources(
    locator, allocator, monkeypatch
):
    zk = configured_zk(["cache1:11211-a"])
    monkeypatch.setattr(routing, "KazooClient", Mock(return_value=zk))
    clock = Mock(side_effect=[100.0, 100.0, 116.0])
    monkeypatch.setattr(routing, "time", SimpleNamespace(monotonic=clock))
    with pytest.raises(TimeoutError, match="ZooKeeper connection setup"):
        locator.connect("zookeeper:2181", "service")
    zk.get_async.return_value.get.assert_not_called()
    zk.get_children_async.assert_not_called()
    zk.stop.assert_called_once_with()
    zk.close.assert_called_once_with()
    allocator.close.assert_called_once_with()
    assert locator.zk is None
