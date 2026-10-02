"""Membership discovery can publish snapshots without a cache client or allocator."""

import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import arcus.discovery as discovery
from arcus.discovery import ZooKeeperDiscovery


def configured_zk(children):
    zk = Mock()
    zk.get_async.return_value.get.return_value = (b"", None)
    zk.get_children_async.return_value.get.return_value = children
    return zk


def connect(monkeypatch, children, on_membership):
    zk = configured_zk(children)
    monkeypatch.setattr(discovery, "KazooClient", Mock(return_value=zk))
    source = ZooKeeperDiscovery(on_membership)
    source.connect("zookeeper:2181", "service")
    return source, zk


def test_discovery_publishes_plain_membership_without_cache_dependencies(monkeypatch):
    received = []
    source, zk = connect(monkeypatch, ["cache1:11211-a"], received.append)
    try:
        assert received == [["cache1:11211-a"]]
        assert source.client is zk
        assert source.path == "/arcus/cache_list/service"
        zk.get_async.assert_called_once_with(source.path)
        zk.get_children.assert_not_called()
    finally:
        source.close()
    assert source.client is None
    zk.stop.assert_called_once_with()
    zk.close.assert_called_once_with()


def test_initial_publication_failure_closes_discovery_resources(monkeypatch):
    on_membership = Mock(side_effect=ValueError("rejected membership"))
    zk = configured_zk(["invalid"])
    monkeypatch.setattr(discovery, "KazooClient", Mock(return_value=zk))
    source = ZooKeeperDiscovery(on_membership)
    with pytest.raises(ValueError, match="rejected membership"):
        source.connect("zookeeper:2181", "service")
    assert source.client is None
    zk.stop.assert_called_once_with()
    zk.close.assert_called_once_with()


def test_failed_async_snapshot_does_not_prevent_later_membership(monkeypatch):
    on_membership = Mock()
    source, zk = connect(monkeypatch, ["cache1:11211-a"], on_membership)
    first, second = Mock(), Mock()
    first.get.side_effect = RuntimeError("connection lost")
    second.get.return_value = ["cache2:11211-b"]
    zk.get_children_async.side_effect = [first, second]
    event = SimpleNamespace(type="CHILD", path=source.path)
    try:
        source.watch_children(event)
        first.rawlink.call_args.args[0](first)
        source.watch_children(event)
        second.rawlink.call_args.args[0](second)
        assert on_membership.call_args_list == [
            ((["cache1:11211-a"],), {}),
            ((["cache2:11211-b"],), {}),
        ]
    finally:
        source.close()


def test_closed_discovery_does_not_republish_pending_snapshot(monkeypatch):
    on_membership = Mock()
    source, zk = connect(monkeypatch, ["cache1:11211-a"], on_membership)
    pending = Mock()
    pending.get.return_value = ["cache2:11211-b"]
    zk.get_children_async.return_value = pending
    source.watch_children(SimpleNamespace(type="CHILD", path=source.path))
    source.close()
    pending.rawlink.call_args.args[0](pending)
    on_membership.assert_called_once_with(["cache1:11211-a"])
    source.watch_children(SimpleNamespace(type="CHILD", path=source.path))
    assert zk.get_children_async.call_count == 2


def test_close_waits_for_publication_then_blocks_all_later_callbacks(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    closed = threading.Event()
    received = []

    def on_membership(children):
        if children == ["cache2:11211-b"]:
            entered.set()
            assert release.wait(1)
        received.append(children)

    source, zk = connect(monkeypatch, ["cache1:11211-a"], on_membership)
    pending = Mock()
    pending.get.return_value = ["cache2:11211-b"]
    zk.get_children_async.return_value = pending
    source.watch_children(SimpleNamespace(type="CHILD", path=source.path))
    callback = pending.rawlink.call_args.args[0]
    publisher = threading.Thread(target=lambda: callback(pending))
    closer = threading.Thread(target=lambda: (source.close(), closed.set()))
    try:
        publisher.start()
        assert entered.wait(1)
        closer.start()
        assert not closed.wait(0.02)
        release.set()
        publisher.join(timeout=1)
        closer.join(timeout=1)
        assert closed.is_set()
        assert not publisher.is_alive()
        callback(pending)
        assert received == [["cache1:11211-a"], ["cache2:11211-b"]]
    finally:
        release.set()
        publisher.join(timeout=1)
        if closer.ident is not None:
            closer.join(timeout=1)
        source.close()
