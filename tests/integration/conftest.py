"""Fixtures for an explicitly selected, isolated Arcus service on Linux."""

import os
import select
import threading
import time
from uuid import uuid4

import pytest
from kazoo.client import KazooClient

from arcus import Arcus, ArcusLocator, ArcusTranscoder
from arcus_mc_node import ArcusMCNodeAllocator

OPERATION_TIMEOUT = 5
CACHE_TTL = 120


@pytest.fixture(scope="session")
def client():
    if not hasattr(select, "epoll"):
        pytest.fail("Integration tests require Linux and its real epoll implementation")

    hosts = os.environ.get("ARCUS_TEST_ZOOKEEPER")
    service_code = os.environ.get("ARCUS_TEST_SERVICE_CODE")
    if not hosts or not service_code:
        pytest.fail(
            "Use scripts/test-integration.sh, or explicitly configure "
            "ARCUS_TEST_ZOOKEEPER and ARCUS_TEST_SERVICE_CODE for an isolated service"
        )

    expected_nodes = int(os.environ.get("ARCUS_TEST_NODE_COUNT", "2"))
    zk = KazooClient(hosts=hosts, timeout=5)
    try:
        zk.start(timeout=10)
        deadline = time.monotonic() + 30
        service_path = f"/arcus/cache_list/{service_code}"
        while time.monotonic() < deadline:
            if zk.exists(service_path):
                children = zk.get_children(service_path)
                if len(children) == expected_nodes:
                    break
            time.sleep(0.1)
        else:
            pytest.fail(f"Expected {expected_nodes} registered nodes at {service_path}")
    finally:
        zk.stop()
        zk.close()

    allocator = ArcusMCNodeAllocator(ArcusTranscoder())
    locator = ArcusLocator(allocator)
    instance = Arcus(locator)
    try:
        instance.connect(hosts, service_code)
        assert len(locator.addr_node_map) == expected_nodes
        yield instance
    finally:
        instance.disconnect()
        assert not allocator.worker.is_alive(), "Arcus worker did not terminate"
        assert not allocator.worker.poll.is_alive(), "Arcus poller did not terminate"
        assert allocator.worker.poll.epoll.closed
        assert locator.zk is None


@pytest.fixture
def key_factory(client):
    prefix = f"pytest:{uuid4().hex}"
    keys = []
    lock = threading.Lock()

    def create_key(label="key"):
        with lock:
            key = f"{prefix}:{label}:{len(keys)}"
            keys.append(key)
            return key

    yield create_key

    # Explicit cleanup plus a TTL keeps independent runs from sharing cache data.
    cleanup_deadline = time.monotonic() + 15
    for key in keys:
        remaining = cleanup_deadline - time.monotonic()
        assert remaining > 0, "Test key cleanup exceeded its deadline"
        client.delete(key).get_result(timeout=min(OPERATION_TIMEOUT, remaining))


@pytest.fixture
def result():
    return lambda operation: operation.get_result(timeout=OPERATION_TIMEOUT)


@pytest.fixture
def cache_ttl():
    return CACHE_TTL
