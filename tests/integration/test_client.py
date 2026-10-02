from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import os

import pytest

from arcus import Arcus, ArcusLocator, ArcusTranscoder, CollectionExist, CollectionType
from arcus import ArcusMCNodeAllocator

pytestmark = [pytest.mark.integration, pytest.mark.timeout(45, method="thread")]


@pytest.mark.parametrize(
    "value",
    [
        "hello",
        "한글 문자열",
        "",
        42,
        -1,
        -(2**63),
        1.25,
        True,
        b"binary\x00\r\npayload",
    ],
)
def test_value_round_trip(client, key_factory, result, cache_ttl, value):
    key = key_factory()
    assert result(client.kv.set(key, value, exptime=cache_ttl)) is True
    actual = result(client.kv.get(key))
    assert type(actual) is type(value)
    assert actual == value


def test_date_round_trip(client, key_factory, result, cache_ttl):
    key = key_factory()
    value = datetime(2025, 1, 2, 3, 4, 5, 123456)
    assert result(client.kv.set(key, value, exptime=cache_ttl)) is True
    assert abs(result(client.kv.get(key)) - value) < timedelta(milliseconds=1)


def test_missing_and_deleted_key(client, key_factory, result, cache_ttl):
    key = key_factory()
    assert result(client.kv.get(key)) is None
    # The existing delete API treats an already absent key as success.
    assert result(client.kv.delete(key)) is True
    assert result(client.kv.set(key, "value", exptime=cache_ttl)) is True
    assert result(client.kv.delete(key)) is True
    assert result(client.kv.get(key)) is None


def test_increment_and_decrement(client, key_factory, result, cache_ttl):
    key = key_factory()
    assert result(client.kv.set(key, "1", exptime=cache_ttl)) is True
    assert result(client.kv.incr(key, 10)) == 11
    assert result(client.kv.decr(key, 3)) == 8
    assert result(client.kv.decr(key, 100)) == 0


def test_compare_and_swap(client, key_factory, result, cache_ttl):
    key = key_factory()
    assert result(client.kv.set(key, "before", exptime=cache_ttl)) is True
    value, token = result(client.kv.gets(key))
    assert value == "before"
    assert result(client.kv.cas(key, "after", token, exptime=cache_ttl)) is True
    assert result(client.kv.cas(key, "stale", token, exptime=cache_ttl)) is False
    assert result(client.kv.get(key)) == "after"


def test_list_collection(client, key_factory, result, cache_ttl):
    key = key_factory("list")
    flags = ArcusTranscoder.FLAG_STRING
    assert result(client.lop.create(key, flags, exptime=cache_ttl)) is True
    values = ["first", "second", "third"]
    for value in values:
        assert result(client.lop.insert(key, -1, value)) is True
    assert result(client.lop.get(key, (0, -1))) == values
    assert result(client.lop.get(key, (1, 2))) == values[1:]


def test_set_collection(client, key_factory, result, cache_ttl):
    key = key_factory("set")
    flags = ArcusTranscoder.FLAG_STRING
    assert result(client.sop.create(key, flags, exptime=cache_ttl)) is True
    values = {"first", "second", "third"}
    for value in sorted(values):
        assert result(client.sop.insert(key, value)) is True
    assert result(client.sop.get(key)) == values
    assert result(client.sop.exist(key, "second")) is True
    assert result(client.sop.exist(key, "absent")) is False


def test_delete_final_set_element_drops_collection(
    client, key_factory, result, cache_ttl
):
    key = key_factory("set-drop")
    assert (
        result(client.sop.create(key, ArcusTranscoder.FLAG_STRING, exptime=cache_ttl))
        is True
    )
    assert result(client.sop.insert(key, "last")) is True
    assert result(client.sop.delete(key, "last", drop=True)) is True
    assert result(client.sop.get(key)) is None
    assert result(client.kv.set(key, "usable", exptime=cache_ttl)) is True
    assert result(client.kv.get(key)) == "usable"


def test_node_statistics_are_complete_and_leave_connection_usable(
    client, key_factory, result, cache_ttl
):
    key = key_factory("stats")
    node = client.locator.get_node(key)
    statistics = result(node.commands.admin.get_stats())
    assert statistics["version"]
    assert int(statistics["curr_connections"]) > 0
    assert all(isinstance(value, str) for value in statistics.values())
    assert result(client.kv.set(key, "after stats", exptime=cache_ttl)) is True
    assert result(client.kv.get(key)) == "after stats"


def test_btree_collection(client, key_factory, result, cache_ttl):
    key = key_factory("btree")
    flags = ArcusTranscoder.FLAG_LONG
    assert result(client.bop.create(key, flags, exptime=cache_ttl)) is True
    for bkey in range(5):
        assert result(client.bop.insert(key, bkey, bkey * 10, "0x01")) is True
    assert result(client.bop.get(key, (1, 3))) == {
        1: ("0x01", 10),
        2: ("0x01", 20),
        3: ("0x01", 30),
    }
    assert result(client.bop.count(key, (1, 3))) == 3


def test_collection_errors_leave_connection_usable(
    client, key_factory, result, cache_ttl
):
    key = key_factory("list")
    flags = ArcusTranscoder.FLAG_STRING
    assert result(client.lop.create(key, flags, exptime=cache_ttl)) is True
    with pytest.raises(CollectionExist):
        result(client.lop.create(key, flags, exptime=cache_ttl))
    with pytest.raises(CollectionType):
        result(client.sop.insert(key, "value"))
    assert result(client.lop.insert(key, -1, "still usable")) is True
    assert result(client.lop.get(key, (0, -1))) == ["still usable"]


def test_shared_client_concurrent_requests(client, key_factory, result, cache_ttl):
    keys_by_node = {address: [] for address in client.locator.addr_node_map}
    for _ in range(200):
        key = key_factory("concurrent")
        address = client.locator.get_node(key).addr
        if len(keys_by_node[address]) < 8:
            keys_by_node[address].append(key)
        if all(len(keys) == 8 for keys in keys_by_node.values()):
            break
    assert all(len(keys) == 8 for keys in keys_by_node.values())
    keys = [key for node_keys in keys_by_node.values() for key in node_keys]

    def round_trip(item):
        index, key = item
        for revision in range(4):
            value = f"value:{index}:{revision}"
            assert result(client.kv.set(key, value, exptime=cache_ttl)) is True
            assert result(client.kv.get(key)) == value

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(round_trip, item) for item in enumerate(keys)]
        for future in futures:
            future.result(timeout=20)


def test_repeated_client_lifecycles_release_descriptors(client):
    before = len(os.listdir("/proc/self/fd"))
    for _ in range(3):
        allocator = ArcusMCNodeAllocator(ArcusTranscoder())
        instance = Arcus(ArcusLocator(allocator))
        try:
            instance.connect(
                os.environ["ARCUS_TEST_ZOOKEEPER"],
                os.environ["ARCUS_TEST_SERVICE_CODE"],
            )
        finally:
            instance.disconnect()
        assert not allocator.worker.is_alive()
        assert not allocator.worker.poll.is_alive()
        assert allocator.worker.poll.epoll.closed
    assert len(os.listdir("/proc/self/fd")) <= before


def test_btree_delete_namespace_preserves_collection_lifecycle(
    client, key_factory, result, cache_ttl
):
    key = key_factory("btree-delete")
    assert result(client.bop.create(key, 0, exptime=cache_ttl)) is True
    assert result(client.bop.insert(key, 1, "first")) is True
    assert result(client.bop.insert(key, 2, "second")) is True
    assert result(client.bop.delete(key, (1, 1))) is True
    assert result(client.bop.get(key, (0, 10))) == {2: (None, "second")}
    assert result(client.bop.delete(key, (2, 2), drop=True)) is True
    assert result(client.bop.get(key, (0, 10))) is None
    assert result(client.kv.set(key, "reused", exptime=cache_ttl)) is True
    assert result(client.kv.get(key)) == "reused"
