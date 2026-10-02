"""Review regressions against the installed wheel and isolated real servers."""

import pytest

from arcus import ArcusNodeConnectionException, ArcusTranscoder, CollectionType

pytestmark = [pytest.mark.integration, pytest.mark.timeout(45, method="thread")]


def key_on_node(client, key_factory, node, label):
    for _ in range(100):
        key = key_factory(label)
        if client.locator.get_node(key) is node:
            return key
    pytest.fail("Could not select a key on the requested test node")


@pytest.mark.parametrize("family", ["list", "set", "btree", "mget", "smget"])
@pytest.mark.parametrize("value", ["a\r\nb", b"\x00\r\n\xff\x00", ""])
def test_collection_payload_framing_and_following_requests(
    client, key_factory, result, cache_ttl, family, value
):
    key = key_factory("collection")
    flags = ArcusTranscoder().encode(value)[0]
    if family == "list":
        assert result(client.lop.create(key, flags, exptime=cache_ttl)) is True
        assert result(client.lop.insert(key, -1, value)) is True
        read = lambda: client.lop.get(key, (0, -1))
        expected = [value]
    elif family == "set":
        assert result(client.sop.create(key, flags, exptime=cache_ttl)) is True
        assert result(client.sop.insert(key, value)) is True
        read = lambda: client.sop.get(key)
        expected = {value}
    else:
        assert result(client.bop.create(key, flags, exptime=cache_ttl)) is True
        assert result(client.bop.insert(key, 1, value, "0x01")) is True
        if family == "btree":
            read = lambda: client.bop.get(key, (0, 10))
            expected = {1: ("0x01", value)}
        elif family == "mget":
            read = lambda: client.bop.mget([key], (0, 10))
            expected = {key: {1: ("0x01", value)}}
        else:
            read = lambda: client.bop.smget([key], (0, 10))
            expected = [(1, key, "0x01", value)]

    node = client.locator.get_node(key)
    found = key_on_node(client, key_factory, node, "found")
    missing = key_on_node(client, key_factory, node, "missing")
    assert result(client.kv.set(found, "correct", exptime=cache_ttl)) is True
    collection_op, found_op, missing_op = (
        read(),
        client.kv.get(found),
        client.kv.get(missing),
    )
    assert result(collection_op) == expected
    assert result(found_op) == "correct"
    assert result(missing_op) is None


def test_collection_decode_failure_recovers_without_response_shift(
    client, key_factory, result, cache_ttl
):
    key = key_factory("invalid-utf8")
    assert result(client.lop.create(key, 0, exptime=cache_ttl)) is True
    # Collection flags belong to the item; inserting bytes leaves FLAG_STRING.
    assert result(client.lop.insert(key, -1, b"\xff")) is True
    node = client.locator.get_node(key)
    following = key_on_node(client, key_factory, node, "after-decode-error")
    missing = key_on_node(client, key_factory, node, "missing-after-decode-error")
    assert result(client.kv.set(following, "ok", exptime=cache_ttl)) is True
    # Keep the real worker/poller outside until all response slots are queued.
    with node._io_lock:
        broken = client.lop.get(key, (0, -1))
        found_op, missing_op = client.kv.get(following), client.kv.get(missing)
    with pytest.raises(UnicodeDecodeError):
        result(broken)
    for operation, expected in ((found_op, "ok"), (missing_op, None)):
        try:
            assert result(operation) == expected
        except ArcusNodeConnectionException:
            pass  # Explicit invalidation is safe; another key's result is not.
    assert result(client.kv.set(following, "ok", exptime=cache_ttl)) is True
    assert result(client.kv.get(following)) == "ok"


def test_set_existence_pipeline_consumes_every_response(
    client, key_factory, result, cache_ttl
):
    key = key_factory("pipeline")
    assert result(client.sop.create(key, 0, exptime=cache_ttl)) is True
    assert result(client.sop.insert(key, "present")) is True
    first = client.sop.exist(key, "present", pipe=True)
    last = client.sop.exist(key, "absent")
    assert result(first) is True  # Existing pipe operation reports send completion.
    assert result(last) == ["EXIST", "NOT_EXIST"]
    assert result(client.sop.exist(key, "present")) is True
    assert result(client.sop.get(key)) == {"present"}


@pytest.mark.parametrize("command", ["get", "count", "smget"])
def test_bkey_type_error_preserves_following_response(
    client, key_factory, result, cache_ttl, command
):
    key = key_factory("integer-bkey")
    assert result(client.bop.create(key, 0, exptime=cache_ttl)) is True
    assert result(client.bop.insert(key, 1, "value")) is True
    node = client.locator.get_node(key)
    connection = node.handle.socket
    with node._io_lock:
        target = [key] if command == "smget" else key
        bad = getattr(client.bop, command)(target, ("0x00", "0xff"))
        good = client.bop.get(key, (0, 10))
    with pytest.raises(CollectionType):
        result(bad)
    assert result(good) == {1: (None, "value")}
    assert node.handle.socket is connection


def test_cached_set_add_matches_remote_state(client, key_factory, result, cache_ttl):
    key = key_factory("cached-set")
    assert result(client.sop.create(key, 0, exptime=cache_ttl)) is True
    assert result(client.sop.insert(key, "old")) is True
    values = client.sop.wrap(key, cache_time=60)
    assert values.add("new") is True
    assert set(values) == {"old", "new"}
    assert result(client.sop.get(key)) == {"old", "new"}


@pytest.mark.parametrize("hex_keys", [False, True])
def test_multinode_unicode_smget_global_window(
    client, key_factory, result, cache_ttl, hex_keys
):
    nodes = list(client.locator.addr_node_map.values())
    assert len(nodes) == 2
    keys = [key_on_node(client, key_factory, node, "한글키") for node in nodes]
    bkeys = (
        ["0x01", "0x0100", "0x02", "0x0200", "0x03", "0x0300"]
        if hex_keys
        else list(range(1, 7))
    )
    for index, key in enumerate(keys):
        assert result(client.bop.create(key, 0, exptime=cache_ttl)) is True
        for bkey in bkeys[index::2]:
            assert result(client.bop.insert(key, bkey, "value")) is True

    forward = (bkeys[0], bkeys[-1])
    backward = tuple(reversed(forward))
    rows = result(client.bop.smget(keys, forward, count=2))
    assert [row[0] for row in rows] == bkeys[:2]
    rows = result(client.bop.smget(keys, forward, offset=1, count=2))
    assert [row[0] for row in rows] == bkeys[1:3]
    rows = result(client.bop.smget(keys, backward, count=3))
    assert [row[0] for row in rows] == list(reversed(bkeys))[:3]
    rows = result(client.bop.smget(keys, backward, offset=1, count=2))
    assert [row[0] for row in rows] == list(reversed(bkeys))[1:3]
    for index, key in enumerate(keys):
        # Exercise the command length for multiple Unicode keys in one wire body.
        sibling = key_on_node(client, key_factory, nodes[index], "다른한글키")
        assert result(client.bop.create(sibling, 0, exptime=cache_ttl)) is True
        assert result(client.bop.insert(sibling, bkeys[0], "sibling")) is True
        values = result(client.bop.mget([key, sibling], forward))
        assert set(values[key]) == set(bkeys[index::2])
        assert values[sibling] == {bkeys[0]: (None, "sibling")}
        rows = result(client.bop.smget([key, sibling], forward, count=2))
        expected_bkeys = sorted(
            [*bkeys[index::2], bkeys[0]],
            key=(lambda value: bytes.fromhex(value[2:])) if hex_keys else None,
        )
        assert [row[0] for row in rows] == expected_bkeys[:2]
        rows = result(client.bop.smget([key, sibling], forward, offset=1, count=2))
        assert [row[0] for row in rows] == expected_bkeys[1:3]

    assert result(client.bop.insert(keys[0], bkeys[-1], "tied")) is True
    rows = result(client.bop.smget(keys, backward, count=2))
    assert [row[0] for row in rows] == [bkeys[-1], bkeys[-1]]
    assert [row[1] for row in rows] == sorted(keys, reverse=True)
