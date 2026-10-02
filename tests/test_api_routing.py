"""Public API contracts across the facade, family APIs and routing boundary."""

import inspect
from unittest.mock import Mock

import pytest

from arcus import (
    Arcus,
    ArcusList,
    ArcusNodeConnectionException,
    ArcusOperation,
    ArcusOperationList,
    ArcusSet,
)
from arcus.api import BTreeAPI, KeyValueAPI, ListAPI, RequestExecutor, SetAPI
from arcus_mc_node import ArcusMCNode


class KeyLocator:
    def __init__(self, nodes):
        self.nodes = nodes
        self.lookups = []

    def get_node(self, key):
        self.lookups.append(key)
        return self.nodes[key]


def completed(result):
    operation = ArcusOperation(None, b"request", None)
    operation.set_result(result)
    return operation


@pytest.mark.parametrize(
    ("method", "arguments"),
    [
        ("set", ("value", 60)),
        ("get", ()),
        ("gets", ()),
        ("incr", (5,)),
        ("decr", (5,)),
        ("delete", ()),
        ("add", ("value", 60)),
        ("append", ("value", 60)),
        ("prepend", ("value", 60)),
        ("replace", ("value", 60)),
        ("cas", ("value", b"42", 60)),
        ("lop_create", (3, 60, True, {"maxcount": 7})),
        ("lop_insert", (2, "value", False, True, {"maxcount": 7})),
        ("lop_get", ((0, 3), True, True)),
        ("lop_delete", ((0, 3), True, False, True)),
        ("sop_create", (3, 60, True, {"maxcount": 7})),
        ("sop_insert", ("value", False, True, {"maxcount": 7})),
        ("sop_get", (4, True, True)),
        ("sop_delete", ("value", True, False, True)),
        ("sop_exist", ("value", True)),
        ("bop_create", (3, 60, True, {"maxcount": 7})),
        ("bop_insert", (3, "value", b"flag", False, True, {"maxcount": 7})),
        ("bop_upsert", (3, "value", b"flag", False, True, {"maxcount": 7})),
        ("bop_update", (3, "value", b"flag", False, True, {"maxcount": 7})),
        ("bop_get", ((0, 3), "filter", True, True)),
        ("bop_delete", ((0, 3), "filter", 4, True, False, True)),
        ("bop_count", ((0, 3), "filter")),
        ("bop_incr", (3, 5, False, True)),
        ("bop_decr", (3, 5, False, True)),
    ],
)
def test_each_single_key_api_routes_and_preserves_arguments_and_operation(
    method, arguments
):
    first, second = Mock(spec=ArcusMCNode), Mock(spec=ArcusMCNode)
    locator = KeyLocator({"first": first, "second": second})
    client = Arcus(locator)

    for key, node in (("first", first), ("second", second)):
        pending = ArcusOperation(node, b"request", None)
        command = getattr(node, method)
        command.return_value = pending
        returned = getattr(client, method)(key, *arguments)
        command.assert_called_once_with(key, *arguments)
        assert returned is pending
        assert not returned.has_result()
        assert len(node.mock_calls) == 1

    assert locator.lookups == ["first", "second"]


@pytest.mark.parametrize(
    ("family", "method", "arguments", "node_method", "expected"),
    [
        (KeyValueAPI, "set", ("key", "value"), "set", ("key", "value", 0)),
        (ListAPI, "get", ("key", (0, -1)), "lop_get", ("key", (0, -1), False, False)),
        (SetAPI, "get", ("key",), "sop_get", ("key", 0, False, False)),
        (BTreeAPI, "count", ("key", (0, 3)), "bop_count", ("key", (0, 3), None)),
    ],
)
def test_family_apis_can_be_used_with_only_an_executor(
    family, method, arguments, node_method, expected
):
    node = Mock(spec=ArcusMCNode)
    api = family(RequestExecutor(KeyLocator({"key": node})))

    returned = getattr(api, method)(*arguments)

    command = getattr(node, node_method)
    command.assert_called_once_with(*expected)
    assert returned is command.return_value


def test_replacing_locator_updates_all_family_routes_and_lifecycle():
    original, replacement = Mock(), Mock()
    client = Arcus(original)
    client.locator = replacement

    assert client.locator is replacement
    assert client.connect("zk:2181", "service") is None
    client.get("kv")
    client.lop_get("list", (0, -1))
    client.sop_get("set")
    client.bop_count("btree", (0, 10))
    assert client.disconnect() is None

    original.assert_not_called()
    assert original.mock_calls == []
    replacement.connect.assert_called_once_with("zk:2181", "service")
    replacement.disconnect.assert_called_once_with()
    assert [call.args for call in replacement.get_node.call_args_list] == [
        ("kv",),
        ("list",),
        ("set",),
        ("btree",),
    ]


def test_mget_groups_keys_per_node_and_preserves_merge_and_miss_semantics():
    first, second = Mock(spec=ArcusMCNode), Mock(spec=ArcusMCNode)
    locator = KeyLocator({"a": first, "b": second, "c": first, "missing": second})
    first.bop_mget.return_value = completed(({"a": {1: "a"}, "c": {3: "c"}}, []))
    second.bop_mget.return_value = completed(({"b": {2: "b"}}, ["missing"]))

    operations = Arcus(locator).bop_mget(
        ["a", "b", "c", "missing", "a"], (0, 10), "filter", 2, 3
    )

    first.bop_mget.assert_called_once_with(["a", "c", "a"], (0, 10), "filter", 2, 3)
    second.bop_mget.assert_called_once_with(["b", "missing"], (0, 10), "filter", 2, 3)
    assert isinstance(operations, ArcusOperationList)
    assert operations.ops == [first.bop_mget.return_value, second.bop_mget.return_value]
    assert operations.get_result() == {"a": {1: "a"}, "b": {2: "b"}, "c": {3: "c"}}
    assert operations.get_missed_key() == ["missing"]
    assert locator.lookups == ["a", "b", "c", "missing", "a"]


def test_smget_merges_completed_results_without_waiting_during_dispatch():
    first, second = Mock(spec=ArcusMCNode), Mock(spec=ArcusMCNode)
    first_result = [(1, "a"), (4, "a")]
    second_result = [(2, "b"), (3, "b")]
    first.bop_smget.return_value = ArcusOperation(first, b"smget", None)
    second.bop_smget.return_value = ArcusOperation(second, b"smget", None)
    client = Arcus(KeyLocator({"a": first, "b": second}))

    operations = client.bop_smget(["a", "b"], (0, 10))

    first.bop_smget.assert_called_once_with(["a"], (0, 10), None, None, 2000)
    second.bop_smget.assert_called_once_with(["b"], (0, 10), None, None, 2000)
    assert not operations.has_result()
    first.bop_smget.return_value.set_result((first_result, ["z"]))
    second.bop_smget.return_value.set_result((second_result, ["x"]))
    assert operations.get_result() == [(1, "a"), (2, "b"), (3, "b"), (4, "a")]
    assert operations.get_missed_key() == ["x", "z"]
    assert first_result == [(1, "a"), (4, "a")]
    assert second_result == [(2, "b"), (3, "b")]


@pytest.mark.parametrize("method", ["bop_mget", "bop_smget"])
def test_multi_node_errors_propagate_as_the_original_operation_error(method):
    node = Mock(spec=ArcusMCNode)
    failure = ArcusNodeConnectionException("disconnected")
    getattr(node, method).return_value = completed(failure)
    operations = getattr(Arcus(KeyLocator({"key": node})), method)(["key"], (0, 3))

    with pytest.raises(ArcusNodeConnectionException) as raised:
        operations.get_result(timeout=0.1)
    assert raised.value is failure


@pytest.mark.parametrize("method", ["bop_mget", "bop_smget"])
def test_multi_node_routing_resolves_all_keys_before_submitting_any_request(method):
    node = Mock(spec=ArcusMCNode)
    client = Arcus(KeyLocator({"known": node}))

    with pytest.raises(KeyError):
        getattr(client, method)(["known", "unknown"], (0, 3))

    assert node.mock_calls == []


@pytest.mark.parametrize(("method", "result"), [("bop_mget", {}), ("bop_smget", [])])
def test_empty_multi_key_request_preserves_empty_result(method, result):
    locator = KeyLocator({})
    operations = getattr(Arcus(locator), method)([], (0, 3))
    assert operations.get_result(timeout=0.1) == result
    assert operations.get_missed_key() == []
    assert locator.lookups == []


def test_single_node_routing_and_submission_errors_remain_synchronous():
    locator = Mock()
    failure = ArcusNodeConnectionException("no destination")
    locator.get_node.side_effect = failure
    client = Arcus(locator)
    with pytest.raises(ArcusNodeConnectionException) as raised:
        client.get("key")
    assert raised.value is failure

    locator.get_node.side_effect = None
    locator.get_node.return_value.get.side_effect = failure
    with pytest.raises(ArcusNodeConnectionException) as raised:
        client.get("key")
    assert raised.value is failure


def test_collection_wrappers_keep_the_client_and_creation_arguments():
    locator = Mock()
    client = Arcus(locator)

    items = client.list_alloc("list", 3, exptime=60)
    members = client.set_alloc("set", 4, exptime=90)

    assert isinstance(items, ArcusList)
    assert isinstance(members, ArcusSet)
    assert items.arcus is members.arcus is client
    assert items.key == "list"
    assert members.key == "set"
    locator.get_node.return_value.lop_create.assert_called_once_with(
        "list", 3, 60, False, None
    )
    locator.get_node.return_value.sop_create.assert_called_once_with(
        "set", 4, 90, False, None
    )
    locator.get_node.return_value.lop_get.assert_not_called()
    locator.get_node.return_value.sop_get.assert_not_called()


# Snapshot of the published facade: named parameters and defaults are compatibility.
EXPECTED_SIGNATURES = {
    "__init__": "(self, locator)",
    "connect": "(self, addr, code)",
    "disconnect": "(self)",
    "set": "(self, key, val, exptime=0)",
    "get": "(self, key)",
    "gets": "(self, key)",
    "incr": "(self, key, val=1)",
    "decr": "(self, key, val=1)",
    "delete": "(self, key)",
    "add": "(self, key, val, exptime=0)",
    "append": "(self, key, val, exptime=0)",
    "prepend": "(self, key, val, exptime=0)",
    "replace": "(self, key, val, exptime=0)",
    "cas": "(self, key, val, cas_id, exptime=0)",
    "lop_create": "(self, key, flags, exptime=0, noreply=False, attr_map=None)",
    "lop_insert": "(self, key, index, value, noreply=False, pipe=False, attr_map=None)",
    "lop_get": "(self, key, range, delete=False, drop=False)",
    "lop_delete": "(self, key, range, drop=False, noreply=False, pipe=False)",
    "sop_create": "(self, key, flags, exptime=0, noreply=False, attr_map=None)",
    "sop_insert": "(self, key, value, noreply=False, pipe=False, attr_map=None)",
    "sop_get": "(self, key, count=0, delete=False, drop=False)",
    "sop_delete": "(self, key, value, drop=False, noreply=False, pipe=False)",
    "sop_exist": "(self, key, value, pipe=False)",
    "bop_create": "(self, key, flags, exptime=0, noreply=False, attr_map=None)",
    "bop_insert": "(self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None)",
    "bop_upsert": "(self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None)",
    "bop_update": "(self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None)",
    "bop_get": "(self, key, range, filter=None, delete=False, drop=False)",
    "bop_delete": "(self, key, range, filter=None, count=None, drop=False, noreply=False, pipe=False)",
    "bop_count": "(self, key, range, filter=None)",
    "bop_incr": "(self, key, bkey, value, noreply=False, pipe=False)",
    "bop_decr": "(self, key, bkey, value, noreply=False, pipe=False)",
    "bop_mget": "(self, key_list, range, filter=None, offset=None, count=50)",
    "bop_smget": "(self, key_list, range, filter=None, offset=None, count=2000)",
    "list_alloc": "(self, key, flags, exptime=0, cache_time=0)",
    "list_get": "(self, key, cache_time=0)",
    "set_alloc": "(self, key, flags, exptime=0, cache_time=0)",
    "set_get": "(self, key, cache_time=0)",
}


@pytest.mark.parametrize(("method", "signature"), EXPECTED_SIGNATURES.items())
def test_public_signatures_remain_compatible(method, signature):
    assert str(inspect.signature(getattr(Arcus, method))) == signature
