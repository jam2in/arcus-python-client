from unittest.mock import Mock, call

import pytest

from arcus import Arcus, ArcusMultiOperation, ArcusOperation


def make_operation():
    return ArcusOperation(None, b"request", None)


@pytest.fixture
def client(monkeypatch):
    locator = Mock()
    monkeypatch.setattr("arcus.client.ArcusMCNodeAllocator", Mock())
    monkeypatch.setattr("arcus.client.ArcusLocator", Mock(return_value=locator))
    return Arcus(zk_hosts="localhost:2181", service_code="test")


@pytest.mark.parametrize("method", ["bop_mget", "bop_smget"])
def test_multi_lookup_groups_keys_and_submits_before_waiting(client, method):
    first_node = Mock()
    second_node = Mock()
    first_operation = make_operation()
    second_operation = make_operation()
    getattr(first_node, method).return_value = first_operation
    getattr(second_node, method).return_value = second_operation
    locator = client.locator
    locator.get_node.side_effect = [first_node, second_node, first_node]
    key_range = (0, 100)
    element_filter = object()

    result = getattr(client, method)(
        ["a", "b", "c"], key_range, filter=element_filter, offset=2, count=7
    )

    assert isinstance(result, ArcusMultiOperation)
    assert not result.has_result()
    assert locator.get_node.call_args_list == [call("a"), call("b"), call("c")]
    getattr(first_node, method).assert_called_once_with(
        ["a", "c"], key_range, element_filter, 2, 7
    )
    getattr(second_node, method).assert_called_once_with(
        ["b"], key_range, element_filter, 2, 7
    )

    if method == "bop_mget":
        first_operation.set_result(({"a": {1: (None, "A")}}, ["z"]))
        second_operation.set_result(({"b": {2: (None, "B")}}, ["c"]))
        assert result.get_result() == {
            "a": {1: (None, "A")},
            "b": {2: (None, "B")},
        }
    else:
        first_operation.set_result(([(1, "a", None, "A"), (3, "c", None, "C")], []))
        second_operation.set_result(([(2, "b", None, "B")], ["z", "c"]))
        assert result.get_result() == [
            (1, "a", None, "A"),
            (2, "b", None, "B"),
            (3, "c", None, "C"),
        ]
    assert result.get_missed_key() == ["c", "z"]


@pytest.mark.parametrize(
    "method, default_count, child_result",
    [("bop_mget", 50, {"a": {}}), ("bop_smget", 2000, [(1, "a", None, "A")])],
)
def test_one_node_still_returns_a_multi_operation(
    client, method, default_count, child_result
):
    node = Mock()
    operation = make_operation()
    getattr(node, method).return_value = operation
    locator = client.locator
    locator.get_node.return_value = node

    result = getattr(client, method)(["a", "b"], (0, 10))

    assert isinstance(result, ArcusMultiOperation)
    getattr(node, method).assert_called_once_with(
        ["a", "b"], (0, 10), None, None, default_count
    )
    operation.set_result((child_result, ["b"]))
    assert result.get_result() == child_result
    assert result.get_missed_key() == ["b"]


@pytest.mark.parametrize("method, expected", [("bop_mget", {}), ("bop_smget", [])])
def test_empty_key_list_needs_no_node_requests(client, method, expected):
    locator = client.locator
    result = getattr(client, method)([], (0, 10))
    assert isinstance(result, ArcusMultiOperation)
    locator.get_node.assert_not_called()
    assert result.get_result() == expected
    assert result.get_missed_key() == []
