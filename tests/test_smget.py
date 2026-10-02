"""Global sort/merge query semantics across independently sorted node replies."""

from unittest.mock import Mock

import pytest

from arcus import Arcus, ArcusOperation, ArcusOperationList


def completed(values, missed=()):
    operation = ArcusOperation(None, b"smget", None)
    operation.set_result((values, list(missed)))
    return operation


def client_for(first_values, second_values):
    first, second = Mock(), Mock()
    first.commands.btree.smget.return_value = completed(first_values, ["missing-z"])
    second.commands.btree.smget.return_value = completed(second_values, ["missing-a"])
    locator = Mock()
    locator.get_node.side_effect = {"a": first, "b": second}.__getitem__
    return Arcus(locator), first, second


@pytest.mark.parametrize(
    ("interval", "offset", "count", "first", "second", "expected"),
    [
        ((0, 10), None, 2, [1, 3], [2, 4], [1, 2]),
        ((10, 0), None, 3, [5, 3, 1], [6, 4, 2], [6, 5, 4]),
        ((0, 10), 1, 2, [1, 3, 5], [2, 4, 6], [2, 3]),
        ((10, 0), 1, 2, [5, 3, 1], [6, 4, 2], [5, 4]),
        ((0, 10), 6, 2, [1, 3, 5], [2, 4, 6], []),
        ((0, 10), 1, 2, [], [2, 4, 6], [4, 6]),
        ((0, 10), 1, 2, [], [], []),
        ((0, 10), 0, 2000, [1], [2], [1, 2]),
    ],
)
def test_smget_applies_direction_and_page_once_across_nodes(
    interval, offset, count, first, second, expected
):
    first_values = [(bkey, "a", None, str(bkey)) for bkey in first]
    second_values = [(bkey, "b", None, str(bkey)) for bkey in second]
    client, first_node, second_node = client_for(first_values, second_values)

    operation = client.bop.smget(["a", "b"], interval, offset=offset, count=count)

    assert [element[0] for element in operation.get_result()] == expected
    assert operation.get_result() is operation.get_result()
    assert operation.get_missed_key() == ["missing-a", "missing-z"]
    assert first_node.commands.btree.smget.call_args.args == (
        ["a"],
        interval,
        None,
        0,
        (offset or 0) + count,
    )
    assert second_node.commands.btree.smget.call_args.args == (
        ["b"],
        interval,
        None,
        0,
        (offset or 0) + count,
    )
    assert [element[0] for element in first_values] == first
    assert [element[0] for element in second_values] == second


@pytest.mark.parametrize("interval", [(0, 10), (10, 0), ("0xFF", "0x00")])
def test_single_node_smget_does_not_apply_server_offset_twice(interval):
    values = [(3, "a", None, "third"), (4, "a", None, "fourth")]
    client, first, second = client_for(values, [])

    result = client.bop.smget(["a"], interval, offset=2, count=2).get_result()

    assert result == values
    first.commands.btree.smget.assert_called_once_with(["a"], interval, None, 2, 2)
    second.commands.btree.smget.assert_not_called()


@pytest.mark.parametrize(
    ("interval", "first", "second", "expected"),
    [
        (
            ("0x00", "0xFF"),
            ["0x0b", "0x10"],
            ["0x0C", "0x0F"],
            ["0x0b", "0x0C", "0x0F"],
        ),
        (
            ("0x0a", "0x0F"),
            ["0x0a", "0x0c"],
            ["0x0b", "0x0f"],
            ["0x0a", "0x0b", "0x0c"],
        ),
        (
            ("0xff", "0x0A"),
            ["0x10", "0x0A"],
            ["0x0f", "0x0b"],
            ["0x10", "0x0f", "0x0b"],
        ),
        (("0x02", "0x0100"), ["0x02"], ["0x0100"], ["0x02", "0x0100"]),
    ],
)
def test_hex_smget_uses_binary_bkey_order_not_hex_spelling_or_integer_order(
    interval, first, second, expected
):
    client, _, _ = client_for(
        [(bkey, "a", None, b"value") for bkey in first],
        [(bkey, "b", None, b"value") for bkey in second],
    )

    result = client.bop.smget(["a", "b"], interval, count=3).get_result()

    assert [element[0] for element in result] == expected


@pytest.mark.parametrize(
    ("interval", "expected"), [((0, 10), ["a", "z"]), ((10, 0), ["z", "a"])]
)
def test_equal_bkeys_use_cache_key_as_tiebreaker_in_query_direction(interval, expected):
    client, _, _ = client_for([(1, "z", None, {})], [(1, "a", b"flag", object())])

    result = client.bop.smget(["a", "b"], interval, count=2).get_result()

    assert [element[1] for element in result] == expected


def test_smget_merge_never_compares_payloads_or_element_flags():
    first = (1, "same", None, object())
    second = (1, "same", b"flag", {})
    operation = ArcusOperationList("bop smget")
    operation.add_op(completed([first]))
    operation.add_op(completed([second]))

    assert operation.get_result() == [first, second]


def test_empty_smget_keeps_empty_result_even_with_nonzero_offset():
    locator = Mock()

    operation = Arcus(locator).bop.smget([], (10, 0), offset=2, count=3)

    assert operation.get_result() == []
    assert operation.get_missed_key() == []
    locator.get_node.assert_not_called()
