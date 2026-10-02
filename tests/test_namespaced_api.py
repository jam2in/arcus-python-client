"""Public namespace access and explicitly deprecated flat compatibility calls."""

import inspect
import queue
import warnings
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from arcus import Arcus, ArcusList, ArcusOperation, ArcusSet, ArcusTranscoder
from arcus.api import BTreeAPI, KeyValueAPI, ListAPI, SetAPI
from arcus.protocol import ArcusMCNode


REPLACEMENTS = {
    **{
        name: ("kv", name)
        for name in (
            "set",
            "get",
            "gets",
            "incr",
            "decr",
            "delete",
            "add",
            "append",
            "prepend",
            "replace",
            "cas",
        )
    },
    **{f"lop_{name}": ("lop", name) for name in ("create", "insert", "get", "delete")},
    **{
        f"sop_{name}": ("sop", name)
        for name in ("create", "insert", "get", "delete", "exist")
    },
    **{
        f"bop_{name}": ("bop", name)
        for name in (
            "create",
            "insert",
            "upsert",
            "update",
            "get",
            "delete",
            "count",
            "incr",
            "decr",
            "mget",
            "smget",
        )
    },
    "list_alloc": ("lop", "alloc"),
    "list_get": ("lop", "wrap"),
    "set_alloc": ("sop", "alloc"),
    "set_get": ("sop", "wrap"),
}


def completed(value):
    operation = ArcusOperation(None, b"request", None)
    operation.set_result(value)
    return operation


def test_namespaces_are_reused_composed_api_objects():
    client = Arcus(Mock())
    assert isinstance(client.kv, KeyValueAPI)
    assert isinstance(client.lop, ListAPI)
    assert isinstance(client.sop, SetAPI)
    assert isinstance(client.bop, BTreeAPI)
    assert client.kv is client.kv
    assert client.lop is client.lop
    assert client.sop is client.sop
    assert client.bop is client.bop
    assert not hasattr(client, "mop")


@pytest.mark.parametrize("legacy", REPLACEMENTS)
@pytest.mark.parametrize("provide_defaults", [False, True])
def test_every_legacy_adapter_warns_once_and_preserves_argument_and_result_identity(
    legacy, provide_defaults
):
    client = Arcus(Mock())
    namespace, method = REPLACEMENTS[legacy]
    target = Mock(return_value=object())
    setattr(getattr(client, namespace), method, target)
    adapter = getattr(client, legacy)
    parameters = inspect.signature(adapter).parameters
    kwargs = {
        name: object()
        for name, parameter in parameters.items()
        if provide_defaults or parameter.default is inspect.Parameter.empty
    }
    bound = inspect.signature(adapter).bind(**kwargs)
    bound.apply_defaults()
    expected_message = (
        f"Arcus.{legacy}() is deprecated; use Arcus.{namespace}.{method}() instead."
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = adapter(**kwargs)

    assert result is target.return_value
    target.assert_called_once_with(*bound.arguments.values())
    assert len(caught) == 1
    assert caught[0].category is DeprecationWarning
    assert str(caught[0].message) == expected_message
    assert Path(caught[0].filename).resolve() == Path(__file__).resolve()
    assert adapter.__deprecated__ == expected_message


def test_every_retained_flat_client_command_is_explicitly_deprecated():
    public_methods = {
        name
        for name, method in inspect.getmembers(Arcus, inspect.isfunction)
        if not name.startswith("_") and name not in {"connect", "disconnect"}
    }
    assert public_methods == set(REPLACEMENTS)


@pytest.mark.parametrize(
    ("namespace", "method", "args", "payload"),
    [
        ("kv", "get", ("key",), b"get key"),
        ("lop", "get", ("key", (0, -1)), b"lop get key 0..-1 "),
        ("sop", "insert", ("key", "value"), b"sop insert key 5\r\nvalue"),
        ("bop", "delete", ("key", (0, 9)), b"bop delete key 0..9 "),
    ],
)
def test_canonical_api_reaches_real_commands_without_compatibility_warnings(
    namespace, method, args, payload
):
    allocator = SimpleNamespace(shutdown=False, worker=SimpleNamespace(q=queue.Queue()))
    with patch("arcus.protocol.node.Connection"):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)
    # Only expose commands: canonical API cannot accidentally use a flat node method.
    locator = Mock()
    locator.get_node.return_value = SimpleNamespace(commands=node.commands)
    client = Arcus(locator)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        operation = getattr(getattr(client, namespace), method)(*args)

    assert operation.request == payload
    assert operation is allocator.worker.q.get_nowait()
    assert not operation.has_result()


@pytest.mark.parametrize(
    ("namespace", "family", "wrapper_type", "initial"),
    [("lop", "list", ArcusList, []), ("sop", "set", ArcusSet, set())],
)
def test_namespace_wrapper_creation_and_mutation_do_not_call_deprecated_client_methods(
    namespace, family, wrapper_type, initial
):
    node = SimpleNamespace(commands=Mock())
    commands = getattr(node.commands, family)
    commands.create.return_value = completed(True)
    commands.get.return_value = completed(initial)
    commands.insert.return_value = completed(True)
    locator = Mock()
    locator.get_node.return_value = node
    client = Arcus(locator)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        wrapper = getattr(client, namespace).alloc("key", 0, exptime=60, cache_time=30)
        if namespace == "lop":
            wrapper.append("new")
        else:
            wrapper.add("new")

    assert isinstance(wrapper, wrapper_type)
    assert wrapper.arcus is client
    assert "new" in wrapper
    commands.create.assert_called_once_with("key", 0, 60, False, None)


@pytest.mark.parametrize(
    ("legacy", "family", "wrapper_type"),
    [("list_alloc", "list", ArcusList), ("set_alloc", "set", ArcusSet)],
)
def test_legacy_collection_allocation_emits_only_the_outer_warning(
    legacy, family, wrapper_type
):
    locator = Mock()
    client = Arcus(locator)

    with pytest.warns(DeprecationWarning) as caught:
        wrapper = getattr(client, legacy)("key", 0)

    assert len(caught) == 1
    assert isinstance(wrapper, wrapper_type)
    assert wrapper.arcus is client
    getattr(locator.get_node.return_value.commands, family).create.assert_called_once()
