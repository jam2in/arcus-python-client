"""Deprecated adapters retain identity, signatures, and asynchronous behavior."""

import inspect
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch
import warnings

import pytest

import arcus
from arcus import ArcusLocator, ArcusTranscoder
from arcus._compat.node import LegacyNodeCommands
from arcus._deprecation import deprecated
from arcus.protocol import ArcusMCNode
from arcus.protocol.request import CommandRequest


def make_node():
    allocator = SimpleNamespace(
        shutdown=False,
        operation_timeout=5,
        worker=SimpleNamespace(q=queue.Queue(), poll=Mock(), register_node=Mock()),
        close=Mock(),
    )
    with patch("arcus.protocol.node.Connection"):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)
    return node, allocator


def test_decorator_preserves_callable_metadata_and_points_warning_at_caller():
    sentinel = object()

    def original(key: str, *, default=None) -> object:
        """Original documentation."""
        return sentinel

    wrapped = deprecated("Use the replacement.")(original)
    assert inspect.signature(wrapped) == inspect.signature(original)
    assert wrapped.__name__ == original.__name__
    assert wrapped.__qualname__ == original.__qualname__
    assert wrapped.__doc__ == original.__doc__
    assert wrapped.__annotations__ == original.__annotations__
    assert wrapped.__wrapped__ is original
    assert wrapped.__deprecated__ == "Use the replacement."
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        call_line = inspect.currentframe().f_lineno + 1
        returned = wrapped("key", default="unused")
    assert returned is sentinel
    assert len(caught) == 1
    assert caught[0].category is DeprecationWarning
    assert caught[0].filename == __file__
    assert caught[0].lineno == call_line
    assert str(caught[0].message) == "Use the replacement."


def test_decorator_preserves_the_original_exception():
    failure = ValueError("original failure")

    @deprecated("Use the replacement.")
    def legacy():
        raise failure

    with pytest.warns(DeprecationWarning), pytest.raises(ValueError) as raised:
        legacy()
    assert raised.value is failure


LEGACY_NODE_REPLACEMENTS = {
    **{
        name: "commands.kv." + name
        for name in (
            "get",
            "gets",
            "set",
            "cas",
            "incr",
            "decr",
            "add",
            "append",
            "prepend",
            "replace",
            "delete",
        )
    },
    **{name: "commands.admin." + name for name in ("flush_all", "get_stats")},
    **{
        "lop_" + name: "commands.list." + name
        for name in ("create", "insert", "delete", "get")
    },
    **{
        "sop_" + name: "commands.set." + name
        for name in ("create", "insert", "get", "delete", "exist")
    },
    **{
        "bop_" + name: "commands.btree." + name
        for name in (
            "create",
            "insert",
            "upsert",
            "update",
            "delete",
            "get",
            "mget",
            "smget",
            "count",
            "incr",
            "decr",
        )
    },
    "add_op": "submit(CommandRequest(...))",
    "disconnect_all": "node_allocator.close()",
}


@pytest.mark.parametrize(("name", "replacement"), LEGACY_NODE_REPLACEMENTS.items())
def test_every_legacy_node_adapter_has_migration_metadata(name, replacement):
    method = getattr(ArcusMCNode, name)
    assert method is getattr(LegacyNodeCommands, name)
    assert method.__deprecated__ == (
        f"ArcusMCNode.{name}() is deprecated; use node.{replacement}"
        + (" instead." if name in ("add_op", "disconnect_all") else "() instead.")
    )
    assert inspect.signature(method) == inspect.signature(method.__wrapped__)


@pytest.mark.parametrize(
    "name",
    [
        "submit",
        "get_fileno",
        "disconnect",
        "close",
        "process_request",
        "process_operation",
        "expire_operations",
        "do_op",
    ],
)
def test_current_node_transport_methods_are_not_deprecated(name):
    assert not hasattr(getattr(ArcusMCNode, name), "__deprecated__")


def test_canonical_submission_has_no_warning_and_retains_pending_operation():
    node, allocator = make_node()
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        operation = node.commands.kv.get("key")
    assert operation.request == b"get key"
    assert not operation.has_result()
    assert allocator.worker.q.get_nowait() is operation
    assert node.ops == [operation]


def test_legacy_submission_warns_once_and_retains_callback_and_reply_semantics():
    node, allocator = make_node()
    callback = object()
    with pytest.warns(DeprecationWarning, match="node.submit") as caught:
        operation = node.add_op("custom", b"custom noreply", callback, noreply=True)
    assert len(caught) == 1
    assert operation.request == b"custom noreply"
    assert operation.callback is callback
    assert operation.noreply is True
    assert not operation.has_result()
    assert node.ops == []
    assert allocator.worker.q.get_nowait() is operation


def test_legacy_disconnect_all_warns_once_and_retains_allocator_scope():
    node, allocator = make_node()
    with pytest.warns(DeprecationWarning, match="node.node_allocator.close") as caught:
        node.disconnect_all()
    assert len(caught) == 1
    allocator.close.assert_called_once_with()


def test_closed_canonical_submission_remains_terminal_without_warning():
    node, allocator = make_node()
    node._closed = True
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        operation = node.submit(CommandRequest("get", b"get key", None))
    assert operation.invalid
    assert allocator.worker.q.empty()


def run_import_script(script):
    source = str(Path(arcus.__file__).resolve().parents[1])
    env = dict(os.environ, PYTHONPATH=source)
    return subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def test_current_package_imports_emit_no_deprecation_warnings():
    run_import_script(
        "import warnings; warnings.simplefilter('error', DeprecationWarning); "
        "import arcus; import arcus.protocol; import arcus.protocol.commands; "
        "import arcus.protocol.responses"
    )


def test_legacy_module_import_warns_without_changing_exported_class_identities():
    result = run_import_script("""
import json
import warnings
import arcus.protocol as current
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always", DeprecationWarning)
    import arcus_mc_node as legacy
assert len(caught) == 1
assert caught[0].category is DeprecationWarning
assert caught[0].filename == "<string>"
assert "arcus.protocol" in str(caught[0].message)
assert legacy.__deprecated__ == str(caught[0].message)
expected = ["ArcusMCNodeAllocator", "Connection", "EflagFilter", "ArcusMCNode", "ArcusMCPoll", "ArcusMCWorker"]
assert legacy.__all__ == expected
for name in expected:
    assert getattr(legacy, name) is getattr(current, name)
print(json.dumps(expected))
""")
    assert len(json.loads(result.stdout)) == 6


def test_old_locator_watch_callback_warns_and_preserves_event_forwarding():
    locator = ArcusLocator(Mock())
    locator._discovery = Mock()
    event = object()
    with pytest.warns(DeprecationWarning, match="locator.connect") as caught:
        locator.watch_children(event)
    assert len(caught) == 1
    locator._discovery.watch_children.assert_called_once_with(event)
