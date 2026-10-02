"""Byte-level contracts between command collaborators and their submitter."""

import inspect
import queue
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from arcus import ArcusOperation, ArcusTranscoder, CollectionHexFormat, EflagFilter
from arcus.protocol.commands import CommandHandlers
from arcus.protocol.request import CommandRequest
from arcus.protocol.responses import ResponseHandlers
from arcus_mc_node import ArcusMCNode


class RecordingSubmitter:
    """Commands receive only submit; there is no connection, socket, queue or lock."""

    __slots__ = ("requests", "operation")

    def __init__(self):
        self.requests = []
        self.operation = ArcusOperation(None, b"pending", None)

    def submit(self, request):
        self.requests.append(request)
        return self.operation


FILTER = EflagFilter("EFLAG[1:] & 0xff == 0x0a")

# family, method, args, kwargs, wire payload, response owner/method, no-reply
WIRE_CASES = [
    ("kv", "get", ("k",), {}, b"get k", "kv.value", False),
    ("kv", "gets", ("k",), {}, b"gets k", "kv.cas_value", False),
    ("kv", "set", ("k", "v"), {}, b"set k 0 0 1\r\nv", "status.stored", False),
    (
        "kv",
        "set",
        ("k", b"a\x00b"),
        {"exptime": 60},
        b"set k 2048 60 3\r\na\x00b",
        "status.stored",
        False,
    ),
    ("kv", "add", ("k", "v", 60), {}, b"add k 0 60 1\r\nv", "status.stored", False),
    (
        "kv",
        "append",
        ("k", "v", 60),
        {},
        b"append k 0 60 1\r\nv",
        "status.stored",
        False,
    ),
    (
        "kv",
        "prepend",
        ("k", "v", 60),
        {},
        b"prepend k 0 60 1\r\nv",
        "status.stored",
        False,
    ),
    (
        "kv",
        "replace",
        ("k", "v", 60),
        {},
        b"replace k 0 60 1\r\nv",
        "status.stored",
        False,
    ),
    (
        "kv",
        "cas",
        ("k", "v", 123),
        {"exptime": 60},
        b"cas k 0 60 1 123\r\nv",
        "status.stored",
        False,
    ),
    (
        "kv",
        "cas",
        ("k", "v", b"123"),
        {"exptime": 60},
        b"cas k 0 60 1 123\r\nv",
        "status.stored",
        False,
    ),
    ("kv", "incr", ("k",), {}, b"incr k 1", "status.stored", False),
    ("kv", "decr", ("k", 3), {}, b"decr k 3", "status.stored", False),
    ("kv", "delete", ("k",), {}, b"delete k", "status.deleted", False),
    (
        "list",
        "create",
        ("k", 0),
        {},
        b"lop create k 0 0 4000",
        "collection.created",
        False,
    ),
    (
        "list",
        "insert",
        ("k", -1, "v"),
        {},
        b"lop insert k -1 1\r\nv",
        "collection.stored",
        False,
    ),
    (
        "list",
        "insert",
        ("k", 2, "v"),
        {"pipe": True},
        b"lop insert k 2 1 pipe\r\nv",
        "collection.stored",
        True,
    ),
    ("list", "get", ("k", (0, -1)), {}, b"lop get k 0..-1 ", "list.get", False),
    (
        "list",
        "get",
        ("k", 2),
        {"delete": True},
        b"lop get k 2 delete",
        "list.get",
        False,
    ),
    (
        "list",
        "delete",
        ("k", (0, 2)),
        {"drop": True, "pipe": True},
        b"lop delete k 0..2 drop pipe",
        "status.deleted",
        True,
    ),
    (
        "list",
        "delete",
        ("k", 2),
        {"noreply": True},
        b"lop delete k 2  noreply",
        "status.deleted",
        True,
    ),
    (
        "set",
        "create",
        ("k", 0, 60, True),
        {},
        b"sop create k 0 60 4000 noreply",
        "collection.created",
        True,
    ),
    (
        "set",
        "insert",
        ("k", "v"),
        {"noreply": True},
        b"sop insert k 1 noreply\r\nv",
        "collection.stored",
        True,
    ),
    ("set", "get", ("k",), {}, b"sop get k 0 ", "set.get", False),
    ("set", "get", ("k", 2), {"drop": True}, b"sop get k 2 drop", "set.get", False),
    (
        "set",
        "delete",
        ("k", "v"),
        {"pipe": True},
        b"sop delete k 1 pipe\r\nv",
        "status.deleted",
        True,
    ),
    ("set", "exist", ("k", "v"), {}, b"sop exist k 1\r\nv", "set.exist", False),
    (
        "btree",
        "create",
        ("k", 2048, 60),
        {},
        b"bop create k 2048 60 4000",
        "collection.created",
        False,
    ),
    (
        "btree",
        "insert",
        ("k", 4, "v"),
        {},
        b"bop insert k 4 1\r\nv",
        "collection.stored",
        False,
    ),
    (
        "btree",
        "insert",
        ("k", "0x01", "v"),
        {"eflag": "0x0a", "pipe": True},
        b"bop insert k 0x01 0x0a 1 pipe\r\nv",
        "collection.stored",
        True,
    ),
    (
        "btree",
        "upsert",
        ("k", 4, "v"),
        {"noreply": True},
        b"bop upsert k 4 1 noreply\r\nv",
        "collection.stored",
        True,
    ),
    (
        "btree",
        "update",
        ("k", 4, "v"),
        {},
        b"bop update k 4 1\r\nv",
        "collection.stored",
        False,
    ),
    ("btree", "get", ("k", (1, 3)), {}, b"bop get k 1..3 ", "btree.get", False),
    (
        "btree",
        "get",
        ("k", ("0x01", "0x03")),
        {"filter": FILTER},
        b"bop get k 0x01..0x03 1 & 0xff EQ 0x0a ",
        "btree.get",
        False,
    ),
    (
        "btree",
        "get",
        ("k", "0x01"),
        {"delete": True},
        b"bop get k 0x01 delete",
        "btree.get",
        False,
    ),
    ("btree", "get", ("k", 1), {"drop": True}, b"bop get k 1 drop", "btree.get", False),
    (
        "btree",
        "delete",
        ("k", (1, 3)),
        {"filter": FILTER, "count": 2, "drop": True, "noreply": True},
        b"bop delete k 1..3 1 & 0xff EQ 0x0a 2 drop noreply",
        "status.deleted",
        True,
    ),
    (
        "btree",
        "delete",
        ("k", ("0x01", "0x03")),
        {},
        b"bop delete k 0x01..0x03 ",
        "status.deleted",
        False,
    ),
    (
        "btree",
        "delete",
        ("k", "0x01"),
        {"pipe": True},
        b"bop delete k 0x01  pipe",
        "status.deleted",
        True,
    ),
    ("btree", "delete", ("k", 1), {}, b"bop delete k 1 ", "status.deleted", False),
    (
        "btree",
        "count",
        ("k", (1, 3), None),
        {},
        b"bop count k 1..3 ",
        "btree.get",
        False,
    ),
    (
        "btree",
        "incr",
        ("k", 3, 5),
        {"noreply": True},
        b"bop incr k 3 5 noreply",
        "status.stored",
        True,
    ),
    (
        "btree",
        "decr",
        ("k", "0x03", 5),
        {"pipe": True},
        b"bop decr k 0x03 5 pipe",
        "status.stored",
        True,
    ),
    (
        "btree",
        "mget",
        (["a", "bb"], (1, 3)),
        {},
        b"bop mget 4 2 1..3 50\r\na,bb",
        "btree.mget",
        False,
    ),
    (
        "btree",
        "mget",
        (["a", "bb"], ("0x01", "0x03")),
        {"filter": FILTER, "offset": 2, "count": 7},
        b"bop mget 4 2 0x01..0x03 1 & 0xff EQ 0x0a 2 7\r\na,bb",
        "btree.mget",
        False,
    ),
    (
        "btree",
        "smget",
        (["a", "bb"], "0x03"),
        {},
        b"bop smget 4 2 0x03 2000\r\na,bb",
        "btree.smget",
        False,
    ),
    (
        "btree",
        "smget",
        (["a"], 3),
        {"offset": 0, "count": 2},
        b"bop smget 1 1 3 0 2\r\na",
        "btree.smget",
        False,
    ),
    ("admin", "flush_all", (), {}, b"flush_all", "admin.ok", False),
    ("admin", "get_stats", (), {}, b"stats", "admin.stats", False),
    ("admin", "get_stats", ("items",), {}, b"stats items", "admin.stats", False),
]


def make_commands():
    submitter = RecordingSubmitter()
    transcoder = ArcusTranscoder()
    # Response handlers are inert until the node invokes the selected callback.
    responses = ResponseHandlers(object(), transcoder)
    return CommandHandlers(submitter, transcoder, responses), submitter, responses


@pytest.mark.parametrize("method", ["mget", "smget"])
def test_multi_key_header_measures_utf8_payload_bytes(method):
    handlers, submitter, _ = make_commands()
    keys = ["한글", "café", "emoji😀"]

    getattr(handlers.btree, method)(keys, (0, 10), count=2)

    header, payload = submitter.requests[0].payload.split(b"\r\n", 1)
    assert payload == ",".join(keys).encode("utf-8")
    assert header == f"bop {method} {len(payload)} 3 0..10 2".encode("ascii")


@pytest.mark.parametrize("entry", WIRE_CASES)
@pytest.mark.parametrize("entrypoint", ["command", "legacy_node"])
def test_wire_bytes_response_contract_and_operation_identity(entry, entrypoint):
    family, method, args, kwargs, payload, response_path, noreply = entry
    handlers, submitter, responses = make_commands()
    if entrypoint == "command":
        command = getattr(getattr(handlers, family), method)
    else:
        allocator = SimpleNamespace(
            shutdown=False, worker=SimpleNamespace(q=queue.Queue())
        )
        with patch("arcus.protocol.node.Connection"):
            node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)
        node.submit = submitter.submit
        responses = node.responses
        prefix = {
            "kv": "",
            "list": "lop_",
            "set": "sop_",
            "btree": "bop_",
            "admin": "",
        }[family]
        command = getattr(node, prefix + method)

    returned = command(*args, **kwargs)

    assert returned is submitter.operation
    assert not returned.has_result()
    assert len(submitter.requests) == 1
    request = submitter.requests[0]
    owner, response_method = response_path.split(".")
    name = {
        "kv": method,
        "list": "lop " + method,
        "set": "sop " + method,
        "btree": "bop " + method,
        "admin": "stats" if method == "get_stats" else method,
    }[family]
    assert request == CommandRequest(
        name, payload, getattr(getattr(responses, owner), response_method), noreply
    )


@pytest.mark.parametrize(
    ("family", "command"), [("list", b"lop"), ("set", b"sop"), ("btree", b"bop")]
)
def test_create_options_keep_existing_attribute_defaults(family, command):
    handlers, submitter, _ = make_commands()
    attributes = {"ovflaction": "head_trim", "readable": False}

    getattr(handlers, family).create("k", 2048, 60, True, attributes)

    assert (
        submitter.requests[0].payload
        == command + b" create k 2048 60 4000 head_trim unreadable noreply"
    )
    assert submitter.requests[0].noreply is True
    assert attributes == {
        "maxcount": 4000,
        "ovflaction": "head_trim",
        "readable": False,
    }


@pytest.mark.parametrize(
    ("family", "args", "expected"),
    [
        (
            "list",
            ("k", -1, "v"),
            b"lop insert k -1 1 create 0 0 4000 head_trim unreadable\r\nv",
        ),
        (
            "set",
            ("k", "v"),
            b"sop insert k 1 create 0 0 4000 head_trim unreadable\r\nv",
        ),
        (
            "btree",
            ("k", "0x01", "v"),
            b"bop insert k 0x01 1 create 0 0 4000 head_trim unreadable\r\nv",
        ),
    ],
)
def test_insert_create_options_keep_existing_attribute_defaults(family, args, expected):
    handlers, submitter, _ = make_commands()
    attributes = {"ovflaction": "head_trim", "readable": False}

    getattr(handlers, family).insert(*args, attr=attributes)

    assert submitter.requests[0].payload == expected
    assert attributes == {
        "flags": 0,
        "exptime": 0,
        "maxcount": 4000,
        "ovflaction": "head_trim",
        "readable": False,
    }


@pytest.mark.parametrize(
    ("method", "args", "kwargs"),
    [
        ("insert", ("k", "01", "v"), {}),
        ("insert", ("k", 1, "v"), {"eflag": "01"}),
        ("get", ("k", ("0x01", "03")), {}),
        ("get", ("k", "03"), {}),
        ("delete", ("k", ("01", "0x03")), {}),
        ("delete", ("k", "03"), {}),
        ("mget", (["k"], ("0x01", "03")), {}),
        ("smget", (["k"], "03"), {}),
        ("incr", ("k", "03", 1), {}),
    ],
)
def test_bad_hex_keys_and_flags_fail_before_submission(method, args, kwargs):
    handlers, submitter, _ = make_commands()
    with pytest.raises(CollectionHexFormat):
        getattr(handlers.btree, method)(*args, **kwargs)
    assert submitter.requests == []


@pytest.mark.parametrize(
    ("family", "method", "args"),
    [
        ("list", "insert", ("k", -1, "v")),
        ("list", "delete", ("k", 1)),
        ("set", "insert", ("k", "v")),
        ("set", "delete", ("k", "v")),
        ("btree", "insert", ("k", 1, "v")),
        ("btree", "delete", ("k", 1)),
        ("btree", "incr", ("k", 1, 2)),
    ],
)
def test_noreply_and_pipe_remain_mutually_exclusive(family, method, args):
    handlers, submitter, _ = make_commands()
    with pytest.raises(AssertionError):
        getattr(getattr(handlers, family), method)(*args, noreply=True, pipe=True)
    assert submitter.requests == []


def test_set_exist_pipe_builds_a_request_without_an_undefined_noreply_option():
    handlers, submitter, responses = make_commands()
    operation = handlers.set.exist("k", "v", pipe=True)
    assert operation is submitter.operation
    assert submitter.requests == [
        CommandRequest(
            "sop exist", b"sop exist k 1 pipe\r\nv", responses.set.exist, True
        )
    ]


def test_command_request_cannot_change_after_submission():
    handlers, submitter, _ = make_commands()
    handlers.kv.get("k")
    with pytest.raises(FrozenInstanceError):
        submitter.requests[0].payload = b"get other"


# Existing node entry points remain callable with the same named parameters.
NODE_SIGNATURES = {
    "__init__": "(self, addr, name, transcoder, node_allocator)",
    "get_fileno": "(self)",
    "disconnect": "(self)",
    "close": "(self)",
    "disconnect_all": "(self)",
    "process_request": "(self, request)",
    "process_operation": "(self, op)",
    "expire_operations": "(self)",
    "get": "(self, key)",
    "gets": "(self, key)",
    "set": "(self, key, val, exptime=0)",
    "cas": "(self, key, val, cas_id, exptime=0)",
    "incr": "(self, key, value=1)",
    "decr": "(self, key, value=1)",
    "add": "(self, key, val, exptime=0)",
    "append": "(self, key, val, exptime=0)",
    "prepend": "(self, key, val, exptime=0)",
    "replace": "(self, key, val, exptime=0)",
    "delete": "(self, key)",
    "flush_all": "(self)",
    "get_stats": "(self, stat_args=None)",
    "lop_create": "(self, key, flags, exptime=0, noreply=False, attr=None)",
    "lop_insert": "(self, key, index, value, noreply=False, pipe=False, attr=None)",
    "lop_delete": "(self, key, range, drop=False, noreply=False, pipe=False)",
    "lop_get": "(self, key, range, delete=False, drop=False)",
    "sop_create": "(self, key, flags, exptime=0, noreply=False, attr=None)",
    "sop_insert": "(self, key, value, noreply=False, pipe=False, attr=None)",
    "sop_get": "(self, key, count=0, delete=False, drop=False)",
    "sop_delete": "(self, key, val, drop=False, noreply=False, pipe=False)",
    "sop_exist": "(self, key, val, pipe=False)",
    "bop_create": "(self, key, flags, exptime=0, noreply=False, attr=None)",
    "bop_insert": "(self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None)",
    "bop_upsert": "(self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None)",
    "bop_update": "(self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None)",
    "bop_delete": "(self, key, range, filter=None, count=None, drop=False, noreply=False, pipe=False)",
    "bop_get": "(self, key, range, filter=None, delete=False, drop=False)",
    "bop_mget": "(self, key_list, range, filter=None, offset=None, count=50)",
    "bop_smget": "(self, key_list, range, filter=None, offset=None, count=2000)",
    "bop_count": "(self, key, range, filter)",
    "bop_incr": "(self, key, bkey, value, noreply=False, pipe=False)",
    "bop_decr": "(self, key, bkey, value, noreply=False, pipe=False)",
    "add_op": "(self, cmd, full_cmd, callback, noreply=False)",
    "do_op": "(self)",
}


@pytest.mark.parametrize(("method", "signature"), NODE_SIGNATURES.items())
def test_legacy_node_signatures_remain_compatible(method, signature):
    assert str(inspect.signature(getattr(ArcusMCNode, method))) == signature


def test_set_delete_drop_separates_the_byte_length_and_drop_option():
    handlers, submitter, responses = make_commands()
    operation = handlers.set.delete("k", "v", drop=True)
    assert operation is submitter.operation
    assert submitter.requests == [
        CommandRequest(
            "sop delete", b"sop delete k 1 drop\r\nv", responses.status.deleted
        )
    ]
