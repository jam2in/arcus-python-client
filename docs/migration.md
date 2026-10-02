# Migrating to data-type APIs

`Arcus` owns four reusable API objects. Keep one client and access its methods
through the appropriate namespace:

| Previous call | Preferred call |
| --- | --- |
| `client.set(key, value)` | `client.kv.set(key, value)` |
| `client.get(key)` | `client.kv.get(key)` |
| `client.lop_insert(key, index, value)` | `client.lop.insert(key, index, value)` |
| `client.sop_exist(key, value)` | `client.sop.exist(key, value)` |
| `client.bop_delete(key, range)` | `client.bop.delete(key, range)` |
| `client.bop_smget(keys, range)` | `client.bop.smget(keys, range)` |
| `client.list_alloc(key, flags)` | `client.lop.alloc(key, flags)` |
| `client.list_get(key)` | `client.lop.wrap(key)` |
| `client.set_alloc(key, flags)` | `client.sop.alloc(key, flags)` |
| `client.set_get(key)` | `client.sop.wrap(key)` |

The same mapping applies to all existing KV, List, Set and BTree methods: add
`kv.` to flat KV calls, or replace `lop_`, `sop_` and `bop_` with `lop.`, `sop.`
and `bop.`. Arguments, defaults, key routing, wire encoding and asynchronous
operation results are unchanged. Connection setup remains `client.connect(...)`
and cleanup remains `client.disconnect()`.

```python
client.bop.create("example:tree", ArcusTranscoder.FLAG_STRING, exptime=60).get_result(5)
client.bop.insert("example:tree", 1, "value").get_result(5)
values = client.bop.get("example:tree", (0, 10)).get_result(5)
client.bop.delete("example:tree", (0, 10), drop=True).get_result(5)
```

`lop.wrap()` and `sop.wrap()` return the existing `ArcusList` and `ArcusSet`
wrappers. `alloc()` submits collection creation before returning a wrapper, as
before. To explicitly confirm creation, wait on `create(...).get_result(...)`
first and then use `wrap()`. `get()` on a namespace submits a cache command;
`wrap()` constructs a Python collection wrapper.

## Deprecated entry points

Old flat client methods remain callable in `arcus._compat.client`, inherited by
`Arcus`. Each method carries `@deprecated`, preserves its signature and emits
`DeprecationWarning` with its replacement when called. New namespace calls bypass
these adapters, including inside collection wrappers.

The old `arcus_mc_node` module still re-exports the same class objects, but importing
it emits a deprecation warning. Prefer public imports from `arcus` for
`ArcusMCNodeAllocator` and `EflagFilter`, and `arcus.protocol` for low-level
transport components such as `ArcusMCNode` and `Connection`. Supported top-level
imports such as `from arcus import Arcus, ArcusLocator, ArcusTranscoder` remain
current APIs.

For direct node users, old command delegates are also deprecated:

| Previous call | Preferred call |
| --- | --- |
| `node.get(key)` | `node.commands.kv.get(key)` |
| `node.lop_get(key, range)` | `node.commands.list.get(key, range)` |
| `node.sop_get(key)` | `node.commands.set.get(key)` |
| `node.bop_get(key, range)` | `node.commands.btree.get(key, range)` |
| `node.get_stats()` | `node.commands.admin.get_stats()` |
| `node.flush_all()` | `node.commands.admin.flush_all()` |
| `node.add_op(...)` | `node.submit(CommandRequest(...))` |
| `node.disconnect_all()` | `node.node_allocator.close()` |

Node lifetime, request submission and response processing methods remain active
APIs. Locator state views remain supported. These APIs are lower-level components;
normal application requests should use the four client namespaces. The legacy
`locator.watch_children(event)` callback also warns; `ZooKeeperDiscovery` now
registers its own watches during `locator.connect()`, so applications do not need
to register the old callback themselves.

Python normally hides `DeprecationWarning` outside `__main__`. To find old calls
while migrating, enable warnings or make them errors in your test run:

```sh
python -Wd your_application.py
python -m pytest -W error::DeprecationWarning
```

The Python 3.11-compatible decorator also sets `__deprecated__` metadata and
preserves function metadata with `functools.wraps`. No removal release is scheduled.
The compatibility modules are implementation details, not preferred import paths.

## Logging configuration

Diagnostics use Python's `logging` module under the logger name `arcus` at `DEBUG`
level. The package installs only a `NullHandler`; the application chooses handlers,
formatters, destinations and levels. For a standalone script:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("arcus").setLevel(logging.DEBUG)
```

For applications with existing logging configuration, configure the `arcus` logger
and its handlers there. A handler set to `INFO` or higher still filters out `DEBUG`
records. Debug traces can include cache keys and values.

`enable_log()` is retained as a deprecated convenience: `True` sets the `arcus`
logger level to `DEBUG`, and `False` sets it to `WARNING`. It no longer prints to
stdout or installs a console handler. Replace it with
`logging.getLogger("arcus").setLevel(...)`. `arcuslog(caller, *parts)` remains the
shared diagnostic function and emits through the same logger.
