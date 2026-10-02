# arcus-python-client

A Python client for the [Arcus distributed cache](https://github.com/naver/arcus).

## Requirements

- Python 3.11 or newer.
- Linux for the network client, which currently uses `select.epoll()`.
- An Arcus cache service registered with ZooKeeper.

The package declares its runtime dependency on Kazoo. Pure unit tests can run on
other operating systems; those tests do not establish network client support there.

## Installation

Install from a checkout:

```sh
python -m pip install .
```

Or install a wheel produced by the build instructions below:

```sh
python -m pip install dist/arcus_python_client-1.0.0-py3-none-any.whl
```

The distribution contains the `arcus` package and a small `arcus_mc_node`
compatibility module. Existing imports remain valid. Creating a wheel does not
publish a release to a package index.

## Project structure

```text
src/
  arcus/
    client.py          # Compatible public Arcus facade
    api/               # KV/List/Set/BTree APIs and routed request execution
    routing.py         # Consistent hash ring and locator lifecycle
    discovery.py       # ZooKeeper sessions, watches and member snapshots
    transcoder.py      # Value encoding and decoding
    operation.py       # Asynchronous results and aggregation
    collections.py     # Python List/Set wrappers
    exceptions.py      # Public error types
    protocol/
      commands/        # Per-type command objects and shared collection encoding
      responses/       # Per-type response objects and shared byte reader
      request.py       # Immutable command message and submit capability
      connection.py    # Socket I/O, buffering and timeouts
      node.py          # Transport/operation lifetime and compatibility delegates
      worker.py        # Request worker and Linux epoll loop
      allocator.py     # Node construction and worker lifecycle
      filter.py        # B+Tree element-flag filters
  arcus_mc_node.py      # Compatibility imports
tests/
  integration/         # Real Arcus and ZooKeeper tests
  legacy/              # Original manual smoke script
tools/                 # Source-only administration utilities
benchmarks/            # Client comparisons and profiling
stability/             # Isolated fault and resource experiments
docs/                  # Validation scope and acceptance criteria
```

Implementation modules own their responsibilities; `arcus.__init__` exposes the
public classes. Applications can keep existing imports or import classes from
their specific modules. Install the package, including an editable install for
development, before running it from a checkout.

The facade composes data-type APIs, command objects submit immutable requests,
and response objects return decoded values to the node for operation completion.
ZooKeeper discovery publishes membership snapshots to a separate hash ring through
the locator. See [object responsibilities and request flow](docs/architecture.md)
for the collaboration boundaries, state ownership and compatibility details.

## Usage

Run this example against a dedicated test service:

```python
from arcus import Arcus, ArcusLocator, ArcusMCNodeAllocator, ArcusTranscoder

allocator = ArcusMCNodeAllocator(
    ArcusTranscoder(), connect_timeout=1, io_timeout=1, operation_timeout=5
)
client = Arcus(ArcusLocator(allocator))
client.connect("localhost:2181", "test")
try:
    client.set("example:key", "hello", exptime=60).get_result(timeout=5)
    assert client.get("example:key").get_result(timeout=5) == "hello"
finally:
    client.disconnect()
```

`exptime` is the cache item's expiration time. It is separate from socket timeouts
and the timeout accepted by an operation's `get_result()` method.
`get_result(timeout=5)` limits that caller's wait to five seconds and raises
`queue.Empty` when the result is not ready. It does not cancel the operation;
calling `get_result()` again can retrieve a later result. The default `timeout=0`
waits for the operation's eventual result. Connection invalidation raises
`ArcusNodeConnectionException` instead of returning a cache miss.

The allocator defaults are a one-second connection timeout, a one-second socket
I/O timeout, and a five-second operation deadline measured from submission. All
three must be finite and positive. Socket and operation timeouts raise
`TimeoutError`; a failed connection can also invalidate other pending operations.
The poller checks silent connections periodically. Because response parsing uses
blocking I/O, another node can delay deadline detection; these limits do not
constitute a strict end-to-end latency guarantee under arbitrary load.

Failed or partially sent requests are not automatically replayed. `noreply`
operations finish after the write succeeds, which does not confirm server-side
execution. `disconnect()` releases nodes, workers, epoll and ZooKeeper resources;
the same client can connect again after disconnecting.

Initial ZooKeeper connection and discovery reads share a 15-second wait budget.
Failure releases the client resources. Cache-node connection and cleanup time are
additional; this budget is separate from the allocator's cache request limits.
After reconnecting, discovery refreshes asynchronously so ZooKeeper reads do not
block routing requests through the known cache-node list.

The legacy smoke script exercises basic operations against a live service:

```sh
python tests/legacy/client_smoke.py <ZOOKEEPER_HOSTS> <SERVICE_CODE>
```

It writes fixed test keys and requires an isolated service. Importing that script
also starts it; default test discovery excludes `tests/legacy/`.

## Development

Create a virtual environment and install development dependencies:

```sh
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
```

Format Python files with Ruff. Formatting uses four spaces, double quotes, an
88-character target line length, and LF line endings:

```sh
ruff format .
ruff format --check .
```

Run the unit tests without Arcus or ZooKeeper:

```sh
python -m pytest --timeout=30
```

Run the Linux integration suite using Docker Engine and Docker Compose v2:

```sh
./scripts/test-integration.sh
```

The script creates an isolated ZooKeeper service and two Arcus nodes, builds and
installs the wheel in a Linux container, and tests basic APIs and concurrent
requests. It saves results under `build/integration/` and removes its test stack.
See [the integration guide](tests/integration/README.md) for versions, limits,
cleanup, and commands. Integration tests are excluded from default discovery.

GitHub Actions separates unit tests on Python 3.11–3.14, formatting and package
checks, and the Docker integration suite. The unit job runs outside the checkout
against the installed package.

See [the validation scope](docs/validation.md) for test assumptions and acceptance
criteria, [benchmarks](benchmarks/README.md) for client comparisons and profiling,
and [stability experiments](stability/README.md) for isolated faults and resource
checks. Short local measurements do not establish production capacity or long-term
stability. Compression and custom-object serialization remain outside the validated
data formats; production workloads and acceptance targets still need qualification.

## Building distributions

Build a source distribution and a wheel, then validate their metadata:

```sh
python -m build
python -m twine check dist/*
```

`python -m build` builds the wheel from the source distribution by default. Before
releasing, install the wheel in a clean environment outside the source checkout
and run the relevant tests against that installed package.

The package version is declared in `pyproject.toml`; the initial value follows
the existing `1.0.0` entry in `ChangeLog`. For each release, update that version
and document user-visible changes in `ChangeLog`. Uploading to an internal index
or PyPI is a separate release step requiring the selected index and credentials.

## Administration utilities

The standalone administration scripts remain available under [tools/](tools/README.md).
They are not installed as part of the client package. Their dependencies include
`scramp` and, for `arcus_cmd.py`, `paramiko`, in addition to Kazoo. The utilities
using `arcus_util.py` currently require Python 3.11 or 3.12 because they import
`telnetlib`, which was removed from Python 3.13. This limitation does not apply
to the packaged core client.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
