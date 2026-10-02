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

The distribution contains the `arcus` package and `arcus_mc_node` compatibility
module. Existing imports remain valid. Creating a wheel does not publish a release to a package index.

## Project structure

```text
src/
  arcus/
    client.py          # Public Arcus API
    routing.py         # Consistent hashing and ZooKeeper discovery
    transcoder.py      # Value encoding and decoding
    operation.py       # Asynchronous results and aggregation
    collections.py     # Python List/Set wrappers
    exceptions.py      # Public error types
    protocol/
      connection.py    # Socket I/O, buffering and timeouts
      node.py          # Node state, commands and response parsing
      worker.py        # Request worker and Linux epoll loop
      allocator.py     # Node construction and worker lifecycle
      filter.py        # B+Tree element-flag filters
  arcus_mc_node.py      # Compatibility imports
tests/
  integration/         # Real Arcus and ZooKeeper tests
  legacy/              # Original manual smoke script
tools/                 # Source-only administration utilities
docs/                  # Validation scope and acceptance criteria
```

Implementation modules own their responsibilities; `arcus.__init__` exposes the
public classes. Applications can keep existing imports or import classes from
their specific modules. Install the package, including an editable install for
development, before running it from a checkout.

## Usage

Run this example against a dedicated test service:

```python
from arcus import Arcus, ArcusLocator, ArcusTranscoder
from arcus_mc_node import ArcusMCNodeAllocator

client = Arcus(ArcusLocator(ArcusMCNodeAllocator(ArcusTranscoder())))
client.connect("localhost:2181", "test")
try:
    client.set("example:key", "hello", exptime=60).get_result()
    assert client.get("example:key").get_result() == "hello"
finally:
    client.disconnect()
```

`exptime` is the cache item's expiration time. It is separate from socket timeouts
and the timeout accepted by an operation's `get_result()` method.

The legacy `tests/legacy/client_smoke.py` script exercises basic operations against a live service:

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
python -m pytest
```

Integration tests under `tests/integration/` are excluded from default discovery
and must be selected explicitly after provisioning their services.

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

The standalone administration scripts remain available in the source repository.
They are kept under `tools/` and are not installed as part of the client package.
Their dependencies include
`scramp` and, for `arcus_cmd.py`, `paramiko`, in addition to Kazoo. The utilities
using `arcus_util.py` currently require Python 3.11 or 3.12 because they import
`telnetlib`, which was removed from Python 3.13. This limitation does not apply
to the packaged core client.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
