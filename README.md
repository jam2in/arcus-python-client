# arcus-python-client

**English** | [한국어](README.ko.md)

A Python client for the [Arcus cache](https://github.com/naver/arcus).

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
deprecated compatibility module. Existing imports remain valid with a warning. Creating a wheel does not
publish a release to a package index.

## Project structure

```text
src/arcus/             # Client package
  api/                 # Key/value, List, Set and B+Tree APIs
  protocol/            # Arcus protocol and network transport
tests/                 # Unit tests
  integration/         # Tests against real Arcus and ZooKeeper
  legacy/              # Original manual smoke script
tools/                 # Standalone administration utilities
benchmarks/            # Performance comparisons and profiling
stability/             # Fault, recovery and sustained-load experiments
docs/                  # Architecture, migration and validation guides
```

See [the architecture guide](docs/architecture.md) for internal responsibilities
and request flow, and [the migration guide](docs/migration.md) for API changes
and logging configuration.

## Usage

Replace the ZooKeeper address (`localhost:2181`) and service code (`test`) with
those of your test service.

```python
from arcus import Arcus, ArcusLocator, ArcusMCNodeAllocator, ArcusTranscoder

allocator = ArcusMCNodeAllocator(ArcusTranscoder())
client = Arcus(ArcusLocator(allocator))
client.connect("localhost:2181", "test")
try:
    client.kv.set("example:key", "hello", exptime=60).get_result(timeout=5)
    print(client.kv.get("example:key").get_result(timeout=5))
finally:
    client.disconnect()
```

Use `client.kv` for key/value commands, `client.lop` for lists, `client.sop` for
sets and `client.bop` for B+Trees. Call `get_result()` to receive a command's result.

See [configuration and error handling](docs/configuration.md) for timeout settings,
and [the migration guide](docs/migration.md) for compatibility and logging.

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
