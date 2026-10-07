
# Arcus Python Client

This is a python client driver for Arcus cache.

## Requirement

- Python 3.10 or newer.
- Linux for network operations, which use `select.epoll()`.

## Installation

From the repository root, run:

```sh
python -m pip install .
```

This installs the `arcus` package, the `arcus_mc_node` compatibility module and
their Kazoo dependency.

## Use

After installation, import `arcus` and `arcus_mc_node` as before.
`tests/legacy/client_smoke.py` is a basic functional test for this driver against
a live Arcus service.

Visit arcus cache cloud project at github to get more detail information.
https://github.com/naver/arcus

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
      connection.py    # Socket I/O and buffering
      node.py          # Node state, commands and response parsing
      worker.py        # Request worker and Linux epoll loop
      allocator.py     # Node construction and worker lifecycle
      filter.py        # B+Tree element-flag filters
  arcus_mc_node.py     # Compatibility imports
tests/
  legacy/              # Original manual smoke script
tools/                 # Source-only administration utilities
```

## Administration utilities

The administration scripts are kept under `tools/` and are not installed with the
client package. See `tools/README.md` for their dependencies and supported Python
versions.

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0


