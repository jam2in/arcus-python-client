# Linux integration tests

Run from the repository root with Docker Engine and Docker Compose v2 available:

```sh
./scripts/test-integration.sh
```

The script creates the `arcus-python-client-test` Compose project with a separate
network and no published host ports. It does not use another project's running
Arcus or ZooKeeper services. A project already containing containers is rejected
to avoid interrupting another test run.

The environment contains:

- ZooKeeper 3.9.4, pinned by image digest.
- A one-shot `jam2in/zkcli:3.5.9` registration container, pinned by digest, which
  resolves the cache containers to IP addresses and registers them under service
  code `python-client-test`. Cache processes wait for registration to finish
  before starting; the coordination volume is removed with the test stack.
- Two Arcus 1.16.1 nodes, pinned by image digest, each with 64 MiB of cache memory.
- A Python 3.11 Linux container (`python:3.11-slim-bookworm`) that builds a wheel
  and installs its `test` extra. Client source files are absent from its
  `/validation` working directory.

ZooKeeper and cache protocol health checks precede the test run. The fixture also
waits for both nodes to appear in ZooKeeper before creating the real client, whose
worker uses Linux `epoll`. Each test uses UUID-prefixed keys, explicit deletion,
and a 120-second cache TTL. Result waits have a separate five-second limit.

The suite covers primitive value round trips, cache misses, deletion, counters,
CAS, List/Set/B+Tree operations, collection errors followed by another request,
and a shared client making concurrent requests across both nodes. It is a basic
correctness suite; performance, node changes, session expiration, and sustained
failure/recovery experiments remain separate validation work.

The script limits service startup to 120 seconds. Each test has a 45-second
`pytest-timeout` deadline, and the container limits the complete test process to
180 seconds plus a ten-second termination grace period. These limits also catch
hangs during client cleanup; they are test-run limits, not client configuration.
Teardown exercises `client.disconnect()`, checks that both worker threads stop,
and verifies that the client releases ZooKeeper and epoll handles itself.

Results remain under `build/integration/<run-id>/` after the script removes its
containers, network, and volumes:

- `integration.xml`: JUnit results, when pytest exits normally.
- `tests.log`: client test output, including timeout diagnostics.
- `services.log`: ZooKeeper, registration, and cache server logs.

Additional pytest arguments are forwarded to the container:

```sh
./scripts/test-integration.sh -k collection
```

To run against an independently prepared isolated Linux environment, install the
package's `test` extra and set all service details explicitly:

```sh
ARCUS_TEST_ZOOKEEPER=127.0.0.1:2181 \
ARCUS_TEST_SERVICE_CODE=isolated-test \
ARCUS_TEST_NODE_COUNT=2 \
python -m pytest tests/integration -v --timeout=45 --timeout-method=thread
```

The older `python3 tests/legacy/client_smoke.py <ZOOKEEPER_HOSTS> <SERVICE_CODE>` script remains a
separate manual integration entry point. It uses fixed keys and should only be
run against an isolated service. This suite adapts its basic cases into fixtures
with bounded result waits and independent test keys.
