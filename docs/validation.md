# Validation scope and acceptance criteria

The validation baseline covers the existing client on Python 3.11 and newer.
Network tests run on Linux because the client uses `select.epoll()`. Unit tests
on macOS validate isolated behavior, not the Linux network path.

## Environment and workloads

| Item | Baseline | Production input still needed |
| --- | --- | --- |
| Runtime | Python 3.11–3.14; Linux containers based on Debian bookworm | Deployment distribution, Python minor version and CPU architecture |
| Cache service | Arcus 1.16.1, ZooKeeper 3.9.4; two isolated cache nodes for integration | Deployed server versions, replication and topology |
| Basic APIs | get/set/gets/cas/delete/incr/decr; List, Set, B+Tree, mget/smget and Set existence pipelines | Actual API mix, multi-key workloads and collection sizes |
| Values | Strings, bytes, booleans, integers, floats, datetimes; CRLF/binary/empty collection values and UTF-8 multi-keys | Serialization flags, compression, custom objects and further boundary values |
| Concurrency | One shared client, four caller threads, two cache nodes | Processes, threads, connections and outstanding requests |
| Benchmark candidates | Single-node get hits and set; bytes of 64, 1,024 and 4,096 bytes; one and four callers | Arrival rate, read/write and hit/miss ratios, key distribution |
| Package distribution | Build and install a wheel and source distribution | PyPI or internal index, publishing credentials and release policy |

Use unique test keys and a dedicated Docker project without published host ports.
Keep preparation, connection setup, warmup and cleanup outside the benchmark's
measurement interval. Compare acknowledged server replies and verify values;
submission rate alone is not completed throughput. Record any differences in
connection count, runtime, serialization or routing before comparing clients.

The initial concurrency and payload choices are test parameters, not production
recommendations. Run a small correctness check before increasing load. Preserve
the command, code revision, dependency versions, resource limits, random seed,
workload settings and raw results with every experiment.

## Independent time limits

Distinguish the following limits when interpreting an outcome:

| Limit | Meaning |
| --- | --- |
| Cache TTL (`exptime`) | Lifetime of cached data; integration uses 120 seconds |
| Connection timeout | Maximum wait for establishing a cache connection |
| Socket I/O timeout | Limit on an individual blocking send or receive |
| Operation deadline | Limit for a submitted operation, including an unresponsive server |
| `get_result(timeout=...)` | One caller's result wait; integration uses five seconds |
| Test watchdog | Test-process protection; integration uses 45 seconds per test and 180 seconds overall |

Runtime defaults and configuration are described in the [configuration guide](configuration.md).
A caller timing out does not establish whether a write executed on the server.
Do not automatically retry increments or collection inserts after an ambiguous
failure. A watchdog terminating the process is a failed test, not successful
client cleanup.

## Completion criteria

Packaging passes when a wheel built from the source distribution installs outside
the checkout, imports the existing modules, has consistent dependencies, and runs
the relevant tests against the installed package. Keep administration utilities
and their additional dependencies separate from the core distribution.

Functional validation passes when expected values and exceptions match, cache
misses remain distinguishable from connection failures, each response belongs to
its request, and pending work and resources are released on shutdown. Reproduce
reported defects before fixing them and retain focused regression tests.

The review regression suite includes integer and binary BKey smget ordering,
cross-node pagination, single-node offsets and equal-BKey ordering. Socket fakes
also exercise partial reads, malformed frames, mid-response decoding failures,
pipeline failures and out-of-order membership callbacks after retirement errors.
Real-server regressions verify valid wire framing and recovery separately from
those controlled failure cases.

Performance reports must include successful completions per second, p50/p95/p99
completion latency, errors, timeouts and unfinished requests. Repeat measurements
after warmup and inspect both client and server resources. Java results are a
reference with runtime differences, and collection tests do not use pymemcache
as a comparator.

Fault experiments must record injection and recovery times, request outcomes,
membership changes, and resource cleanup. Prove ZooKeeper session expiration
with session IDs or server/client events rather than elapsed time alone. Record
resource trends during sustained load and after stopping it. A short smoke run
only validates the harness and observed interval.

Production throughput, latency percentiles, allowed error rates, recovery times,
sustained-load duration and resource-growth thresholds have not been supplied.
Report measurements and unresolved defects, but leave production acceptance
pending until these service requirements are agreed. Missing service targets do
not prevent building or checking the validation tools.
