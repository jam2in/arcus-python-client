# Performance measurements

Run the isolated single-node comparison with Docker Engine and Compose v2:

```sh
./benchmarks/run.sh --collections
```

A quick harness check is:

```sh
./benchmarks/run.sh --sizes 64 --concurrency 1 --repeats 1 --warmup 0.5 --duration 1 --collections
```

Results remain under `build/benchmarks/<run-id>/`. The script refuses an existing
`arcus-python-client-benchmark` project, creates a separate network without host
ports, and removes only that project's containers and volumes on exit. It records
the Git revision, uncommitted tracked source changes, effective Compose file,
Docker version, logs, and server container statistics. Commit benchmark changes
before claiming a reproducible release baseline; a dirty tree is a candidate run.

## Comparison contract

- One Arcus 1.16.1 node, ZooKeeper 3.9.4, pinned service image digests; cache limit
  1 CPU / 256 MiB, benchmark limit 2 CPUs / 1 GiB, cache memory 64 MiB.
- Python 3.11, `pymemcache==4.0.0`, and Maven Central's published
  `com.navercorp.arcus:arcus-java-client:1.13.2`, OpenJDK 17 (`-Xms64m -Xmx256m`).
  The locally reviewed Java source is newer; this measures the pinned published
  release, not that checkout. Its ZooKeeper dependency is explicitly overridden to
  3.9.4: the published old dependency timed out during connection in the JDK 17
  smoke run. The exact runtime versions are written to results.
- Common get-hit/set scenarios use 256 keys cycled uniformly, 64/1024/4096-byte
  `x` payloads, wire flags 2048, TTL 300 seconds, replies enabled, no compression.
  TCP_NODELAY is off for all three clients, matching the current Python client.
  Every get result is compared byte for byte, and every set waits for success.
- Each client reuses one cache connection. Arcus Python and Java share their client
  across workers; pymemcache's basic client uses a lock because one socket cannot
  concurrently parse responses. Lock wait is included. This is a fixed-connection
  comparison, not a claim about pymemcache's best performance with a larger pool.
- Concurrency 1 and 4: each worker waits for its result before issuing another
  operation. Completion latency starts before the client call and includes lock,
  queue, network, parsing, and result waiting. Result validation is included for
  every client. Throughput divides successful completions by actual elapsed time.
  This closed-loop generator reduces issue rate when a client slows; percentiles
  are not corrected for coordinated omission and do not model a fixed arrival rate.
- Warmup and measurement default to 1 and 2 seconds, repeated 3 times. Client order
  rotates between repeats. Seeding, connection establishment, and cleanup are outside
  measurement. These short runs establish a provisional local baseline; they do not
  demonstrate JVM steady state, saturation, production capacity, or an agreed SLO.
  For a controlled environment, increase `--warmup 30 --duration 60` (also increase
  the Dockerfile's outer 1200-second timeout when expanding the whole matrix).
- `--collections` adds Arcus Python List/Set/BTree full reads at 16 and 128 elements,
  one key/one worker. Elements have an 8-byte unique prefix and a 64-byte payload;
  every result is fully checked. Collections are read-only during measurement.
  Collection writes, eflag filters, multi-key mget/smget and Java collection
  comparisons are outside this initial baseline and require separate semantic tests.

`--operations get,set,get-miss,mixed` additionally selects misses and deterministic
80% reads / 20% writes for Python clients. The Java runner supports only `get,set`;
select `--clients arcus-python,pymemcache` for additional operations. Production API
mix, arrival rate, latency/error targets, and acceptable resource limits are unknown.

## Evidence and resource scope

Each `*-summary.jsonl` row stores scenario/repeat, counts, throughput, nearest-rank
p50/p95/p99 of **successful** samples, CPU time, process memory, and before/after
server statistics. `*-samples.json` stores every measured `[duration_ns,status]`;
status 0 is success, 1 unsuccessful set, 2 mismatch, 3 timeout, 4 another exception.
Errors are excluded from successful throughput/latency but retain raw durations.
The summary tool also reports the maximum observed duration. A shared socket lock
can starve an individual caller; its rare long waits can be hidden by request-weighted
p99 even though they remain in the raw samples and maximum.
All returned rows completed their requests; a killed or crashed run is incomplete
and must not be interpreted as zero errors merely because a summary is absent.

Python CPU and server deltas cover measurement; process RSS/thread/fd observations
are after measurement. Java CPU covers measurement; its sampled peak RSS and server
stats include process startup, seeding, warmup and cleanup. Their different resource
scopes are recorded per row and must not be compared as equivalent CPU/memory totals.
Docker stats independently track the server and ZooKeeper every few seconds. Host
load, shared Docker VM contention, network saturation and CPU throttling can still
limit the run. A single shared desktop cannot establish a dedicated-host baseline.

## Profiling

```sh
./benchmarks/run.sh --profile
```

This separately samples the Python 1024-byte get path with four workers for 15 seconds
using `py-spy==0.4.1` at 99 Hz. The benchmark container gets `SYS_PTRACE` so py-spy can
inspect its child process; application containers do not receive that capability.
`profile-active.json` samples active Python threads; `profile-idle.json` also
includes waiting threads, in two separate 15-second runs using speedscope format.
Profiled rows are labeled and excluded by the summary tool.
Profiling affects execution, so profiled measurements are diagnostic only. Use
unprofiled repeated runs for comparisons and rerun them after any optimization.

Generate a comparison table from one unprofiled run:

```sh
python benchmarks/summarize.py build/benchmarks/<run-id>
```

Inspect profile stack shares with the standard library:

```sh
python benchmarks/profile_summary.py \
  build/benchmarks/<profile-run>/profile-active.json \
  build/benchmarks/<profile-run>/profile-idle.json
```

These shares include native calls and lock acquisition, even in the active profile.
They identify paths to investigate; they are not exact CPU time or proof that removing
a lock is safe. Keep correctness and connection-generation tests when testing a
future concurrency optimization.
