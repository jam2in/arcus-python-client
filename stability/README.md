# Isolated stability experiments

Run from the repository root with Python 3 and Docker Compose:

```sh
./stability/run.sh --soak-seconds 120
```

The host driver builds the installed-wheel test image and creates only the
`arcus-python-stability` Compose project. Existing containers in that project cause
an immediate refusal. Cache data, service discovery, and fault injection are
isolated from other projects; no host ports or Docker socket are mounted into the
client. The driver always saves logs and removes its own containers, network, and
volume. Ctrl-C and the external watchdog also trigger cleanup.

The default 120-second run is a smoke test for sustained load, not a production
soak qualification. `--soak-seconds` accepts 1–1200 seconds. Longer qualification
runs and service-specific error, latency, recovery, and resource budgets must be
agreed before production approval.

The runner exercises Linux `epoll` against two Arcus 1.16.1 nodes and ZooKeeper
3.9.4. Image digests and service setup come from the root Compose file. It covers:

- Four callers sharing clients, two independent clients, key/value round trips,
  and concurrent List operations using independent keys.
- A 100 ms response delay, an accepted request whose response is discarded, a
  three-second late response, and a connection reset. The lost increment reply
  must produce an error; reading the server afterward verifies a single increment.
- Cache-node graceful stop, forced kill, restart, actual membership comparison,
  manually emptying membership, defined empty-ring errors, and restoration.
  A background caller continues throughout these changes and distinguishes routing
  cache misses from defined connection errors and incorrect values. Membership
  traffic uses a new key for each revision: a reused key can legitimately expose
  an older value on another cache node after a routing change.
- A short ZooKeeper disconnect retaining the session and a disconnect longer than
  the negotiated session timeout. A `LOST` event and changed session ID establish
  session expiration. Membership changes during the outage and again after recovery
  test snapshot refresh and watch registration.
- Four-thread normal load with request IDs, exact result checks, a 256-byte value,
  20 ms pacing between each worker's round trips, queue draining, and five complete
  disconnect/reconnect cycles.

Each run uses UUID-prefixed keys with a 300-second TTL. It does not flush shared
cache state. Node membership is deliberately changed only inside the disposable
stack. Tests use reply mode and never count request submission as successful work.

Manual membership tests restore child names and data with temporary persistent
entries inside this disposable stack. They do not recreate the cache process's
original ephemeral ownership. Real node-session removal is tested by stop/kill
before those manual changes, and each new run creates a fresh ZooKeeper volume.

Cache and ZooKeeper faults use TCP proxies inside the client container. Cache
response loss and delay operate on TCP stream payloads; they are not packet-level
loss simulation. A blocked ZooKeeper proxy terminates established connections and
rejects new ones. It is the only ZooKeeper endpoint configured in the client.
Physical packet loss, asymmetric routes, and partial-send faults need additional
experiments.

The runner configures independent limits: connection 0.5 seconds, I/O 0.5 seconds,
operation completion 2 seconds, and caller result wait 4 seconds. The host watchdog
allows the configured load duration plus 420 seconds, and the container has an
1800-second outer limit. Watchdog termination is a failed run, never successful
client timeout handling. These are reproducible test settings, not agreed service
objectives.

Results are written under `build/stability/<UTC timestamp>-<pid>/`:

- `summary.json`: scenario outcomes, unfinished/duplicate request IDs, actual soak
  duration and request counts, latency percentiles, metric bounds, and session proof.
- `environment.json`: revision, dirty-tree state, project, and load duration.
- `events.jsonl`: scenario transitions, errors, membership, session IDs and states,
  cleanup durations, and latency summaries.
- `requests.jsonl`: command IDs, keys, routing nodes, API entry/enqueue/completion
  times, outcomes, and end-to-end durations. Submission and terminal records are
  separate so an interrupted run preserves the IDs of unfinished requests.
- `metrics.jsonl`: client RSS, CPU time, open FDs, Python threads, worker queue size,
  pending operation count, completed attempts, and errors.
- `server-metrics.jsonl`: Docker CPU, memory, network, and PID observations for the
  two cache containers and ZooKeeper during the run.
- `client.log` and `services.log`: client output and timestamped server logs.

The independent-client/List scenario records per-worker completion summaries;
individual commands in that scenario are not included in `requests.jsonl`.
The latency summary retains at most the last 10,000 attempts, can include faults,
and is a diagnostic rather than a benchmark.
Metrics observe a client containing test proxies, so its total RSS/thread counts
include the fault harness. Inspect trends and queue drain evidence without treating
those totals as a production client's resource footprint.

To regenerate a summary from existing logs:

```sh
python3 stability/summarize.py build/stability/<run-directory>
```
