# Timeouts and connection lifecycle

**English** | [한국어](configuration.ko.md)

For a connection and get/set example, see [README usage](../README.md#usage).

## Cache expiration and result waits

`exptime` is the cache item's expiration time. It is separate from socket timeouts
and the timeout accepted by an operation's `get_result()` method.

`get_result(timeout=5)` limits that caller's wait to five seconds and raises
`queue.Empty` when the result is not ready. It does not cancel the operation;
calling `get_result()` again can retrieve a later result. The default `timeout=0`
waits for the operation's eventual result. Connection invalidation raises
`ArcusNodeConnectionException` instead of returning a cache miss.

## Cache request timeouts

Configure cache request limits when creating the allocator:

```python
from arcus import ArcusMCNodeAllocator, ArcusTranscoder

allocator = ArcusMCNodeAllocator(
    ArcusTranscoder(), connect_timeout=1, io_timeout=1, operation_timeout=5
)
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `connect_timeout` | 1 second | Cache-node connection timeout |
| `io_timeout` | 1 second | Socket I/O timeout |
| `operation_timeout` | 5 seconds | Operation deadline measured from submission |

All three values must be finite and positive. Socket and operation timeouts raise
`TimeoutError`; a failed connection can also invalidate other pending operations.
The poller checks silent connections periodically. Because response parsing uses
blocking I/O, another node can delay deadline detection; these limits do not
constitute a strict end-to-end latency guarantee under arbitrary load.

## Request failures and cleanup

Failed or partially sent requests are not automatically replayed. `noreply`
operations finish after the write succeeds, which does not confirm server-side
execution.

`disconnect()` releases nodes, workers, epoll and ZooKeeper resources. The same
client can connect again after disconnecting.

## ZooKeeper startup and recovery

Initial ZooKeeper connection and discovery reads share a 15-second wait budget.
Failure releases the client resources. Cache-node connection and cleanup time are
additional; this budget is separate from the allocator's cache request limits.
After reconnecting, discovery refreshes asynchronously so ZooKeeper reads do not
block routing requests through the known cache-node list.
