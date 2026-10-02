# Object responsibilities and collaboration

The public `Arcus` facade exposes composed data-type APIs as `kv`, `lop`, `sop`
and `bop`. Each API object has explicit methods such as `client.bop.delete()`. Each node composes command and
response objects around one transport. Components communicate through explicit
method calls, immutable command messages, membership callbacks and asynchronous
operation results. Command implementations use composition. Explicit deprecated adapters live in
`arcus._compat` and are inherited only to preserve old entry points; new request
paths bypass them. There is no dynamic method forwarding or new cache command.

## Responsibilities

| Object | Owns | Collaborates through |
| --- | --- | --- |
| `Arcus` | Public `kv`/`lop`/`sop`/`bop` objects and client lifetime | Four API objects sharing the locator |
| Compatibility adapters | Deprecated flat client and node method signatures | Explicit forwarding to the current API with a migration warning |
| `KeyValueAPI`, `ListAPI`, `SetAPI`, `BTreeAPI` | Data-type API dispatch; BTree multi-node result assembly | Shared `RequestExecutor` and node command objects |
| `RequestExecutor` | Destination lookup and key grouping | `ArcusLocator.get_node()` and an explicit node operation callable |
| `ZooKeeperDiscovery` | ZooKeeper connection, watches, session recovery, snapshot ordering and startup deadline | A callback carrying a list of member names |
| `ConsistentHashRing` | Hash points, node allocation/reuse/retirement and key placement | Node allocator; no ZooKeeper dependency |
| `ArcusLocator` | Discovery/ring composition and synchronized client lifecycle | Discovery, ring and allocator |
| `KVCommands`, `ListCommands`, `SetCommands`, `BTreeCommands`, `AdminCommands` | Command bytes and selection of the response parser | Codec, shared collection builder and `CommandSubmitter.submit()` |
| `CommandRequest` | Immutable command name, payload, parser callback and reply mode | Passed from commands to transport |
| `ArcusMCNode` | Connection lifetime, queue ordering, deadlines and operation completion/failure | Connection, worker/poller and response callback |
| Response objects | Family-specific result shapes and protocol errors | `ResponseReader`; no node, worker or queue reference |
| `ResponseReader` | Reading framed bytes and decoding values | Connection read methods and transcoder |
| `ArcusOperation` / `ArcusOperationList` | Waiting, cached results/errors and multi-node aggregation | Callers and completion notifications |

The shared collection builder constructs common wire options without submitting
requests. `CollectionResponses` reads common collection framing; List, Set and
BTree response objects own their result containers and element interpretation.
`CommandHandlers` and `ResponseHandlers` are composition helpers for a node.

```mermaid
flowchart TD
    Arcus --> API[KV / List / Set / BTree API]
    API --> Executor[RequestExecutor]
    Executor --> Locator[ArcusLocator]
    Locator --> Ring[ConsistentHashRing]
    Discovery[ZooKeeperDiscovery] -->|membership snapshot| Locator
    Executor -->|selected node.commands| Commands[Data-type command object]
    Commands -->|CommandRequest via submit| Node[ArcusMCNode]
    Node --> Worker[Worker / poller]
    Worker -->|send or receive readiness| Node
    Node --> Connection
    Node -->|parse response| Responses[Data-type response object]
    Responses --> Reader[ResponseReader]
    Reader --> Connection
    Node -->|result or error| Operation[ArcusOperation]
    Operation -->|get_result| Caller
```

## A get request and its response

1. `client.kv.get(key)` calls `KeyValueAPI`. Its shared executor resolves the
   key through the locator and invokes the selected node's `commands.kv.get()`.
2. `KVCommands` produces
   a frozen `CommandRequest` containing the wire bytes and `KVResponses.value`
   callback. It knows only the submit capability, codec and response handlers.
3. `ArcusMCNode.submit()` creates an operation and registers its generation,
   deadline and response slot under the existing transport locks. The exact
   operation is returned through the API to the caller.
4. The worker sends the operation through the connection. The poller signals
   read readiness; the node invokes the parser while holding its I/O lock and
   applying the operation's remaining read deadline.
5. The parser uses the reader and transcoder to return a Python value. The node
   completes the operation, waking callers in `get_result()`. Transport/protocol
   failures close the connection and resolve affected pending operations. A
   decoding or unexpected parser error also invalidates the connection unless
   the reader has marked the entire response consumed. Fully consumed domain
   errors and KV decoding errors can preserve the connection safely.

BTree mget/smget use the same per-node flow. `BTreeAPI` asks the executor to group
keys by node, submits each group, and returns `ArcusOperationList`. Multi-node
smget fetches the first `offset + count` elements from each node, merges in the
query's direction, and applies the global offset/count once. A single node
applies its offset on the server. Binary BKeys use bytewise order, with cache keys
as the tiebreaker in the same direction; element flags and values are not compared.
Key-list wire lengths count encoded UTF-8 bytes.

## State ownership and locking

- Commands never access sockets, queues, connection generations or locks.
- Response objects do not close connections or complete operations. They return
  values or raise errors; the node owns transport recovery and completion.
- A node reuses its collaborators under its I/O lock, which serializes response
  reading and connection changes. Response data is local to each call; the reader's
  completion marker is reset before every parser invocation.
- Discovery performs only asynchronous refresh requests on Kazoo callbacks.
  It orders snapshots before invoking the membership callback; no blocking
  ZooKeeper request holds the routing lock.
- Discovery publication can acquire the locator's routing lock. Locator cleanup
  releases that lock before closing discovery, preserving one-way lock ordering.
- The ring publishes a new node set only after allocation succeeds. The locator
  synchronizes ring access and coordinates cleanup after disconnect or setup failure.
  Once published, retirement errors are logged without undoing the applied snapshot
  sequence. Failed retired nodes remain tracked by the allocator for shutdown cleanup.
  Shutdown attempts every node and stops the worker even when a node cleanup fails.

## Compatibility and limits

Existing imports, `Arcus`/`ArcusMCNode` method signatures, key placement and normal
asynchronous result contracts are preserved. `client.locator` remains replaceable.
The List/Set Python wrappers still refer to the same public client and invoke its
`lop`/`sop` APIs. `client.lop.wrap()` and `client.sop.wrap()` construct them;
`alloc()` retains the existing create-and-wrap behavior.
Flat client and node command methods, the old `arcus_mc_node` import and the
`enable_log()` convenience are deprecated. Current top-level public class exports
and locator state views remain supported. See [the migration guide](migration.md)
for replacements and logging configuration. Map commands remain unimplemented.

Discovery/ring state is now owned by its collaborators. `locator.zk`,
`locator.node_list` and `locator.addr_node_map` expose read views; replacing those
attributes directly is not supported. Node connections and transcoders are wired
into collaborators at construction. Replacing `node.handle` or `node.transcoder`
afterward is not a supported reconfiguration mechanism. Reconnecting through the
normal lifecycle continues to use the same connection object and a new socket.

Focused regressions found and corrected three existing command paths while moving
their implementations:

- `node.get_stats()` now returns an operation with the complete `dict[str, str]`
  response, consuming all STAT lines and END before the next queued reply.
- Set deletion separates the payload length from the `drop` option. Successful
  `DELETED_DROPPED` replies now return `True` through the shared deletion parser.
- Set existence pipelines now consume the full `RESPONSE` frame. As with existing
  collection pipelines, operations carrying `pipe=True` report send completion;
  the final operation without `pipe` returns the ordered list of status strings.
  An invalid terminator or `PIPE_ERROR` fails the connection and pending requests.
  Callers must keep a pipeline on one node and prevent unrelated calls from
  interleaving on that connection; this API does not provide atomic batch isolation.

Collection response handlers read each payload by its byte length, preserving
embedded CRLF, binary and empty values. Malformed lengths, headers and terminators
are protocol errors, rather than empty results. An error midway through a collection
frame preserves the failing operation's exception and invalidates other pending
operations before the connection can be reused. A complete BTree `BKEY_MISMATCH`
response raises `CollectionType` and leaves the connection usable. Cached
`ArcusSet.add()` updates its local set only after acknowledged remote success.

Compression/custom-object serialization remains unvalidated. The transport still
performs blocking response reads; this separation does not promise strict latency
bounds under arbitrary load or establish production readiness.

## Validation

`tests/test_command_contracts.py` checks exact command bytes, reply modes, callbacks
and compatibility delegates using a submit-only fake. `tests/test_responses.py`
feeds deterministic byte streams into standalone handlers. API tests cover routing,
grouping, signatures and unchanged operation identity; discovery tests exercise
ordering, session lifecycle and cleanup separately from hashing.

Existing connection, worker, result and deadline regressions remain in the suite.
The isolated Linux integration suite exercises the installed wheel against actual
Arcus and ZooKeeper; the separate stability runner covers failure and recovery.
See [validation scope](validation.md) for the distinction between these checks and
service-specific performance or long-duration acceptance.
