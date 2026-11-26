# Redis Syncer

**Type:** `redis-syncer`
**Interface:** `CrossReplicaSyncer`

Shares endpoint and request coordination state between EPP replicas through
Redis. Each EPP publishes its local endpoint value. During `Set`, the plugin
prepares a peer aggregate in its in-process cache. `Get` reads the contributor's
live local value and combines it with the cached peer aggregate.

## Configuration

Enable this Alpha plugin with `--allow-experimental-plugins=true`.

```yaml
plugins:
  - type: redis-syncer
    name: redis
    parameters:
      address: my-router-redis:6379
      passwordFile: /var/run/secrets/redis/password
      stateTTL: 2s
      inflightStateTTL: 2s
      prefixStateTTL: 10m
      coordinationTTL: 180s

dataLayer:
  crossReplica:
    syncerPluginRef: redis
```

Parameters:

- `address`: Redis address. Defaults to `localhost:6379`.
- `passwordEnv`: Optional name of an environment variable containing the Redis
  password.
- `passwordFile`: Optional path to a file containing the Redis password.
  Trailing carriage returns and newlines are removed.
  `passwordEnv` and `passwordFile` are mutually exclusive. Plaintext
  `password` configuration is rejected.
- `db`: Redis database number. Defaults to `0`.
- `stateTTL`: Expiration for every kind of synced state in Redis. Defaults to
  `2s` and must be at least `1ms`. It remains the single expiry unless
  `inflightStateTTL` or `prefixStateTTL` overrides it, so a configuration that
  sets only `stateTTL` behaves as before the TTLs were split.
- `inflightStateTTL`: Expiration for a replica's in-flight load and budget
  accounting values. Defaults to `stateTTL` when it is set; otherwise to `2s`.
  Must be at least `1ms`.
- `prefixStateTTL`: Expiration for a replica's prefix windows and their streams.
  Defaults to `stateTTL` when it is set; otherwise to `10m`. Must be at least
  `1ms`.
- `coordinationTTL`: Expiration for request-level coordination values. Defaults
  to `180s` and must be at least `1ms`.
- `warmStartTimeout`: How long a starting replica reads peer state before it
  reports ready. Defaults to `30s` and must be at least `1ms`. The timeout never
  blocks readiness indefinitely: a slow or unreachable Valkey costs at most this
  long at startup, and the replica becomes ready with whatever peer state it
  read. See [Warm start](#warm-start).
- `windowReplication`: How sequenced values such as approximate-prefix windows
  are published. `full` (default) writes the whole value every tick.
  `incremental` appends only new generations to a per-replica Redis stream.
  See [Incremental replication](#incremental-replication).
- `maxBytesPerSecond`: Stream bytes each replica may write, and separately
  read, per second in `incremental` mode. Defaults to `8388608` (8 MiB/s) and
  must not be negative; `0` selects the default. Unused budget accumulates up to
  one second. Values that are not sequenced ignore this limit.

### Publisher timing

Publisher settings are configured under `dataLayer.crossReplica`:

| Setting | Default | Purpose |
|---|---|---|
| `syncInterval` | `200ms` | Interval between endpoint publication ticks. |
| `publishTimeout` | `1s` | Context timeout for one endpoint publish (shared by its concurrent contributor writes), or one batch chunk per contributor when the syncer supports batching. Also bounds one endpoint deletion. Must be positive. |

The Redis syncer publishes endpoints in batches: the publisher calls the
syncer once per contributor per cycle with the live endpoint set, without
holding its endpoint lock, and each chunk runs under one `publishTimeout`. Syncers without batch
support visit endpoints sequentially, with concurrent contributor writes for
each endpoint. Endpoints removed while a batch runs are deleted again after
it finishes. A slow cycle can exceed `syncInterval`. Choose `inflightStateTTL`
longer than the observed interval between successful refreshes of an endpoint,
with margin for Redis latency and variation in cycle duration.

Deletion waits for the publisher's write lock, then starts one timeout shared by
all contributor deletions. An earlier caller deadline takes precedence for both
publication and deletion.

The Redis client honors context deadlines. Cancellation cannot interrupt mutex
waits or local computation, so `publishTimeout` does not guarantee that the
entire operation returns within that duration. Scheduling-path `Get` calls do
not acquire the publisher mutex.

`GetOrSet` uses its caller's context to bound Redis access; `coordinationTTL`
controls how long the stored value is retained. Redis connection, read, and
write timeouts use the client library's defaults and have no plugin parameters.

The `v1alpha1` field names are listed in the
[configuration API migration guide](../../../../../../../docs/architecture.md#api-versions).

### Redis requirements

TLS and Redis ACL usernames are not supported. Connections use unencrypted TCP.
Optional password authentication uses the default Redis user. Managed Redis
services that require TLS or an ACL username cannot use this plugin.

The configured Redis server must support field expiration and `SET NX GET`.
Redis 7.4 or newer is required.

Values stored under the same state key must use a compatible `encoding/gob`
schema. Each EPP decodes peer values into the type supplied by its local
contributor without requiring global `gob.Register` calls.

## State Model

Endpoint state is stored in one Redis hash per state key and endpoint. Each EPP
owns one field in that hash, identified by its hostname and a process-specific
UUID. `Set` refreshes the field TTL, reads the hash in the same transaction,
and caches the prepared peer aggregate locally until the next successful `Set`
replaces it. A replica that stops publishing loses its field `stateTTL` after
its last write, and the next refresh drops it from the aggregate. `Get` reads
the cache without accessing Redis and combines it with the contributor's live
local value. If no peer value is available, `Get` returns the local value.

The expiry depends on what a field holds, because the two kinds of synced state
have different costs:

| Kind | Values | Default TTL |
|---|---|---|
| In-flight accounting | Anything that is not a sequenced value, such as `inflight-load-producer`'s request and token counts. | `inflightStateTTL`, `2s` |
| Prefix windows | Sequenced values such as `approximate-prefix-cache-producer`'s window, including the hash field and the Redis stream beside it. | `prefixStateTTL`, `10m` |

The short accounting TTL bounds how long a crashed replica's in-flight load
counts against its peers; at `syncInterval: 200ms`, `2s` is ten publication
cycles, so a live replica always refreshes well before expiry, and a dead one
drops out within a few seconds. The long prefix TTL is what lets state outlive
a replica process, because a prefix window is expensive to rebuild from traffic.

Both per-kind parameters fall back to `stateTTL` when it is set and they are
not, so a configuration that sets `stateTTL` alone expires every kind of state
on that TTL, exactly as before the split.

### Peer liveness

The prefix TTL keeps a window readable after the replica that wrote it is gone.
That is the point of the long retention, but the window must not keep counting
as a live peer. Each replica therefore writes a liveness field, `refresh:<replica
field>`, next to its value on every publication tick, with `inflightStateTTL`.
A peer counts as active while its liveness field is present; its retained
window still contributes prefix prompts until the prefix TTL removes it.

A replica that leaves the endpoint set has its own field removed, so its window
stops counting immediately for the replica that deleted it. For every other
replica the field expires `inflightStateTTL` after the last publication, which
is the same bound a crashed replica had before.

A hash written before the liveness field existed has none, and is read as it was
before: every replica it names counts as active. A hash that mixes both forms is
read per replica, so a replica running an older version while another writes the
field reports as inactive; this affects only the `active_peers` gauge, since
load and budget accounting are never retained. This lets a fleet roll through
versions that write the field.

### Retention

Prefix state outlives the replica that published it, so a restarted replica, or
every replica after a correlated crash, can rebuild from Valkey while Valkey
itself is up. When an endpoint is deleted, the replica that deletes it removes
only its liveness field. Its value or marker and its stream stay behind, and
keep the field TTL `prefixStateTTL` set by their last publication. Removing an
endpoint therefore does not discard the prefix window that other replicas or a
replacement process may need.

Retained state is garbage-collected by Valkey on that field TTL, counted from
the last publication before the replica departed; nothing else has to run. The
window is readable for the whole retention window, and `active_peers` drops the
replica within `inflightStateTTL`.

A retained window keeps contributing prefix prompts to peer aggregates. Those
prompts were prefetched on the model servers, which are still running, so they
remain useful for routing after the replica that recorded them is gone.

### Warm start

A starting replica reads peer state from Valkey before it reports ready.
`WarmStart` runs once, after the notification sources deliver their initial
endpoint sets and therefore after endpoints exist, and reads:

- every endpoint's hash for each synced state key, then
- the stream elements the peer markers name, so incremental windows are
  restored as well as whole-value ones.

Contributors that implement `CrossReplicaWarmStartMerger`, such as
`approx-prefix-cache-producer`, fold that state into their own index before
readiness flips. The effect is that the prefix index is populated before the
first request rather than on it.

The whole read is bounded by `warmStartTimeout`. If it expires, or Valkey is
unreachable, the replica logs the shortfall, records
`redis_syncer_warm_start_total{outcome="timeout"}` or `outcome="aborted"`, and
becomes ready with the peer state it did read. Readiness is never blocked
indefinitely, because peer state is an optimization: an EPP without it routes on
local state exactly as it did before this feature.

`redis_syncer_warm_start_total{outcome="completed"}` counts a warm start that
read every endpoint, and `redis_syncer_warm_start_peers` reports what it merged.

### Incremental replication

With `windowReplication: incremental`, a replica publishes a sequenced value
as a stream next to the hash, `{<hash key>}:stream:<replica field>`. Entry
`0-(g+1)` holds generation `g`; for approximate-prefix windows a generation is
one prompt, packed as little-endian 8-byte hashes. A prompt larger than 1 MiB
is stored as a retraction of that generation, and readers skip it. The replica's hash field
`stream:<replica field>` holds the marker `epoch base tip size`, naming the
generations `[base, tip)` the stream holds and their total bytes. Each tick
appends generations the replica has not written, trims entries below `base`
when the window evicts prompts, and refreshes the TTLs of the hash, marker
field, and stream. An unchanged window costs a few commands and no payload.

Readers fetch only the generations past the last tip they hold. A withdrawn
prompt keeps its generation until it is dropped from the oldest end. The
writer appends a 9-byte retraction of that generation, and the tip does not
move backwards.
When a write is lost, a stream is missing or truncated, or the tip does move
backwards, the writer starts a new epoch and republishes from `base`; readers
that see a new epoch or a gap reread from `base`. When the byte budget is spent
the writer stops appending and readers stop fetching for that tick, and peers
catch up on later ticks. A cold window, after a restart or reset, reaches
peers over several ticks.

Readers in either mode merge stream peers and whole-value peers, so a fleet
can switch modes one replica at a time: roll out the new version in `full`
mode, then switch replicas to `incremental`.

| Metric | Labels | Meaning |
|---|---|---|
| `redis_syncer_stream_bytes_total` | `state_key`, `direction` | Stream payload bytes written or read. |
| `redis_syncer_stream_budget_exhausted_total` | `state_key`, `direction` | Ticks that deferred stream work because the byte budget was spent. |
| `redis_syncer_stream_resets_total` | `state_key` | New epochs started by the writer. |
| `redis_syncer_stream_peer_lag_generations` | `state_key` | Generations a peer read was behind the peer's tip. |
| `redis_syncer_set_batch_duration_seconds` | `state_key` | Duration of one batched publication. |
| `redis_syncer_valkey_commands_total` | `op`, `result` | Commands sent to Valkey, by command and outcome (`success`, `error`). A pipeline counts each command in it. |
| `redis_syncer_valkey_command_duration_seconds` | `op` | Command wall time. Commands sent in one pipeline share its duration. |
| `redis_syncer_valkey_up` | - | `1` when the last command reached Valkey, `0` when it did not. `0` until the first successful command. |
| `redis_syncer_active_peers` | `state_key` | Peer replicas this replica holds in a cached peer aggregate. |
| `redis_syncer_peer_index_entries` | `state_key` | Sequenced elements merged from peer streams; a whole-value peer counts as one. |
| `redis_syncer_last_successful_sync_timestamp_seconds` | `state_key` | Unix time of the last publication that failed for no endpoint. `time()` minus this is the sync lag. |
| `redis_syncer_stream_budget_bytes_per_second` | `direction` | Effective per-replica `maxBytesPerSecond`; publish and read budgets are separate. |
| `redis_syncer_warm_start_total` | `outcome` | Warm starts attempted: `completed`, `timeout`, or `aborted`. |
| `redis_syncer_warm_start_duration_seconds` | `outcome` | Wall time of one warm start, which is readiness latency added at startup. |
| `redis_syncer_warm_start_peers` | - | Peer values the last warm start merged. |

Request-level coordination uses separate string keys and `SET NX GET` so the
first value stored for a request is selected atomically across EPP replicas.

## Scheduling and Admission

Periodic endpoint synchronization provides peer-aware scheduling state. It is
eventually consistent: two EPP replicas can make scheduling or admission
decisions before either observes the other's latest work.

This synchronization does not implement an atomic cluster-wide admission
quota. Flow control admission across replicas, including fixed per-replica
shares and the `concurrency-detector` dynamic budget, is described in
[Multiple active replicas](../../../../../flowcontrol/README.md#multiple-active-replicas).

The syncer reports the live replica set to `concurrency-detector` through
`ReplicaCounter.ActiveReplicas`, so the divided flow-control budget is sized from
the same heartbeat peers are judged by. It counts the live replicas, this one
included, using the `refresh:<replica field>` liveness fields described in
[Peer liveness](#peer-liveness). A retained prefix window from a departed
replica is not counted, so a long `prefixStateTTL` never inflates the divisor,
and a hash with no liveness field at all keeps the pre-liveness reading in which
every replica it names is live. The count is cached and survives a failed
publication, so an unreachable Valkey leaves each replica on its last share
instead of widening to the full limit.

The load a divided budget is compared against comes from the in-flight load
producer, not from the syncer: `LocalInFlightLoadReader.LocalInFlightLoad`
returns that replica's own counters. The attribute this publisher installs
carries the aggregate across replicas, which routing scorers keep reading
unchanged; counting it against a per-replica share would charge peer load to this
replica and block cluster-wide below the budget.

`redis_syncer_active_peers` and the detector's own
`llm_d_epp_flow_control_budget_replicas` measure different things and are
expected to differ by one: `active_peers` excludes this replica and is labeled
per state key, while `budget_replicas` includes it and is labeled per detector.
A `budget_replicas` of N therefore corresponds to `active_peers` of N-1 for the
state key the detector reads.

`GetOrSet` coordinates one value for one request identifier. It does not reserve
capacity across different requests.

## Deployment

The plugin is a Redis client and does not deploy a Redis server. The server must
be provisioned separately. Plugin construction does not connect to Redis, so an
unavailable server does not prevent the EPP from starting. Failed publications
are logged at error level with rate limiting and leave the peer cache unchanged,
so scheduling keeps counting the last known peer state until a publication
succeeds. A peer that stops while Redis is unreachable keeps counting until
then. Before its first successful publication an EPP uses local state only.

Startup reads peer state once, bounded by `warmStartTimeout`, before readiness
goes true. That read is the only point where an unreachable Valkey delays
readiness; it never prevents it.

`GetOrSet` does not use a local fallback. It returns Redis errors because its
callers require cross-replica atomicity.

## Related Documentation

- [Plugins Index](../../../README.md)
