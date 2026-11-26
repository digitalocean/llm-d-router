# Concurrency Detector Plugin

**Type:** `concurrency-detector`

Synchronous saturation detection and scheduling filter mechanism based on active in-flight request accounting.

## What it does

This plugin uses a two-tier approach to manage average pool load and protect individual endpoints.

### Role in Flow Control (The Gatekeeper)
The detector implements the `SaturationDetector` interface to provide a utilization gradient, allowing the Flow Controller to apply proportional backpressure.

    PoolSaturation = Aggregate Inflight Load / Aggregate Pool Capacity

In token mode, both numerator and denominator are evaluated in tokens: the aggregate inflight token count divided by the sum of all endpoints' MaxTokenConcurrency.

Hybrid mode is the exception: rather than one aggregate fraction, it evaluates each endpoint's saturation as the larger of its request and token ratios and reports the unweighted average across endpoints. This prevents distinct endpoints saturating on different dimensions from being masked by aggregate ratios that each remain low.

**Dispatch reservations:** The detector implements `DispatchReservationTracker`. Flow control reserves a slot when it dispatches a request and the director releases it after the `PreRequest` hooks have published the request's in-flight load, so a burst of dispatches cannot all read the same headroom. Pending reservations are added to the aggregate in-flight request count in `requests` mode. In `hybrid` mode the reported saturation is the larger of the per-endpoint average and the aggregate request ratio including reservations. `tokens` mode does not count reservations. Reservations are pool-wide and local to the EPP replica: a reservation counts against this detector whichever endpoint the request is later scheduled to, and it is not shared with other replicas.

**Heterogeneous Deployments:** Because this detector calculates saturation globally as a single aggregate fraction (in requests and tokens mode), it utilizes an aggregate queueing model. In deployments with heterogeneous compute (e.g., mixing H100 and L4 nodes), this heavily biases the pool saturation metric toward the state of the larger nodes. Contrast this with the Utilization Detector, which evaluates saturation as an unweighted average of individual endpoint scores.

### Role in Scheduling (The Traffic Shaper)
The detector implements the `Filter` interface to protect individual endpoints. It removes endpoints from candidate lists if their local inflight count exceeds the safety limit:

    EndpointLimit = Capacity * (1 + Headroom)

In tokens and hybrid modes the token check also counts the uncached tokens the incoming request would add to each endpoint (the `UncachedRequestTokens` attribute from the in-flight load producer), so an endpoint is removed when admitting the request would take it over the limit:

    InflightTokens + IncomingUncachedTokens <= TokenLimit

An endpoint with no in-flight tokens always passes the token check. A request larger than the limit can still be placed on an idle endpoint, which is the best placement the pool can offer, and the engine's own limits decide whether it runs.

This approach allows the Flow Controller to manage average pool load, while the Scheduler retains the flexibility to burst above ideal targets (the "Headroom") to satisfy affinity or scoring objectives.

**Fail-Open Fallback:** To prevent complete routing failure, if *all* candidate endpoints are filtered out (i.e., the entire cluster is over the safety limits), the filter softens and returns the original list of endpoints, allowing the scheduler's scorers to pick the least-bad option. With `failOpen: false` the filter returns no endpoints instead, so the profile finds no endpoint and the request fails rather than overloading one.

`failOpen: false` sheds requests, it does not queue them. With flow control enabled, the two gates use different inputs: flow control releases a request when pool saturation, computed from current load, is below 1, while the filter checks current load plus the incoming request. A request that flow control has released can therefore still be rejected by the filter when it fits on no endpoint.

## Inputs consumed

The plugin reads the per-endpoint load that an [in-flight load producer](../../../requesthandling/dataproducer/inflightload/README.md) publishes, selected by `inFlightLoadProducerName`:
- **All modes**: the `InFlightLoad` attribute, with in-flight requests and tokens per endpoint.
- **Tokens and hybrid modes**: also the `UncachedRequestTokens` attribute, the uncached tokens the incoming request would add to each endpoint.

## Configuration

The plugin accepts JSON parameters decoding to the following fields:

- `concurrencyMode` (`string`): Evaluation mode. Valid values are `"requests"`, `"tokens"`, or `"hybrid"`. In `"hybrid"` mode both request and token accounting are evaluated. Pool saturation is computed per endpoint as the larger of that endpoint's request and token ratios, then averaged across endpoints, so an endpoint saturated on either dimension is reflected even when distinct endpoints saturate on different dimensions. An endpoint is filtered out when either its request load reaches the limit or its token load plus the request's uncached tokens exceeds the limit. (Default: `"requests"`)
- `maxConcurrency` (`int64`): Maximum requests in flight. Serves as the "ideal" request capacity for a single endpoint. Must be > 0. (Default: `100`)
- `maxTokenConcurrency` (`int64`): Maximum tokens in flight. The "tokens" mode equivalent of `maxConcurrency`. Must be > 0. (Default: `1000000`)
- `headroom` (`float64`): Allowed burst capacity above the ideal threshold, expressed as a fraction (e.g., `0.2` for 20%). Must be >= 0.0. (Default: `0.0`)
- `failOpen` (`bool`): Whether the filter returns all candidates when every candidate is over its limit. When `false` it returns none. (Default: `true`)
- `inFlightLoadProducerName` (`string`): Name of the `inflight-load-producer` instance to read. Unset selects the instance named `inflight-load-producer`, the name an unnamed instance gets. (Default: unset)
- `dynamicBudget` (`bool`): Divide `maxConcurrency` and `maxTokenConcurrency` across the live EPP replicas, and compare the share against this replica's own in-flight load. See [Dynamic budget](#dynamic-budget). (Default: `false`)
- `budgetOversubscription` (`float64`): Multiplier on each replica's share when the limit is divided, to tolerate uneven load across replicas. Must be > 0. (Default: `1.0`)
- `expectedReplicas` (`int`): Replica count the limit is divided by before the cross-replica syncer reports one, such as during a cold start. Must be >= 0; `0` leaves the deployment size unknown. (Default: `0`)

## Dynamic budget

With N live EPP replicas each enforcing the full `maxConcurrency`, the deployment admits up to N
times the engine capacity. `dynamicBudget: true` shares one deployment-wide limit instead:

```
effective = ceil(global_limit / live_replicas * budgetOversubscription)
```

and each replica admits that share **of its own in-flight load**. Global 10 over two live replicas is
5 each. When one replica's liveness expires the survivor's share becomes 10; when it returns, both
drop back to 5.

### Why the share is measured against local load

Synced peer load arrives asynchronously. During a burst every replica can admit the full limit before
it sees the others' admissions, so a synced aggregate compared against a full limit allows up to N
times the budget. Measuring a per-replica share against the replica's own load bounds the total at
the deployment-wide limit even while replicas have not seen each other yet.

That is why the detector reads `LocalInFlightLoadReader.LocalInFlightLoad` from the in-flight load
producer named by `inFlightLoadProducerName`, rather than the endpoint's `InFlightLoad` attribute. The
cross-replica publisher replaces that attribute with an aggregate across peers, which is the right
input for routing scorers and for the full limit, but against a share it would count peer load as this
replica's own: global 10, share 5, replica 1 at 3 and replica 2 at 2 would look full at 5 and block
cluster-wide far below the budget.

Routing scorers are unaffected. They keep reading the `InFlightLoad` attribute, which still carries
the synced aggregate, so `token-load-scorer`, `active-request-scorer` and the rest keep the
deployment-wide view. With `dynamicBudget: false` the detector also reads that attribute, exactly as
before.

### Trade-off: early shedding under imbalance

A replica caps at its share even while another replica sits idle, so traffic that is unevenly spread
across replicas waits in a queue while the pool has spare capacity. This is deliberate: the
alternative lets every replica admit the full limit during the sync lag and overshoots the budget by
up to N times. `budgetOversubscription` above 1.0 trades some of that headroom back.

### Replica count

`live_replicas` comes from the `CrossReplicaSyncer` configured as
`dataLayer.crossReplica.syncerPluginRef`. With `redis-syncer` it counts the replicas that hold a
`refresh:<replica field>` liveness field, which every replica rewrites with `inflightStateTTL` on each
publication tick. A peer leaves the count when that field expires, and joins it when it first
publishes. The liveness field matters because prefix windows are retained past the replica that wrote
them, on the much longer `prefixStateTTL`: counting value fields alone would keep a departed replica
in the divisor for up to ten minutes after its load stopped counting, shrinking every survivor's
share for a replica that takes no traffic. A hash with no liveness field at all predates the field
and is read as before, with every replica it names counting. This replica always counts itself.

- **No replica count yet.** Before the first successful publication the limit is divided by
  `expectedReplicas` when set, else kept at the full global limit. Set `expectedReplicas` to the
  steady-state replica count so a cold start does not over-admit for one publication cycle.
- **Store unreachable.** The syncer keeps reporting its last count, so each replica keeps its share.
  It never widens to the global limit, which would over-admit, and never drops to zero.
- **Rounding.** Shares round up, so the replicas together never fall short of the global limit
  through rounding. `budgetOversubscription` above 1.0 deliberately admits past it to absorb skew.

### Configuration

```yaml
- type: concurrency-detector
  parameters:
    maxConcurrency: 10            # deployment-wide requests
    maxTokenConcurrency: 352000   # deployment-wide tokens
    dynamicBudget: true
    budgetOversubscription: 1.0
    expectedReplicas: 2           # cold-start divisor until a count is known
```

Point the detector at a producer that reports local load, which `inflight-load-producer` does. A
producer without that capability leaves the detector on the attribute the cross-replica publisher may
have replaced, and the detector logs once at startup saying so.

Metrics, labeled by the detector name:

| Metric | Meaning |
|---|---|
| `llm_d_epp_flow_control_effective_request_budget` | Requests this replica may have in flight. |
| `llm_d_epp_flow_control_effective_token_budget` | Tokens this replica may have in flight. |
| `llm_d_epp_flow_control_budget_replicas` | Live replica count the limit was divided by; `1` when the full limit is in force. |
| `llm_d_epp_flow_control_local_inflight_requests` | Requests in flight on this replica that the detector counts. |
| `llm_d_epp_flow_control_local_inflight_tokens` | Tokens in flight on this replica that the detector counts. |

With `dynamicBudget: false` the effective budgets report the configured limits, `budget_replicas`
reports `1`, and the local-load gauges stay at 0: the detector reads the attribute map and nothing is
divided.

`llm_d_epp_flow_control_budget_replicas` and `redis-syncer`'s `redis_syncer_active_peers` both count
replicas but are not the same series, and their values differ by one by design:
`budget_replicas` includes this replica and is labeled by detector, while `active_peers` excludes it
and is labeled by state key. A `budget_replicas` of N corresponds to `active_peers` of N-1 for the
state key the detector reads.

## Trade-offs

Unlike the Utilization Detector, this approach reacts instantaneously to new requests, preventing sudden bursts from overwhelming an endpoint before telemetry updates. However, it suffers from two critical flaws:

1. **Open-Loop Divergence**: The detector operates as an open-loop controller, completely blind to actual hardware telemetry. While the internal counters are mathematically zero-sum and do not leak, consistent under- or over-estimations of token lengths will cause the view of pool saturation to systematically drift from the physical reality of the GPU workload.
2. **KV Cache Blindness**: Because the detector cannot observe true engine memory pressure, it is highly vulnerable to continuous-batching edge cases. If actual output generations exceed static estimates, the underlying KV cache will silently fill up. This forces the inference engine to preempt active requests and swap KV blocks to CPU memory, causing severe latency degradation (TPOT spikes) that remains completely invisible to this detector.
