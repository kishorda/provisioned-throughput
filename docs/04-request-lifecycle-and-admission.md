# 04 · Request Lifecycle & Admission Control

> Decision records: [ADR-002](adr/ADR-002-debt-based-wu-bucket.md), [ADR-003](adr/ADR-003-lease-based-distributed-quota.md), [ADR-012](adr/ADR-012-single-instance-quota-coordinator.md), [ADR-027](adr/ADR-027-quota-coordinator-standby.md), [ADR-028](adr/ADR-028-input-token-counting.md), [ADR-030](adr/ADR-030-gateway-prefix-cache-index.md)

## 1. End-to-end sequence

```mermaid
sequenceDiagram
  autonumber
  participant C as Client / agent
  participant G as PT Gateway (Rust)
  participant Q as Quota Coordinator
  participant R as Tenant-aware Router (Dynamo)
  participant P as Prefill worker
  participant D as Decode worker
  participant M as Metering

  Note over G,Q: Gateway holds a WU lease per reservation (renewed every ~250 ms)
  C->>G: POST /v1/chat/completions (deployment, session_id)
  G->>G: auth → resolve deployment → reservation, tier, policy
  G->>G: tokenise (HF tokenizers) + prefix block hashes
  G->>G: estimate WU (exact prefill, predicted decode, KV-sec)
  alt within local lease
    G->>G: debit bucket (class = provisioned)
  else lease exhausted
    G->>G: apply boundary policy (burst / queue / spillover / reject)
  end
  G->>R: forward + {tenant, class, wu_est, prefix_hashes, deadline}
  R->>R: WFQ by WU within priority class, score workers by KV overlap × load
  R->>P: dispatch when worker pulls (credit available)
  P->>D: KV blocks via NIXL (RDMA)
  D-->>G: token stream
  G-->>C: SSE stream (TTFT stamped at first byte out)
  D->>M: actual tokens, KV-token-seconds, phase timings
  G->>G: settle: actual WU − estimated WU → bucket
  G->>M: UsageRecord (class, WU, latencies, reason codes)
  G-->>Q: periodic debt/consumption report with lease renewal
```

## 2. Gateway pipeline (hot path)

Built on Pingora (or hyper + tower) as a chain of tower layers. Each stage is
allocation-light and has a latency budget:

| Stage | Budget (p99) | Notes |
|-------|--------------|-------|
| TLS + HTTP parse | amortised | Keep-alive, HTTP/2 |
| Auth + deployment resolve | 50 µs | In-memory map from the entitlement snapshot. API keys hashed with SipHash/BLAKE3 |
| Tokenise | ≤ 2.5 ms (4 KB of new text) | HF `tokenizers` with each model's `tokenizer.json`, cached per message. At most `inline_bytes` of uncached text is tokenized before admission. Longer new messages are estimated by a learned ratio and tokenized in the background ([ADR-028](adr/ADR-028-input-token-counting.md)). Measured: 4 KB ≈ 2.7 ms, 512 KB ≈ 194 ms uncached, ≈ 0.2 ms cached |
| Prefix block hashing | 100 µs | Same block size and hash as Dynamo's KV router, so hashes are reusable downstream |
| WU estimate + admit | 20 µs | Lock-free per-reservation atomic bucket |
| Forward | none | Streaming proxy with back-pressure |

## 3. Cost estimation

The blog's central admission problem is that you must price a request before you know
its output length.

- **Prefill: exact.** Tokenisation gives input length. Messages seen before come from a
  per-message cache. A new message longer than the inline budget is estimated at the
  model's learned bytes-per-token ratio for this request only, and is exact on the next
  one ([ADR-028](adr/ADR-028-input-token-counting.md)). The gateway passes its count to
  the router in `x-pt-prompt-tokens`. The prefix hashes are checked
  against a gateway-local, approximate **prefix-cache index**: a Bloom filter per pool, fed
  from Dynamo KV events via NATS. That gives an expected `cached_prefill_tokens` value. A
  wrong guess only changes the estimate, and settlement fixes it.

  > **Implementation** ([ADR-030](adr/ADR-030-gateway-prefix-cache-index.md)): until
  > KV events are available, the index is the gateway's own history. It holds cumulative
  > message-prefix hashes per reservation, recorded when the engine accepts a request.
  > Expected cached tokens are the longest remembered prefix times a per-model hit rate
  > learned from the engine's reported cached tokens (starting at 0.5).
  > `GET /internal/v1/prefix-cache` shows the rates.
- **Decode: predicted.**
  `decode_est = min(max_tokens, P90_output(tenant, deployment, route_hint))`
  `P90_output` is a streaming quantile sketch (DDSketch) kept per deployment and refreshed
  from settlement. It falls back to the declared shape p95 on cold start.
- **KV-seconds:** `(input + decode_est/2) × decode_est × TPOT_target`.
- **Settlement:** on completion, or on disconnect, `Δ = actual_WU − est_WU` is applied to
  the bucket. The estimation error distribution per deployment is exported. If the
  estimator is biased by more than 10%, an alert fires.

## 4. Boundary policies

Each deployment configures what happens when its bucket is empty:

```yaml
boundary_policy:
  mode: burst            # reject | queue | burst | spillover
  burst:
    accrual: unused      # unused entitlement accrues as burst credit
    max_credit_seconds: 60     # at most 60 s of entitlement banked
    max_rate_multiple: 2.0     # never exceed 2× entitlement instantaneous
  queue:
    max_depth_wu_seconds: 5
    deadline_ms: 2000
  spillover:
    enabled: true
    to: payg              # billed at PAYG list price, class = spillover
  on_exhausted: reject    # after burst/queue/spillover are exhausted
```

| Mode | Behaviour | Class on the wire | Guarantee |
|------|-----------|-------------------|-----------|
| `reject` | HTTP 429, `Retry-After` computed from bucket refill, `x-pt-reason: entitlement_exhausted` | none | Predictable |
| `queue` | Held at the gateway in a deadline-aware queue. Rejected if the deadline would pass | provisioned (queue time counted in TTFT, excluded from SLA) | Latency not guaranteed for queued time |
| `burst` | Draws from banked credit up to the rate multiple | `burst` | Best-effort SLO. Served with priority above PAYG. **No extra charge** within the cap |
| `spillover` | Forwarded to the shared PAYG pool | `spillover` | No SLO. Billed at **PAYG list price** |

The policies chain: `burst → queue → spillover → reject`, in the order the customer
enables them.

## 5. Agentic workloads

The blog notes three agentic traits: bursty, long-context, and repetitive. Each gets a
specific response:

1. **Bursty → burst credit.** Unused entitlement accrues (bounded), so a fan-out of dozens
   of calls after idle time is absorbed without throttling. `burst_factor` in the shape
   declaration sizes the credit limit at sale time. The Planner accounts for correlated
   bursts across tenants with a statistical multiplexing factor ([06 §2](06-capacity-planning-and-reliability.md#2-sizing-formula)).
2. **Repetitive → session affinity plus a cache discount.** `x-pt-session-id` pins a
   session to a worker set (a consistent-hash hint for the router). Combined with KV-aware
   routing, the growing context is a cache hit, so it is charged at coefficient `b`, not `a`.
   Customers see this as a lower WU per call.
3. **Long-context → disaggregated pools.** Long-context reservations are placed on P/D
   disaggregated pools ([05 §5](05-isolation-and-scheduling.md#5-prefilldecode-disaggregation)), with KVBM offload keeping idle-session KV warm in CPU/NVMe between tool calls.
4. **Chain integrity → intra-tenant priority.** `x-pt-priority: continuation` marks
   mid-chain calls. When the bucket is tight, continuation calls draw burst credit first
   and new-session calls are queued or rejected first. A half-finished agent task is worse
   than a delayed new one. At launch, `continuation` is the only intra-tenant priority.
   Customer-defined priority levels (for example, paid vs free users) are not supported.

## 6. Distributed enforcement

A reservation's WU/s must be enforced across many gateway replicas, with no central store
on the hot path ([ADR-003](adr/ADR-003-lease-based-distributed-quota.md)):

- The Quota Coordinator (an active/standby pair per region, ADR-027) holds each reservation's region
  share. It grants each gateway a **lease**: a WU/s slice and a burst-credit slice, valid
  for 1 s and renewed every 250 ms.
- Slices are proportional to each gateway's recent demand for that reservation, with a
  small floor so a new request anywhere is admitted promptly.
- Gateways report consumption and debt on renewal. The coordinator rebalances slices.
  Over-admission is bounded by `Σ leases ≤ entitlement`, plus in-flight estimation error,
  which is settled within one lease period. That satisfies N6.
- **Coordinator unavailable:** gateways keep their last lease and decay it linearly to
  50% over 30 s. After that they hold at 50% of `entitlement / gateway_count`. This fails
  safe without a hard outage.
- Low-volume tenants (< 1 gateway's worth of traffic) are routed by consistent hashing to
  2 "home" gateways, so leases don't fragment.

> **Implementation** (`crates/pt-quota`, `crates/pt-gateway/src/quota.rs`, [ADR-012](adr/ADR-012-single-instance-quota-coordinator.md)):
> the coordinator is a single instance per region with soft state, not Raft, and uses
> HTTP/JSON. Demand is attempted WU/s, and each gateway sends it with its snapshot's
> entitlement. A gateway is granted `min(fair target, entitlement − other unexpired
> grants)`, so grants never oversell, even mid-rebalance. Burst credit accrues from the
> local lease instead of separate burst slices. Gateways apply leases by resizing their
> limiters in place. Home-gateway routing isn't built yet.
>
> **High availability** ([ADR-027](adr/ADR-027-quota-coordinator-standby.md)). Two
> replicas run active/standby on a Kubernetes Lease instead of Raft. Only the leader
> grants, and a standby answers 503 `not_leader`, so gateways try the next replica in
> the same renewal. A dead leader stops granting 3 s after its last lease renewal and is
> replaced 5 s after it. By then all its grants have expired, so the new leader starts
> cold. A leader that shuts down releases the lease and is replaced at once. The new
> leader then *warms up* for one grant hold: it grants each gateway no more than the
> unexpired lease the gateway reports holding, so a rolling update leaves no gateway in
> fallback and never oversells.

## Blog problems addressed
P7, P8, P11, P12. See [traceability](01-requirements-and-traceability.md#2-traceability-matrix).
