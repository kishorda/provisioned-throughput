# 02 · Capacity Unit & Cost Model

> Decision record: [ADR-001](adr/ADR-001-capacity-unit.md)

## 1. Why not tokens per minute

Raw TPM puts a 50-token classification request and a 200K-token RAG request on the same
scale. As the blog notes, that overcharges light workloads, undercharges heavy ones, and
pushes customers to oversize. We keep TPM as a **translation** for customers, but we do
not use it as the **unit of entitlement**.

## 2. Work Unit (WU)

Every request costs WU, computed from a `PerformanceProfile` specific to
**(model, GPU class, engine+version, parallelism config)**:

```
WU(request) = a · uncached_prefill_tokens
            + b · cached_prefill_tokens            # prefix-cache / KVBM hit, b ≪ a
            + c · decode_tokens · m_decode         # m_decode: speculative / structured-output modifier
            + d · KV_token_seconds                 # Σ over lifetime of (resident KV tokens × seconds)
```

- **a, c**: fitted from benchmarks. Prefill is compute-bound (a scales with FLOPs per token).
  Decode is memory-bandwidth-bound (c reflects the batch-amortised cost per step).
- **d**: charges KV-cache residency. This term makes a 100K-token prompt cost more than
  ten 10K-token prompts, because it holds a large KV footprint for its whole decode lifetime
  and blocks concurrency. KV-token-seconds is computed after the fact by the worker, from
  actual residency including time spent offloaded (at a discount per tier).
- **m_decode**: multiplier table. For example, speculative decoding at the measured
  acceptance rate is < 1, and grammar-constrained output is > 1 on some backends.
- Coefficients are **normalised** so the reference workload on the reference pool is
  1 WU per "reference token". This keeps numbers intuitive when we explain them.

The WU formula is computed twice: **estimated** at admission ([04 §3](04-request-lifecycle-and-admission.md#3-cost-estimation))
and **actual** at completion. Billing and entitlement settlement always use actual WU.

## 3. Capacity Unit (CU) and SLO tiers

**1 CU = W WU/s of sustained entitlement, delivered at SLO tier T.**

A CU is not tied to a GPU. The Capacity Planner converts CUs into replicas using each
pool's measured *WU/s-per-replica-at-SLO* ([06 §2](06-capacity-planning-and-reliability.md#2-sizing-formula)).

| Tier | p95 TTFT | p95 TPOT | Short-output TPOT (≤ 64 tok) | Typical use |
|------|----------|----------|------------------------------|-------------|
| **Interactive** | ≤ 800 ms (≤ 8K in), +100 ms / additional 8K | ≤ 40 ms | ≤ 30 ms | Chat, copilots |
| **Agentic** | ≤ 500 ms on cached-prefix requests; ≤ 1.5 s otherwise | ≤ 30 ms | ≤ 20 ms | Tool-calling agents |
| **Standard** | ≤ 3 s | ≤ 80 ms | none | Summarisation, RAG backends |

Latency targets are defined at the gateway ([09 §4](09-metering-observability-and-slas.md#4-sla-definition)).
A tighter tier forces smaller batches, so each replica delivers fewer WU/s. The price
per CU therefore differs by tier, but the CU definition does not change.

| Tier | CU price multiplier |
|------|---------------------|
| Standard | 1.0× (base price) |
| Interactive | 1.25× |
| Agentic | 1.5× |

Surcharges add to the tier multiplier, as multiples of the base price
([11 §4](11-roadmap-risks-open-questions.md#4-product-decisions)): **strict-dedicated +0.3×**,
and **Multi-region +0.2×**, which pays for failover headroom reserved in the paired region.
For example, an Agentic Multi-region CU costs 1.7× base.

Once calibration has run, check the multipliers against each tier's WU/s-per-replica
ratio, so every tier covers its GPU cost.

## 4. Workload shape declaration

A reservation declares a shape. This is the blog's "for workloads shaped roughly like
this" clause, made explicit:

```yaml
shape:
  input_tokens:   { p50: 2000, p95: 12000, max: 64000 }
  output_tokens:  { p50: 150,  p95: 800 }
  cache_hit_ratio: 0.6           # fraction of prefill tokens expected from prefix cache
  burst_factor:   3.0            # peak-1s / mean-60s
  context_ceiling: 128000
```

- The shape sizes the reservation and routes it to the right pool type (for example,
  long-context reservations go to disaggregated pools).
- **The SLO covers in-shape traffic.** Out-of-shape requests (input > declared max, or
  context > ceiling) are still served and charged at actual WU. They are excluded from SLA
  attainment and flagged in telemetry, so customers can see drift and resize. There is
  **no grace margin**: a request even slightly over a declared maximum is out-of-shape.
- Drift detection: telemetry compares the observed shape with the declared shape weekly
  and recommends a new CU count or tier.

## 5. Sizing / Quote API

Customers do not have to learn WU to buy.

- `POST /v1/quotes` accepts a shape, **or** a sample trace (JSONL of request lengths and
  timestamps), **or** a reference to the tenant's last 30 days of PAYG logs.
- It returns the required CUs per tier, the equivalent "≈ input TPM / output TPM for this
  shape", the expected p95 latencies, and a feasibility check per region (from the Capacity
  Planner).
- Replay mode runs the trace through the calibrated cost model, and optionally through a
  shadow benchmark on the target pool for large deals.

> **Implementation** (`crates/pt-control-plane/src/quote.rs`, `POST /v1/quotes`). There are
> three sources: `shape` plus `requests_per_minute`, a `trace`, or `from_reservation`. The
> last uses an existing reservation's recent usage from telemetry, rejected requests
> included, which makes the quote a resize recommendation. Each request is priced with the
> region pool's profile and the tier's TPOT target, which sets the KV term.
>
> ```text
> sustained = busiest hour's WU/s      peak = busiest 10 s's WU/s   (shape: sustained × burst_factor)
> recommended CUs = ceil(sustained ÷ (WU/s per CU × 0.8))           peak CUs = ceil(peak ÷ WU/s per CU)
> ```
>
> For each region and tier, a quote returns:
> - demand, and the recommended and peak CUs;
> - "1 CU ≈ N requests/min, X input TPM, Y output TPM" for this workload;
> - the monthly price and the SLO targets;
> - feasibility (available CUs and whether the region's pools serve the context length);
> - a burst or spillover policy when peaks exceed the recommendation.
>
> For traces and history, it also returns an observed shape and the cheapest feasible
> option, or the resize action. The shadow benchmark isn't built. Profiles come from
> `[[profiles]]` in the control-plane config, standing in for the Profile Registry.

## 6. Calibration Service

- For each (model, GPU class, engine version, parallelism) it runs a benchmark matrix with
  Dynamo's perf tooling (AIPerf / genai-perf). The matrix sweeps input length, output
  length, cache-hit ratio, and concurrency.
- It fits a, b, c, d and m_decode with non-negative least squares on measured GPU-seconds.
  It then computes **WU/s-per-replica** at the max concurrency that still meets each tier's
  SLO.
- It publishes a versioned `PerformanceProfile` CR ([08 §2](08-kubernetes-and-dynamo-integration.md#2-custom-resources)).
  **A pool is not sellable without a profile.**
- Continuous validation: a nightly job compares production actual-WU vs GPU-seconds per
  pool. If drift exceeds 5%, it pages perf engineering and blocks new sales on that pool.

## 7. Hardware efficiency gains (the blog's dilemma)

- The **CU definition stays fixed**. A new GPU generation or a faster kernel raises
  WU/s-per-replica, so the provider needs fewer GPUs per CU.
- **Re-rating policy:** re-rating is infrequent and has no fixed calendar cadence. When
  it happens, the CU price is re-rated downwards so that **50% of realised efficiency
  gains** go to customers. Existing reservations get the lower price at renewal, which is at
  most 6 months away ([§8](#8-reservation-terms)). Customers are never stranded on old hardware, because
  entitlement does not name hardware.
- Customers who need a specific GPU (compliance, or determinism of numerics) buy a
  **hardware-pinned** SKU at a premium ([10 §2](10-hardware-and-model-lifecycle.md#2-hardware-pinned-sku)).

## 8. Reservation terms

| Term | Rule |
|------|------|
| Minimum size | 1 CU |
| Term length | 1, 3, or 6 months |
| Increase mid-term | Allowed and effective as soon as the Capacity Planner confirms feasibility (target < 60 s, N3). The added CUs are billed for the remaining term and end with the original term. |
| Decrease mid-term | Not allowed. CUs can be reduced only at renewal. |
| Renewal | At the then-current CU price, including any re-rating (§7) |

Short terms keep planner commitments short and let re-rating reach customers quickly.
They also put more weight on the Capacity Planner's lead-time forecasts, because demand
can leave the platform every 1–6 months.

## Blog problems addressed
P1, P2, P3, P4, P5, P6, P8. See [traceability](01-requirements-and-traceability.md#2-traceability-matrix).
