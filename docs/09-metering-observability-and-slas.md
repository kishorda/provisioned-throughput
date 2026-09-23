# 09 · Metering, Observability & SLAs

> Decision record: [ADR-010](adr/ADR-010-sla-at-gateway.md)

## 1. Usage record

The gateway emits one record per request after settlement. Workers contribute actual
counts via NATS.

```rust
pub struct UsageRecord {
    pub request_id: Uuid,              // idempotency key
    pub tenant: TenantId,
    pub reservation: ReservationId,
    pub deployment: DeploymentId,
    pub region: Region,
    pub pool: PoolId,
    pub class: TrafficClass,           // Provisioned | Burst | Spillover | Payg
    pub session_id: Option<String>,
    pub tokens: TokenBreakdown,        // uncached_prefill, cached_prefill, decode
    pub kv_token_seconds: f64,
    pub wu_estimated: f64,
    pub wu_actual: f64,
    pub timings: Timings,              // gateway_in, admitted, first_byte_out, last_byte_out, queue_ms
    pub in_shape: bool,
    pub outcome: Outcome,              // Ok | Rejected(Reason) | ClientCancelled | Error(Code)
    pub profile_version: String,
}
```

**Pipeline:** gateway → Redpanda (acks=all, idempotent producer) → ClickHouse
(`ReplacingMergeTree` on `request_id`) → Rust billing aggregator that writes hourly
aggregates → global Usage Aggregator. Exactly-once billing comes from idempotent keys, not
distributed transactions (N5).

## 2. Internal observability

- **Traces:** OpenTelemetry across gateway → router → prefill → decode, sampled with tail
  sampling (all errors, all SLO-violating requests, 1% baseline).
- **Metrics:** Prometheus + Thanos for fleet health (DCGM, engine batch occupancy, KV
  utilisation, NIXL throughput, router queue depth by class). ClickHouse holds
  high-cardinality per-tenant series.
- **Key SLIs per pool:** WU/s delivered vs capacity, estimation bias, preemptions by
  class, offload/restore counts, P:D ratio, headroom remaining vs `k(p)`.

## 3. Customer-facing observability

The blog says this is a product feature, not an internal nicety. Exposed per deployment
via the portal, a Prometheus-compatible metrics API, and an OpenTelemetry export:

| Metric | Granularity | Notes |
|--------|-------------|-------|
| Utilisation vs entitlement (WU/s and %) | 10 s | Includes burst credit balance |
| TTFT, TPOT, short-output TPOT | p50 / p95 / p99, 1 min | Gateway-measured, in-shape vs out-of-shape split |
| Requests by class | 1 min | provisioned / burst / spillover |
| Throttles and rejections | 1 min | Reason codes: `entitlement_exhausted`, `queue_deadline`, `out_of_shape_context`, `region_failover` |
| Cache hit rate | 1 min | Fraction of prefill tokens served from cache, plus WU saved |
| Shape drift | daily | Observed vs declared distribution; resize recommendation |
| Agent session view | per session | Calls, cumulative latency, cache reuse, throttles within the session |
| SLA attainment | monthly, with daily burn-down | See §4 |

Every rejection response carries `x-pt-reason` and `x-pt-entitlement-remaining`, so
customer-side monitoring agrees with ours.

## 4. SLA definition

The blog notes that latency differs by vantage point. We publish exactly one:

- **Measurement point:** the regional PT Gateway. TTFT = request fully received at gateway
  → first response byte sent by gateway. TPOT = (last byte − first byte) / (output tokens − 1).
  This includes admission, routing, and engine queueing. It excludes the client's network.
- **Window and percentile:** p95 per 5-minute window, per deployment. Windows with fewer
  than 100 eligible requests are merged with adjacent windows.
- **Eligible requests:** in-shape, class `provisioned`, successfully completed or cancelled
  after first token.
- **Exclusions:** traffic above entitlement (burst, queued, spillover), out-of-shape
  requests, client-caused errors, the declared failover window for Multi-region SKUs, and
  customer-initiated changes (for example, a resize in progress).
- **Attainment:** percentage of eligible windows meeting both TTFT and TPOT targets. The
  commitment is **99.8%** per month (N2).
- **Service credits** are a percentage of that month's fee for the affected reservation:

  | Monthly attainment | Credit |
  |--------------------|--------|
  | ≥ 99.8% | none |
  | < 99.8% and ≥ 99.7% | 10% |
  | < 99.7% and ≥ 99.6% | 20% |
  | < 99.6% and ≥ 99.5% | 30% |
  | < 99.5% | 50% |

- **No grace margin for shape:** out-of-shape requests are excluded exactly as declared
  ([02 §4](02-capacity-unit-and-cost-model.md#4-workload-shape-declaration)).
- The SLA report is generated from the same ClickHouse data that customers query, so
  customers can reproduce it.

## Blog problems addressed
P8, P18, P19. See [traceability](01-requirements-and-traceability.md#2-traceability-matrix).
