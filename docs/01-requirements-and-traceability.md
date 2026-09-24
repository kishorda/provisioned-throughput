# 01 · Requirements & Traceability

## 1. Problem statement

The PM blog makes one core argument. "Reserve X tokens per minute" does not describe GPU
work, and it does not guarantee a customer experience. A PT product has to (a) define a
unit that tracks real cost, (b) pair throughput with latency, (c) enforce the unit in real
time without knowing output length in advance, (d) isolate tenants who share batches,
(e) carry hidden headroom for failures and maintenance, and (f) make all of it observable
to the customer.

## 2. Traceability matrix

Every problem raised in the blog maps to at least one requirement and one design section.

| # | Blog problem | Requirement | Design response | Section |
|---|--------------|-------------|-----------------|---------|
| P1 | A token is not a fixed unit of work (prefill vs decode) | R1: The unit must weight prefill and decode separately | WU cost model with per-phase coefficients | [02 §2](02-capacity-unit-and-cost-model.md#2-work-unit-wu) |
| P2 | Context length drives KV memory, and KV is often the real bottleneck | R2: The unit must charge for KV residency | `KV_token_seconds` term; per-tenant KV budgets | [02 §2](02-capacity-unit-and-cost-model.md#2-work-unit-wu), [05 §4](05-isolation-and-scheduling.md#4-level-3--engine) |
| P3 | Prompt caching, speculative decoding, and structured output change the real cost | R3: The unit must credit cache hits and adjust for decode modifiers | Separate `cached_prefill` coefficient; modifier table | [02 §2](02-capacity-unit-and-cost-model.md#2-work-unit-wu) |
| P4 | Unit options (RPM, TPM, weighted tokens, abstract) each have trade-offs | R4: Honest unit plus a customer-understandable translation | Abstract CU plus Sizing/Quote API with a TPM translation | [02 §4–5](02-capacity-unit-and-cost-model.md#4-workload-shape-declaration), [ADR-001](adr/ADR-001-capacity-unit.md) |
| P5 | Throughput is meaningless without latency (TTFT/TPOT p95) | R5: Every CU is sold at a named SLO tier | SLO tiers are part of the CU definition | [02 §3](02-capacity-unit-and-cost-model.md#3-capacity-unit-cu-and-slo-tiers) |
| P6 | Hardware generations change throughput 2–3×; stranded customers; migration ratios | R6: Hardware-agnostic entitlement; per-hardware conversion; migration path | Per-pool PerformanceProfile; re-rating policy; hardware-pinned SKU | [02 §6–7](02-capacity-unit-and-cost-model.md#6-calibration-service), [10](10-hardware-and-model-lifecycle.md) |
| P7 | Agentic workloads are bursty, long-context, and repetitive | R7: Burst allowance, session cache affinity, intra-tenant priority | Burst bucket; `session_id` affinity; KV-aware routing | [04 §5](04-request-lifecycle-and-admission.md#5-agentic-workloads) |
| P8 | Agentic end-to-end latency compounds; TPOT on short outputs matters | R8: SLOs cover short-output TPOT; session-level telemetry | Tier definitions include short-output TPOT; per-session views | [02 §3](02-capacity-unit-and-cost-model.md#3-capacity-unit-cu-and-slo-tiers), [09 §3](09-metering-observability-and-slas.md#3-customer-facing-observability) |
| P9 | Reliability: GPU errors, node loss, drains, upgrades, hardware refresh | R9: N+k per failure domain; warm spares; fast load; capacity-aware drain | Planner headroom; spare tiers; drain controller; region failover from heartbeats with reserved headroom | [06](06-capacity-planning-and-reliability.md), [07 §4](07-multi-region.md#4-region-failure-sequence-multi-region-sku) |
| P10 | Large weights make cold start slow | R10: Replacement replica serving in under 2 minutes (warm tier) | NVMe weight cache, P2P distribution, streaming loader | [06 §4](06-capacity-planning-and-reliability.md#4-fast-model-loading) |
| P11 | Admission must estimate cost before output length is known | R11: Accurate estimate plus settlement | Exact prefill from tokenisation; learned decode estimate; debt bucket | [04 §3](04-request-lifecycle-and-admission.md#3-cost-estimation), [ADR-002](adr/ADR-002-debt-based-wu-bucket.md) |
| P12 | Boundary behaviour: reject, queue, spillover, burst | R12: Customer-configurable boundary policy | `BoundaryPolicy` per deployment, set through the control-plane API | [04 §4](04-request-lifecycle-and-admission.md#4-boundary-policies), [12 §3](12-control-plane-api.md#3-rules) |
| P13 | Noisy neighbour: long prompts stall decode | R13: Prefill must not stall other tenants' decode | Chunked prefill; P/D disaggregation | [05 §5](05-isolation-and-scheduling.md#5-prefilldecode-disaggregation) |
| P14 | Noisy neighbour: KV exhaustion forces preemption and recompute | R14: Per-tenant KV budgets; offload instead of recompute | Engine KV budget; KVBM offload | [05 §4](05-isolation-and-scheduling.md#4-level-3--engine) |
| P15 | Noisy neighbour: one tenant's burst raises everyone's TTFT | R15: Cost-weighted fair queuing | Router WFQ by WU; pull-based dispatch | [05 §3](05-isolation-and-scheduling.md#3-level-2--router), [13 §2](13-tenant-aware-routing.md#2-algorithms) |
| P16 | Isolation strategies: dedicated, shared, or hybrid | R16: Tiered pool model | Shared-provisioned, dedicated with backfill, PAYG, spare | [05 §6](05-isolation-and-scheduling.md#6-pool-tiers-hybrid-isolation) |
| P17 | Provisioned traffic must preempt on-demand traffic | R17: Strict priority classes | `provisioned > burst > spillover/PAYG`; failover preempts PAYG on hot spares | [05 §2](05-isolation-and-scheduling.md#2-priority-classes), [13 §2](13-tenant-aware-routing.md#2-algorithms) |
| P18 | Observability is a customer-facing product feature | R18: Per-deployment utilisation, latency, throttles, cache hits | Customer telemetry API and dashboards | [09 §3](09-metering-observability-and-slas.md#3-customer-facing-observability) |
| P19 | Latency depends on the vantage point; the SLA needs a precise definition | R19: One published measurement point, percentile, window, and exclusions | Gateway-measured SLA spec | [09 §4](09-metering-observability-and-slas.md#4-sla-definition), [ADR-010](adr/ADR-010-sla-at-gateway.md) |
| P20 | Build product and engine together | R20: Product constructs (CU, shape, tier) are first-class in engine config | CRDs carry the product model end to end | [08 §2](08-kubernetes-and-dynamo-integration.md#2-custom-resources) |

## 3. Functional requirements (summary)

- **FR1** Customers can get a quote (CUs needed) from a declared shape or a replayed trace.
- **FR2** Customers can buy reservations per model, tier, and region (minimum 1 CU; 1-,
  3-, or 6-month terms), increase them mid-term, and renew them. Decreases take effect only
  at renewal. The platform rejects sales and increases it cannot deliver.
- **FR3** Customers can create deployments (endpoints) bound to a reservation, with a boundary policy.
- **FR4** The data plane serves an OpenAI-compatible API and enforces entitlement, tier, and policy.
- **FR5** Every request produces a usage record: WU, token breakdown, class, and latency.
- **FR6** Customers see near-real-time per-deployment telemetry and monthly SLA attainment.
- **FR7** Operators can add hardware pools, roll models, and drain nodes without SLO breach.

## 4. Non-functional requirements

| ID | Requirement | Target |
|----|-------------|--------|
| N1 | Gateway admission overhead (tokenise + estimate + admit) | p99 < 2 ms for ≤ 32K-token prompts |
| N2 | SLO attainment per reservation (in-shape, within entitlement) | ≥ 99.8% of 5-minute windows per month (SLA credits start below this, see [09 §4](09-metering-observability-and-slas.md#4-sla-definition)) |
| N3 | Entitlement change propagation (resize or new reservation) | < 60 s to all gateways in region |
| N4 | Regional independence | Region serves on last-known-good entitlements for ≥ 24 h without the global plane |
| N5 | Metering durability | < 0.001% usage record loss; exactly-once billing aggregation |
| N6 | Over-admission bound | Tenant sustained consumption ≤ 102% of entitlement over any 60 s window (excluding explicit burst) |
| N7 | Failure absorption | Single node or NVL domain loss absorbed with no SLO breach for in-shape traffic |
| N8 | Spare-to-serving time | Hot: < 1 s (preempt PAYG); warm: < 2 min; cold: < 15 min |
| N9 | Availability of the regional data plane | 99.95% monthly |
| N10 | Data residency | Prompts and completions stay in the serving region unless the tenant opts in |

## 5. Out of scope for v1

- Fine-tuned or customer-uploaded model weights (LoRA hosting is a P2 roadmap item).
- Training or batch-inference reservations.
- On-prem or sovereign-cloud deployments.
