# Provisioned Throughput (PT) for AI Inference: Architecture

**Status:** Draft v0.1 · **Date:** 2026-09-23 · **Owner:** Principal Architect, Inference Platform
**Input:** PM blog: [Provisioned Throughput for AI Inference: why "just reserve some capacity" is harder than it sounds](https://kishoraher.wordpress.com/2026/09/23/provisioned-throughput-for-ai-inference-why-just-reserve-some-capacity-is-harder-than-it-sounds/)

## Executive summary

Provisioned Throughput is a **contract**, not a pricing SKU. The customer buys a guaranteed
amount of inference *work* per second, at a named latency tier, for a declared workload
shape, in one or more regions. The platform has to price that work accurately, admit
traffic against it in real time, isolate it from other tenants inside shared GPU batches,
keep enough headroom to survive failures and maintenance, and prove all of this to the
customer with per-deployment telemetry.

The design rests on six decisions:

1. **Sell an abstract Capacity Unit (CU), measured in Work Units (WU).** A WU is a
   calibrated cost: uncached prefill, cached prefill, decode, and KV-cache residency
   (KV-token-seconds) each get their own weight, per model × GPU × engine version. One CU
   is a fixed WU/s delivered at a named SLO tier. It does not depend on the hardware. Each
   pool's performance profile converts CUs into replicas. ([02](02-capacity-unit-and-cost-model.md), [ADR-001](adr/ADR-001-capacity-unit.md))
2. **Admit on an estimate, then settle on actuals.** The gateway tokenises the request,
   so the prefill cost is exact. It estimates decode length, admits against a debt-based
   WU token bucket, and settles the bucket with actual usage when the request completes.
   Boundary behaviour is configurable per deployment: reject, queue, burst, or spill over
   to PAYG. ([04](04-request-lifecycle-and-admission.md), [ADR-002](adr/ADR-002-debt-based-wu-bucket.md), [ADR-003](adr/ADR-003-lease-based-distributed-quota.md))
3. **Treat isolation as a scheduling problem first.** Fairness works at three levels: the
   gateway enforces rate, the router runs WFQ by WU with priority classes, and the engine
   applies per-tenant KV budgets and preemption. By default, prefill and decode are
   disaggregated for long-context models. ([05](05-isolation-and-scheduling.md), [ADR-004](adr/ADR-004-three-level-fairness.md), [ADR-005](adr/ADR-005-disaggregation-default.md))
4. **Headroom is never idle.** N+k spares and hot spares serve preemptible PAYG traffic.
   Provisioned traffic evicts that traffic within one second. ([06](06-capacity-planning-and-reliability.md), [ADR-006](adr/ADR-006-headroom-backfill.md))
5. **Regions are statically stable.** A global control plane sells and plans capacity.
   Regional data planes keep serving on the last-known-good entitlements if the global
   plane goes down. ([07](07-multi-region.md), [ADR-007](adr/ADR-007-regional-static-stability.md))
6. **Use NVIDIA Dynamo on Kubernetes as the serving substrate, and Rust for everything on the hot path.**
   Dynamo provides the KV-aware router, disaggregated serving, KVBM, NIXL, and Planner. We
   add tenant-aware routing, the WU gateway, and kube-rs operators. ([08](08-kubernetes-and-dynamo-integration.md), [ADR-008](adr/ADR-008-dynamo-substrate.md), [ADR-009](adr/ADR-009-rust.md))

## Reading order

| # | Document | Audience |
|---|----------|----------|
| 01 | [Requirements & traceability](01-requirements-and-traceability.md) | Everyone. Maps every blog problem to a requirement and a design section |
| 02 | [Capacity Unit & cost model](02-capacity-unit-and-cost-model.md) | PM, pricing, perf engineering |
| 03 | [System architecture](03-system-architecture.md) | Engineering |
| 04 | [Request lifecycle & admission](04-request-lifecycle-and-admission.md) | Gateway / router teams |
| 05 | [Isolation & scheduling](05-isolation-and-scheduling.md) | Router / engine teams |
| 06 | [Capacity planning & reliability](06-capacity-planning-and-reliability.md) | Capacity, SRE |
| 07 | [Multi-region](07-multi-region.md) | SRE, PM |
| 08 | [Kubernetes & Dynamo integration](08-kubernetes-and-dynamo-integration.md) | Platform team |
| 09 | [Metering, observability & SLAs](09-metering-observability-and-slas.md) | PM, billing, SRE |
| 10 | [Hardware & model lifecycle](10-hardware-and-model-lifecycle.md) | Capacity, PM |
| 11 | [Roadmap, risks, product decisions](11-roadmap-risks-open-questions.md) | Leadership, PM |
| 12 | [Control-plane API](12-control-plane-api.md) | PM, API and portal teams |
| 13 | [Tenant-aware routing](13-tenant-aware-routing.md) | Router, platform, and Dynamo integration teams |

## Architecture decision records

| ADR | Decision |
|-----|----------|
| [001](adr/ADR-001-capacity-unit.md) | Sell an abstract Capacity Unit backed by a calibrated Work Unit cost model |
| [002](adr/ADR-002-debt-based-wu-bucket.md) | Debt-based WU token bucket with settlement on actuals |
| [003](adr/ADR-003-lease-based-distributed-quota.md) | Lease-based distributed quota; no central store on the hot path |
| [004](adr/ADR-004-three-level-fairness.md) | Three-level fairness: gateway, router, engine |
| [005](adr/ADR-005-disaggregation-default.md) | Disaggregated prefill/decode by default for long-context models |
| [006](adr/ADR-006-headroom-backfill.md) | Backfill reserved headroom with preemptible PAYG |
| [007](adr/ADR-007-regional-static-stability.md) | Regional data planes are statically stable |
| [008](adr/ADR-008-dynamo-substrate.md) | NVIDIA Dynamo as the serving substrate |
| [009](adr/ADR-009-rust.md) | Rust for the gateway, router extensions, and controllers |
| [010](adr/ADR-010-sla-at-gateway.md) | SLA latency measured at the regional gateway |
| [011](adr/ADR-011-single-pt-resource.md) | One customer-facing Provisioned Throughput resource |
| [012](adr/ADR-012-single-instance-quota-coordinator.md) | Single-instance Quota Coordinator with soft state (amends 003) |
| [013](adr/ADR-013-tenant-scheduling-tier.md) | Tenant scheduling as a tier in front of Dynamo (refines 004, 008) |
| [014](adr/ADR-014-automatic-region-failover.md) | Automatic region failover from gateway heartbeats (refines 007) |
| [015](adr/ADR-015-failover-payg-preemption.md) | Fence, then preempt, PAYG on hot spares during failover (refines 006, 013) |
| [016](adr/ADR-016-warm-spare-loading.md) | Load warm spares from snapshot failover demand (refines 006, 014) |
| [017](adr/ADR-017-sql-control-plane-store.md) | A Postgres-protocol store for the control plane (CockroachDB in production) |

## Glossary

| Term | Meaning |
|------|---------|
| **WU** (Work Unit) | Normalised cost of one request, from the calibrated cost model |
| **CU** (Capacity Unit) | The sellable unit: a fixed WU/s at an SLO tier |
| **Reservation** | A contract for N CUs of model M at tier T, with a shape declaration, regions, and a term |
| **Deployment** | Customer-facing endpoint bound to one reservation. It carries boundary policy and keys |
| **Pool** | A set of Dynamo serving replicas for one model on one GPU class in one cluster |
| **Shape** | The declared workload profile: input/output length distributions, cache-hit rate, burst factor, context ceiling |
| **TTFT / TPOT** | Time to first token / time per output token |
| **PAYG** | Pay-as-you-go (on-demand) traffic. Lowest priority and preemptible |
| **KVBM** | Dynamo KV Block Manager: tiered KV cache across GPU, CPU, and SSD |
| **NIXL** | NVIDIA Inference Xfer Library: KV transfer between prefill and decode workers |
