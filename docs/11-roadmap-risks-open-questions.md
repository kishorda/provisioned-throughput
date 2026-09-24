# 11 · Roadmap, Risks & Product Decisions

## 1. Phased delivery

| Phase | Scope | Exit criteria |
|-------|-------|---------------|
| **P0 · Foundations** (≈ 1 quarter) | Calibration Service + profiles for 2 models × 2 GPU classes; WU cost model; PT Gateway with debt bucket and `reject`/`spillover`; Quota Coordinator; shared-provisioned pools on Dynamo (aggregated + chunked prefill); router priority classes; metering to ClickHouse; basic customer dashboard; **one region** | Internal dogfood tenants hold Interactive SLO at 100% entitlement with a PAYG flood |
| **P1 · Isolation & agents** | Router WFQ by WU + pull-based dispatch; P/D disaggregation pools; per-tenant KV budgets + KVBM offload; `burst` and `queue` policies; session affinity; Quote API with trace replay; dedicated pools with PAYG backfill plus the strict-dedicated (no backfill) option; mid-term CU increases; capacity-aware drain controller | Interference suite ([05 §7](05-isolation-and-scheduling.md#7-validation-interference-test-suite)) passes; first external GA customers |
| **P2 · Global** | Multi-region SKUs with failover headroom; Entitlement Distributor share rebalancing; MILP rebalancer; hardware migration flow; CU re-rating; hardware-pinned SKU; SLA credits automation | Region-failover game day passes; first hardware-generation migration done with no SLO breach |

## 2. Team topology (suggested)

- **Gateway & Admission** (Rust): PT Gateway, Quota Coordinator, cost estimation.
- **Scheduling** (Rust/C++/Python): Dynamo router extensions, engine adapters, upstream work with NVIDIA.
- **Capacity & Fleet** (Rust): planner, controllers, drain, Calibration Service.
- **Product Platform**: Reservation Service, Quote API, portal, billing, telemetry API.
- **Perf Engineering**: benchmarks, profiles, drift monitoring.

## 3. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Dynamo APIs change quickly (router, operator CRDs) | Fork rebase cost, delays | Keep extensions behind traits; contribute an upstream plugin interface; pin minor versions; dedicated upstream liaison |
| Engine KV-budget patch not accepted upstream | Maintenance burden across 3 backends | Start with one backend (TRT-LLM or vLLM) for P1; router-side KV accounting as fallback |
| Cost model drift (new kernels, new traffic mixes) | Over- or under-selling | Nightly drift checks; sales block on > 5% drift; recalibration pipeline |
| Customers find CU abstract | Sales friction | Quote API with a TPM translation; trace replay; shape-drift recommendations |
| Correlated agentic bursts exceed statistical-multiplexing assumptions | SLO misses | Conservative `z`; burst accounting by class; per-pool burst caps; monitor and tune |
| Hot headroom unused by PAYG in some regions | Margin erosion | Shift to warm spares where PAYG demand is thin; price Multi-region SKU accordingly |
| Tokenisation at the gateway for huge prompts | Admission latency | Parallel tokenisation; accept client-supplied token counts for trusted tenants with post-hoc verification |

## 4. Product decisions

The PM answered the open questions on 2026-09-23.

| # | Question | Decision | Where it's applied |
|---|----------|----------|--------------------|
| 1 | How are burst and spillover priced? | **Burst credit is free** within the configured cap. **Spillover is billed at PAYG list price.** | [04 §4](04-request-lifecycle-and-admission.md#4-boundary-policies) |
| 2 | How often is the CU re-rated, and what share of gains passes through? | **Re-rating is infrequent**, with no fixed calendar cadence. **50% of realised efficiency gains** are passed to customers. | [02 §7](02-capacity-unit-and-cost-model.md#7-hardware-efficiency-gains-the-blogs-dilemma) |
| 3 | What is the SLA credit schedule? Is there a grace margin for out-of-shape traffic? | Credits are 10% / 20% / 30% / 50% of the monthly reservation fee at the 99.8% / 99.7% / 99.6% / 99.5% thresholds. **No grace margin:** anything beyond the declared shape is out-of-shape. | [09 §4](09-metering-observability-and-slas.md#4-sla-definition), [02 §4](02-capacity-unit-and-cost-model.md#4-workload-shape-declaration) |
| 4 | What are the minimum reservation size and term lengths? | **Minimum 1 CU.** Terms are **1, 3, or 6 months**. | [02 §8](02-capacity-unit-and-cost-model.md#8-reservation-terms) |
| 5 | Can customers resize mid-term? | **Increases only**, effective immediately (subject to feasibility) and billed for the remaining term. **No decreases mid-term.** A customer can reduce CUs only at renewal. | [02 §8](02-capacity-unit-and-cost-model.md#8-reservation-terms) |
| 6 | Is strict-dedicated (no PAYG backfill) offered at launch? | **Yes**, from GA (phase P1), with a **surcharge of 0.3× the base CU price** on top of the tier price (Standard 1.3×, Interactive 1.55×, Agentic 1.8×). | [05 §6](05-isolation-and-scheduling.md#6-pool-tiers-hybrid-isolation) |
| 7 | Can customers set their own traffic priority beyond `continuation`? | **Not at launch.** `continuation` is the only intra-tenant priority. | [04 §5](04-request-lifecycle-and-admission.md#5-agentic-workloads) |
| 8 | What are the per-tier CU price multipliers? | **Standard 1.0×** (base), **Interactive 1.25×**, **Agentic 1.5×**. | [02 §3](02-capacity-unit-and-cost-model.md#3-capacity-unit-cu-and-slo-tiers) |

**Open:** how is the Multi-region SKU priced? Its failover headroom is reserved in the
paired region from 2026-09-24 ([ADR-014](adr/ADR-014-automatic-region-failover.md)), so a
Multi-region reservation holds up to twice its CUs. Today it's priced like a Regional one.
