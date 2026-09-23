# 11 · Roadmap, Risks & Open Questions

## 1. Phased delivery

| Phase | Scope | Exit criteria |
|-------|-------|---------------|
| **P0 · Foundations** (≈ 1 quarter) | Calibration Service + profiles for 2 models × 2 GPU classes; WU cost model; PT Gateway with debt bucket and `reject`/`spillover`; Quota Coordinator; shared-provisioned pools on Dynamo (aggregated + chunked prefill); router priority classes; metering to ClickHouse; basic customer dashboard; **one region** | Internal dogfood tenants hold Interactive SLO at 100% entitlement with a PAYG flood |
| **P1 · Isolation & agents** | Router WFQ by WU + pull-based dispatch; P/D disaggregation pools; per-tenant KV budgets + KVBM offload; `burst` and `queue` policies; session affinity; Quote API with trace replay; dedicated pools with PAYG backfill; capacity-aware drain controller | Interference suite ([05 §7](05-isolation-and-scheduling.md#7-validation-interference-test-suite)) passes; first external GA customers |
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

## 4. Open questions for PM

1. **Pricing:** burst credit (free within cap, or discounted?), spillover (PAYG list price
   or a discount?), and the tier price multipliers.
2. **Re-rating cadence** and the share of efficiency gains passed through.
3. **SLA credit schedule** and whether out-of-shape exclusion needs a grace margin (for
   example, 10% over the declared max).
4. **Minimum reservation size** and term lengths (monthly vs 1-year vs 3-year).
5. **Resize semantics:** can customers increase CUs mid-term instantly (subject to
   feasibility)? What notice is needed to decrease?
6. **Strict-dedicated option** (no PAYG backfill): offer at launch or later?
7. Should customers be able to **bring their own traffic priority** beyond `continuation`
   (for example, paid vs free users in their app)?
