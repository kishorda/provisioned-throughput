# ADR-026: Cap PAYG backfill on floor workers

- **Status:** Accepted. Refines [ADR-006](ADR-006-headroom-backfill.md) and [ADR-015](ADR-015-failover-payg-preemption.md).
- **Date:** 2026-09-24

## Context
Idle provisioned capacity is backfilled with PAYG (ADR-006). The router dispatches
pull-based: a request only goes to a worker with a free slot. Outside a failover, running
PAYG is never aborted (ADR-015). Together, these let a PAYG flood take every slot on the
provisioned floor. A provisioned request that arrived next waited for a PAYG request to
finish. The interference suite (ADR-025) measured a TTFT p95 of about 1.9 s for a tenant
at 90% of its entitlement, against an 800 ms target. Docs/05 §6 had named a "backfill
ratio" as a planner parameter but hadn't defined it. The options were:

- **Preempt PAYG whenever provisioned work waits.** ADR-015 rejected this outside
  failover, because it aborts PAYG during ordinary bursts.
- **Cap backfill on the floor.** PAYG keeps part of each floor worker free, so
  provisioned work finds room without aborting anything.
- **Reserve room from each reservation's expected concurrency.** Tighter, but it needs a
  concurrency model per reservation that doesn't exist yet.

## Decision
- **Backfill** (PAYG and spillover) may hold at most `backfill_ratio` of each floor
  worker's slots and KV blocks. The router setting defaults to 0.5, a placeholder until
  calibration. The KV cap doesn't apply to a worker's first backfill request, so one
  larger request can still run.
- **Hot spares aren't capped.** They exist to serve PAYG until they're claimed, and
  ADR-015 reclaims them during a failover.
- **Spillover counts as backfill.** It's a PT customer's overflow and is never aborted,
  so it mustn't fill the floor either.
- **Pools.** A PAYG-only pool sets the ratio to 1.0. A strict-dedicated pool (no
  backfill) would set it to 0.0.

## Consequences
- ✅ Provisioned work finds a free slot without aborting anything. In the PAYG-flood
  scenario, A's TTFT p95 fell from about 1.9 s to about 65 ms.
- ✅ The rule is pure and lives in `workers.rs`, next to the other selection rules, so it
  moves into a Dynamo `WorkerFilter` with them (docs/13 §3).
- ⚠️ Up to `1 − backfill_ratio` of each floor worker can sit idle while PAYG waits. That
  capacity was sold to provisioned customers, so it's an economic cost, not an SLA
  risk. The planner should tune the ratio per model from observed provisioned
  concurrency.
- ⚠️ A reservation whose concurrency exceeds the uncapped share still competes with
  running PAYG for slots until PAYG completes. The third option above would remove that.
