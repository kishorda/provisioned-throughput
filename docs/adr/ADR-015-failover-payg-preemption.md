# ADR-015: Fence, then preempt, PAYG on hot spares during failover

- **Status:** Accepted. Refines [ADR-006](ADR-006-headroom-backfill.md) and [ADR-013](ADR-013-tenant-scheduling-tier.md).
- **Date:** 2026-09-24

## Context
Hot spares serve preemptible PAYG until provisioned traffic needs them (ADR-006, docs/06
§3). When a region fails, its pair takes on the failed share (ADR-014), and provisioned
traffic grows onto the spares. The router orders only new dispatches, so PAYG requests
already running on spares would hold them until they finish. That can take seconds to
minutes, which misses the "< 1 s" hot-spare target. Aborting PAYG mid-generation loses work
and upsets PAYG customers, so the question was when to do it:

- **Always**, whenever provisioned work waits. Simple, but it also aborts PAYG during
  ordinary provisioned bursts, which the floor and burst allowance should absorb.
- **Drain only** during failover. No aborts, but reclaiming a spare is slow.
- **Fence, then abort** during failover.

## Decision
- **Fence, then abort, only during a failover.** The gateway marks provisioned requests
  that use a failover entitlement with `x-pt-failover`. Each one keeps the router's
  failover state active for `failover_hold_ms` (30 s).
- While failover is active, new PAYG isn't placed on hot spares.
- A provisioned queue head that has waited `preempt_grace_ms` (250 ms) with every eligible
  worker busy aborts one running `payg` request whose room lets it fit. The router prefers
  one on a hot spare, then the youngest.
- Victims are named once, and their capacity counts as freed, so each waiting request
  aborts at most what it needs.
- Spillover is never preempted.
- A preempted client gets 503 `preempted` with `Retry-After: 1`. If it was streaming, it
  gets a final SSE error event. Dropping the upstream response cancels the work on the
  worker.
- At other times, PAYG *prefers* hot spares, and provisioned traffic prefers the floor.

## Consequences
- ✅ Spares are reclaimed within about 250–300 ms of provisioned work queuing, which meets
  the docs/06 hot-spare target.
- ✅ PAYG is never aborted outside a failover, and never more than needed.
- ✅ Preemption lives in the pure dispatcher and selection code, so it can move into a
  Dynamo plugin with the rest (docs/13 §4).
- ⚠️ Aborted PAYG work is lost. A preempted PAYG request should be billed only for tokens
  delivered. The PAYG metering path isn't in this repository yet.
- ⚠️ This is whole-request preemption. Iteration-level preemption with KV offload
  (docs/05 §4) would lose less and still needs the engine patch.
- ⚠️ The failover signal is carried by traffic. A pool whose provisioned traffic doesn't
  pass the router won't fence.
- ⚠️ `hot_spare` is router configuration today. The capacity controller doesn't yet
  render spares as separately addressable workers.
