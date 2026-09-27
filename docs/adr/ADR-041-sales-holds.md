# ADR-041: Pause new sales on a pool during an expedited drain

- **Status:** Accepted. Completes docs/06 §6 and [ADR-033](ADR-033-surge-before-drain.md).
- **Date:** 2026-09-26

## Context
An expedited drain (a node annotated `pt.example.com/drain=expedite`, ADR-033) gives up
the surge and lets pods leave through the maintenance slots and hot spares at once. For its
duration, the pool has less headroom than it was sold with. docs/06 §6 said it should pause
new sales on the pool. But the control plane sells capacity without reading cluster
state.

## Decision
- **The controller reports holds.** When a `ModelPool` with `spec.catalogModel` (the
  control plane's model id) has an expedited drain, the controller calls
  `PUT /internal/v1/holds/{model}` with the region's token: source `namespace/pool`,
  reason, and a 120 s TTL. It renews the hold on every reconcile (every 15 s while
  draining) and lifts it with `DELETE` once no node drains expedited. Failures are logged
  and never block reconciling.
- **The control plane refuses growth.** While any unexpired hold exists for a region and
  model, `create` and any update that grows the region answer 409 `sales_paused`, saying
  until when. Renewals, shrinks, other regions, rebalancing, and failover are unaffected.
  `GET /internal/v1/holds` (operator key) lists holds in force.
- **Storage.** Holds live in the store (`sales_holds`, migration 0006), so every
  control-plane instance sees them. They expire on their own, so a controller that dies
  mid-drain can't pause sales for good.

## Consequences
- ✅ No new sales land on a pool while it's short of its maintenance headroom.
- ✅ The region token already authenticates the controller, so there are no new
  credentials.
- ⚠️ A hold pauses the whole model in the region, not one pool. A region with several
  pools for a model pauses all of them.
- ⚠️ `catalogModel` is set by hand on each pool. Pools without it don't pause sales.
