# ADR-045: Place each region share on one pool, by best fit

- **Status:** Accepted. Extends [ADR-031](ADR-031-capacity-in-replicas-per-tier.md) and
  [ADR-037](ADR-037-planner-lead-times.md).
- **Date:** 2026-09-27

## Context
The planner counted capacity per (region, model): one pool of replicas with one
`PerformanceProfile`. Real regions run a model on several pools: a new GPU generation
beside the old one (docs/10 §1), a long-context disaggregated pool beside a shorter one,
or pools in different clusters. A region with two pools could only be configured as one,
which mixed profiles and hid which hardware a reservation was on. It also left no way to
move reservations to newer hardware.

## Decision
- **Pools have ids.** `[[capacity]]` gains `pool`, unique within a region, defaulting to
  the model id. A region can list several pools per model. `[[capacity_changes]]` names
  its pool when the region has more than one. Costs per CU (ADR-031) and scheduled
  arrivals (ADR-037) are per (region, pool).
- **One pool per share.** Each region share of a reservation is placed on exactly one
  pool, and the reservation records it in `placements` (region → pool).
  - A gateway prices the share with that pool's profile, so WU accounting stays exact.
  - The share's failover headroom in that region (ADR-014) lives on the same pool.
  - A share never splits across pools. When no single pool fits, the sale fails with
    `capacity_unavailable`, and `available` is the most any one pool could take.
- **Best fit.** A new share goes to the pool with the least free capacity that still
  fits it (in micro-replicas at the reservation's tier, counting arrivals by the start
  date). Ties go to the pool listed first in configuration, so operators list preferred
  hardware first. Packing small sales keeps large blocks free for large sales. The rule
  is one pure function (`planner::place`) that both planners use.
- **Existing shares stay put.** Every change to a region the reservation already holds
  (tier changes, renewals, rebalancing, shape checks) uses its placed pool.
  - If growth doesn't fit that pool, the whole regional holding moves to a pool that fits
    the new total. The new pool is reserved first, then the old one released.
  - The customer sees a `relocated` event. CUs and price don't change, and the event opens
    an SLA grace window like a resize.
- **Operator moves.** `POST /internal/v1/reservations/{id}/move {"region", "pool"}` moves a
  region's holding to a named pool the same way. It's the hardware-migration path from
  docs/10. `GET /internal/v1/reservations/{id}/placements` shows where each region is.
- **Pools are internal.** Customers buy CUs, not hardware. Placements never appear in
  customer responses, and the model catalog lists each region once, with its longest
  context. The snapshot format is unchanged, and only `profile` follows the pool. Adding
  a field would break gateways that deny unknown fields during a rolling upgrade.
- **Compatibility.** A reservation without a placement for a region lives on the region's
  default pool: the one whose id is the model id, else the first listed. Migration 0008
  copies each (region, model) counter row into `pool_capacity` under pool = model id, so
  existing sales keep their counts. The old table stays for a rollback.

## Consequences
- ✅ A region can run a model on several GPU classes or context lengths, each priced and
  counted by its own profile. New hardware can be added and filled without reconfiguring
  the old pool.
- ✅ Reservations move to new hardware one at a time, with capacity reserved on the target
  first, so a move never oversells or leaves a reservation without capacity.
- ⚠️ A share that fits no single pool fails, even when the region's total free capacity
  would cover it. Splitting shares was rejected, because a gateway would then enforce one
  reservation with two profiles.
- ⚠️ Best fit is greedy. The nightly MILP rebalance (docs/06 §2) that would repack pools
  isn't built, and moves are operator-driven.
- ⚠️ The capacity controller still maps reservations to pools through `PoolAllocation`
  CRDs that are maintained separately. Generating them from placements is future work.
