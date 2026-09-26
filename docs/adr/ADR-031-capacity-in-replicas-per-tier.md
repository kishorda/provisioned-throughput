# ADR-031: Count sellable capacity in replicas, and cost each CU by its tier

- **Status:** Accepted. Implements docs/06 §1–2 for sale-time checks.
- **Date:** 2026-09-26

## Context
The control plane checked every sale against a count of CUs per (region, model), whatever
the tier. A CU is a fixed WU/s, but a replica delivers fewer WU/s at a tighter latency
target. With the development B200 profile, a replica serves 58,000 WU/s at Standard,
41,000 at Interactive, and 35,500 at Agentic. So an Agentic CU needs about 1.6 times the
GPU of a Standard one. A pool sized as "200 CUs" could therefore be sold 200 Agentic CUs
that need more replicas than it has. That's an SLA breach decided at the moment of sale.

The options were:
- **Replicas**, the unit the capacity controller deploys and the operator sizes floors in.
- **Standard-equivalent CUs**, with other tiers weighted by capacity ratio. Easier for
  sales to read, but a second unit that only approximates what's deployed.

## Decision
- **Pools are counted in replicas.** `[[capacity]] replicas` is what provisioned floors
  may use in a region, before the operator's failure-domain and maintenance headroom
  (docs/06 §2).
- **A CU at tier T costs** `wu_per_cu ÷ (profile capacity at T × 0.8)` replicas. The 0.8
  is the target utilisation quotes use (`quote::TARGET_UTILISATION`), a placeholder until
  calibration. Costs are rounded up to whole micro-replicas and counted as integers, so
  SQL arithmetic is exact and rounding never oversells (`capacity::Costs`).
- **Every planner call names the tier.** Reservations reserve and release at their tier.
  Undo records (`PlanOp`) carry the tier.
  - A tier change before the term starts moves the capacity at once, and fails cleanly if
    the new amount doesn't fit.
  - A scheduled tier change is costed at renewal. If it no longer fits, the reservation
    renews at its current tier and records `ScheduledChangeFailed`.
- **Availability is per tier.** Quotes report `available_cus` for each tier they price,
  and a reservation's own capacity counts at its current tier's cost.
- **Upgrade.** Migration 0005 adds `capacity_micro`, `reserved_micro`, and `micro_counted`
  to `capacity_pools`. The first upgraded instance to start recomputes reserved amounts
  from live reservations, once, under row locks, so two instances starting together count
  each pool exactly once. The old CU columns stay, unused, for a rollback.
- Config validation requires every pool's profile to have a positive capacity for each
  tier its model is offered at.

## Consequences
- ✅ A region can't sell more of a tier than its replicas can serve at that tier's SLO.
  Mixed-tier pools are costed exactly. Eight B200 replicas hold 227 Agentic CUs, or 371
  Standard ones, or any mix.
- ✅ The planner and the operator use the same unit, so what's sold is what gets deployed.
- ⚠️ Sale-time checks don't include the correlated-burst allowance the operator adds when
  sizing (docs/06 §2). That comes out of the headroom that `replicas` already excludes.
- ⚠️ During a rolling upgrade, instances on the old version sell against the old CU
  columns. Their sales aren't in `reserved_micro` until the leader's reconcile corrects the
  drift (two runs). Keep the rollout short, or drain sales during it.
- ⚠️ `[[capacity]] cus` is replaced by `replicas`. Existing configs must be converted, for
  example: replicas = CUs sold × the costliest tier's cost per CU.
