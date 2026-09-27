# ADR-039: Size the floor's room for provisioned work from its expected load

- **Status:** Accepted, opt-in. Refines [ADR-026](ADR-026-backfill-ratio.md).
- **Date:** 2026-09-26

## Context
ADR-026 caps backfill (PAYG and spillover) at a fixed `backfill_ratio` of each floor
worker, 0.5 by default, so provisioned work always finds room. A fixed ratio is wrong both
ways. A lightly used floor leaves half its slots idle while PAYG queues. A floor whose
reservations need more than half its slots can still be crowded, since the cap only keeps
room free at the moment PAYG is placed.

## Decision
With `[adaptive_backfill]`, the router sizes the room from what the floor should expect:
- **Expected provisioned slots** = Σ allocation WU/s × *slot-seconds per WU*. The router
  learns the slot-seconds per WU as a moving average over finished provisioned and burst
  requests on the floor: the time each held its slot ÷ its WU estimate.
- **Observed peak.** The router also tracks a decaying peak of provisioned and burst slots
  in use on the floor (`half_life_secs`, 60 by default). This covers reservations without
  allocations and bursts above the average.
- **The ratio.** Reserve = max(expected, peak) × `headroom` (1.25). Then
  backfill ratio = 1 − reserve ÷ floor slots, clamped to `min_ratio` (0.1) and `max_ratio`
  (0.9). Hot spares are never capped, as before.
- **Before anything is learned,** the fixed `backfill_ratio` applies. `/v1/router/status`
  shows the current `backfill_ratio`, `expected_provisioned_slots`, and
  `peak_provisioned_slots`.
- It stays pure (`Dispatcher::with_adaptive_backfill`, `finish(id, now)`) and opt-in. The
  fixed ratio remains the default.

## Consequences
- ✅ PAYG gets more of a quiet floor, and less of a busy one, without aborting anything.
- ⚠️ It trades margin for utilisation. In the interference suite's PAYG flood, PAYG got
  about 11 of 16 slots instead of 8, and tenant A's TTFT p95 rose from 64 ms to 405 ms:
  still within its 800 ms target, but closer. Raise `headroom` to buy margin back.
- ⚠️ Slot-seconds per WU is one number for the pool, learned from the WU estimate. Tenants
  with very different request shapes share it.
- ⚠️ The allocations' WU/s is the entitlement, not the traffic: a reservation that uses
  little of it keeps its room anyway, which is the point of a reservation.
