# ADR-024: Move multi-region splits toward demand, within 20%

- **Status:** Accepted. Implements docs/07 §3.
- **Date:** 2026-09-24

## Context
A reservation with shares in several regions is sold as a fixed split, for example
eu-west 6 and eu-central 4. Behind a global endpoint, traffic lands where the clients are.
When demand doesn't match the split, one region throttles while the other idles. Docs/07
§3 planned to shift up to 20% of the CUs toward demand, never beyond placed capacity. The
open question was which reservations it should apply to.

## Decision
- **On by default for every reservation with shares in 2+ regions**, any SKU. Customers
  opt out per reservation (`"rebalance": false`). Opting out returns the split to the
  contract at once.
- **Effective split, not contract.** `regions` stays the contract, for the price, renewals,
  and the API. `effective_regions` is what gateways enforce. Failover shares and DNS
  steering follow it too. Any contract change (a CU increase, a renewal change) resets it.
- **Demand** is attempted WU per region over the last `window_minutes` (15). Throttled
  requests count at their estimate, so a region that's rejecting traffic shows its real
  demand. Below `min_requests` (100), nothing moves.
- **Algorithm** (`rebalance::target_split`, pure). It starts from the contract every time,
  and moves one CU at a time from the region with the least demand per CU to the one with
  the most. It continues while that lowers the busiest region's load by at least 10%,
  within `max_shift_fraction` (0.2) of the total CUs, and never below 1 CU in a region.
  It's deterministic, so the split drifts back when demand evens out.
- **Capacity first.** The new split's shares and failover headroom are reserved in the
  planner before it's published: growth first, then shrink released. A move that doesn't
  fit is skipped until the next pass.
- **Guard rails.**
  - The leader runs it every `interval_secs` (300).
  - A reservation moves at most once per `cooldown_minutes` (15).
  - It's frozen while an incident or its return ramp touches one of the reservation's
    regions: failover owns the split then.
  - Each move is a `SplitRebalanced` event, and opens an SLA grace window (`rebalance`)
    like a resize, while placement catches up.

## Consequences
- ✅ Throttling in one region while another idles is corrected within about a window plus
  one pass, with no customer action and no price change.
- ✅ Safe with several instances: only the leader rebalances. The planner reservation and
  the versioned write keep it consistent, and a lost race is undone.
- ⚠️ It reacts in minutes, not seconds. Short bursts are the burst bucket's job.
- ⚠️ A move that doesn't fit is skipped entirely, not partly applied.
- ⚠️ The SLA grace after each move slightly shrinks the measured window for reservations
  that move often. The cooldown bounds it.
- The numbers (window, cooldown, 20%, 10% gain, 100 requests) are placeholders until
  tuned on real traffic.
