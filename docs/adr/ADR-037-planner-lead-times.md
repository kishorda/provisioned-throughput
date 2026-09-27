# ADR-037: Sell capacity that is scheduled to arrive, from the date it arrives

- **Status:** Accepted. Implements docs/06 §1 ("yes, from date D").
- **Date:** 2026-09-26

## Context
The planner answered only "yes" or "no" against the replicas a region has today
(ADR-031). Capacity is planned ahead: GPUs are on order, and clusters come up on known
dates. A customer who asked for more than fits today got a flat refusal, even when enough
capacity would arrive next month. docs/06 §1 asked for "yes, from date D".

## Decision
- **Schedule.** `[[capacity_changes]]` lists replicas added to a pool from a date
  (`region`, `model`, `add_replicas`, `from`). Capacity only grows.
- **Checking a sale.** Every reservation is checked when it's made, against the pool's
  capacity on its start date (configured replicas plus arrivals by then) minus everything
  already reserved, whenever that starts. `reserve` takes the start date.
  - Create uses `start_at`.
  - A change before the start uses the term start.
  - Renewal uses the new term's start.
  - A mid-term change uses now, or the term start if later.
- **Why this can't oversell.** Take any moment *t*. The last reservation accepted among
  those active at *t* was checked against the sum of every reservation accepted before it.
  It was checked against capacity at its own start, which is at most *t*, and capacity
  never shrinks. So the active reservations never exceed capacity at *t*. The check is
  conservative: capacity reserved for a later start isn't lent out before then.
- **Telling the customer.** When a sale doesn't fit, the error names the first arrival
  date from which it would (`available_from` in the 409 `capacity_unavailable` body and in
  the message). Quotes that aren't feasible now carry `available_from`, and a note, when an
  arrival makes them feasible.
- The SQL planner applies the schedule in its conditional UPDATE
  (`reserved + need ≤ capacity + arrived`). The schedule comes from configuration, which
  every instance shares.

## Consequences
- ✅ Sales can be made ahead of hardware arriving, and customers learn the date.
- ✅ No new state: the counters stay aggregates, and the proof above needs nothing more.
- ⚠️ Conservative: a reservation starting later holds capacity from the moment it's sold,
  so a short reservation that would finish before then is refused.
- ⚠️ Capacity that frees up when a reservation ends (no renewal) isn't scheduled. Only
  arrivals are.
- ⚠️ A `capacity_changes` entry that's removed or delayed after sales were made against it
  overcommits the pool, as shrinking `replicas` does. Reconcile reports it, but can't fix
  it.
