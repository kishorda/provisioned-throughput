# ADR-023: Several control-plane instances, shared state in the database, one leader

- **Status:** Accepted. Amends [ADR-014](ADR-014-automatic-region-failover.md) and
  [ADR-017](ADR-017-sql-control-plane-store.md).
- **Date:** 2026-09-24

## Context
Reservations, keys, incidents, and invoices were durable (ADR-017), but four things lived
in each process, so only one control-plane instance could run:
- **The capacity planner's reserved counts,** rebuilt at startup. Two instances would
  each think they owned all capacity and could oversell.
- **The entitlement version,** from each process's clock. Two instances could label
  different content with the same version, or wake only their own long-polls.
- **Region health,** from heartbeats that land on whichever instance a gateway reaches.
- **The background loops** (lifecycle, failover, invoices), which would run everywhere.

## Decision
- **Every instance serves the full API.** Shared state lives in the database (migration
  0003).
- **Capacity counters** (`capacity_pools`, `SqlPlanner`). A sale locks its pools in a
  fixed order and raises `reserved` with a conditional UPDATE
  (`reserved + n <= capacity`), for all regions in one transaction. Pool sizes follow the
  configuration at startup.
- **Reconciliation.** Reserving and saving the reservation are separate transactions, so
  an instance dying in between leaks capacity. The leader compares the counters with live
  reservations every 60 s. It corrects a pool only when the same drift shows on two runs
  in a row, so a sale in flight is never undone.
- **Entitlement version** (`control_plane_state`). Every committed change runs
  `GREATEST(value + 1, now_ms)`. Every instance bumps it at startup, so a restart always
  republishes (ADR-020). Snapshots read the shared version before the data. Each instance
  polls it every 500 ms to wake its own long-polls for other instances' changes. That
  works on CockroachDB, which has no LISTEN/NOTIFY.
- **Heartbeats** (`gateway_heartbeats`). Each gateway has a row with its last heartbeat,
  whether it's serving, and the start of its current serving run. Region health is
  computed from the rows (`failover::region_status`), so any instance can judge it.
- **One leader** (`leases`, name `background`). The holder renews every 5 s, with a 15 s
  TTL. It runs the lifecycle, failover detection, capacity reconciliation, invoice
  finalisation, and pruning. If it stops renewing, another instance takes over when the
  lease expires. The loops stay safe if two briefly overlap during a handover: they rely on
  version checks and unique constraints.
- **Stricter failover declaration** (amends ADR-014). Heartbeats now persist across
  restarts, so after a control-plane or database outage every region looks stale at once.
  A region is declared down only while another region has served continuously for at least
  the heartbeat timeout, which proves heartbeats are flowing again.

## Consequences
- ✅ The control plane can run as several instances behind a load balancer. Tested
  against PostgreSQL with two instances: concurrent sales can't oversell, a change through
  one reaches snapshots and long-polls from the other, heartbeats count wherever they land,
  leadership hands over, and a leak is repaired while a sale in flight isn't.
- ✅ With the real binaries, the second instance took over about 18 s after the leader was
  killed: the TTL plus the renewal cadence.
- ⚠️ Failover detection, lifecycle transitions, and invoice finalisation pause for up to
  about 20 s during a leader handover.
- ⚠️ The lease uses each instance's clock. Clock skew between instances beyond a few
  seconds could let two instances lead at once. The loops tolerate that, but it should be
  avoided (NTP).
- ⚠️ The version counter is one hot row, written on every change. That's fine at control-
  plane write rates.
- ⚠️ A version bump that fails after a committed change is logged. Gateways see the change
  on the next bump.
- The in-memory store and planner still work for one instance, for tests and local runs.
