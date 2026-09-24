# ADR-012: Single-instance Quota Coordinator with soft state

- **Status:** Accepted. Amends [ADR-003](ADR-003-lease-based-distributed-quota.md).
- **Date:** 2026-09-23

## Context
ADR-003 specifies a 3-node Raft group per region for the Quota Coordinator. Lease state is
soft: every gateway re-reports demand and its snapshot's entitlement every 250 ms, so a
fresh coordinator rebuilds its full state within one lease period. Gateways already have a
safe fallback when the coordinator is unreachable. Raft would add a large replicated
state machine to protect state that can be rebuilt in a second.

## Decision
- Run **one coordinator per region**, with in-memory state only.
- Gateways renew every 250 ms over **HTTP/JSON** (`POST /v1/leases/renew`). The message
  shapes (`pt_quota::wire`) map onto a gRPC service later. gRPC isn't used yet because
  its codegen needs `protoc`.
- **Never oversell.** A gateway's target share is `floor + max-min fair share of demand +
  an even split of what's left over`. It's granted only what other gateways' unexpired
  grants leave free. The coordinator counts a grant as outstanding for 1.5 × the lease TTL,
  longer than the gateway uses it.
- **Demand** is WU *attempted* per second, including rejected requests, so a gateway
  starved of quota can show it needs more.
- **Entitlement** comes from the gateways' snapshots. The highest snapshot version wins.
- **Fallback.** Before the first lease, a gateway admits `entitlement ÷ assumed_gateways`.
  After a lease expires, its rate moves linearly to `50% × entitlement ÷ active gateways`
  over `fallback_decay_secs`, as in ADR-003.
- **No burst slices.** Burst credit accrues locally from unused lease, so it's bounded by
  the lease too.

## Consequences
- ✅ Simple to run and test. A coordinator restart costs at most one fallback period.
- ✅ The invariant "sum of grants ≤ entitlement" holds at every instant, including while
  gateways join, leave, or shift demand, and when entitlements shrink.
- ⚠️ While the coordinator is down, gateways fall back to half the entitlement in total.
  This is safe, but underserves busy tenants during the outage.
- ⚠️ A gateway partitioned from the coordinator while others aren't can briefly overlap
  with re-granted capacity. The overlap is bounded by its decaying fallback rate.
- ⚠️ Consistent-hash "home gateways" for small tenants (docs/04 §6) aren't built. Every
  replica holds a floor for every reservation in the region.
- Revisit Raft, or an active/standby pair, if fallback periods become a measurable SLO risk.
