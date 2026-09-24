# ADR-003: Lease-based distributed quota; no central store on the hot path

- **Status:** Accepted. Amended by [ADR-012](ADR-012-single-instance-quota-coordinator.md): a single coordinator with soft state, not Raft.
- **Date:** 2026-09-23

## Context
Each region runs tens of gateway replicas. A reservation's WU/s must be enforced across
all of them within 2% (N6), with sub-2 ms admission (N1).

## Decision
A per-region **Quota Coordinator** (Rust, 3-node Raft via openraft) grants each gateway
time-bounded **leases**: a WU/s slice and a burst slice. Leases last 1 s and are renewed
every 250 ms, sized by recent local demand. Admission is local and lock-free. When the
coordinator is unavailable, leases decay to a safe floor. Small tenants are pinned to 2
home gateways.

## Consequences
- ✅ No network hop on the admission path. The hot path does not depend on Redis.
- ✅ Bounded over-admission (Σ leases ≤ entitlement).
- ⚠️ Rebalancing lag (≤ 250 ms) can cause brief local throttling when load shifts between
  gateways. Mitigated by a minimum lease floor and burst slices.
- ⚠️ One more stateful component to operate per region.

## Alternatives rejected
- Central Redis/GCRA per request: adds a network hop, and the store becomes a regional
  single point of failure.
- Static division by gateway count: wastes entitlement under uneven load-balancer hashing.
