# ADR-027: Active/standby Quota Coordinator on a Kubernetes Lease

- **Status:** Accepted. Amends [ADR-012](ADR-012-single-instance-quota-coordinator.md).
- **Date:** 2026-09-24

## Context
ADR-012 runs one Quota Coordinator per region with soft state. While it's down, each
gateway's lease expires and decays towards 50% of `entitlement ÷ gateways`, so a busy
reservation is underserved until the coordinator returns. A crash or a rolling update
therefore costs throttling that counts against the SLA. ADR-012 said to revisit Raft or an
active/standby pair if that became a risk.

A fresh coordinator was also not quite safe. It started empty while gateways still held
grants from before the restart, so it could grant new ones on top of them for up to one
lease period.

The options were:

- **Raft (openraft), 3 nodes**, as in ADR-003. Replicates grants, so any node can take
  over with full state. That's a large replicated state machine for state that can be
  rebuilt within a second.
- **Gateway-driven failover with no election.** Gateways switch to a second URL when the
  first fails. Under an asymmetric partition both coordinators grant the full
  entitlement, which oversells up to 2×.
- **Active/standby on a Kubernetes Lease.** Only the lease holder grants. The election is
  regional, so static stability holds.

## Decision
- **Two replicas** per region, as a StatefulSet (`deploy/quota/`). They compete for a
  `coordination.k8s.io/v1` Lease with client-go's rules
  (`pt_quota::election`):
  - The leader renews every `retry_period` (1 s). It serves renewals only until
    `renew_deadline` (3 s) after its last successful renewal, timed from before the write.
  - A candidate takes over a record only after watching it unchanged for
    `lease_duration` (5 s) by its own clock. Every write is a compare-and-swap on
    `resourceVersion`.
- **Taking over a dead leader needs no warm-up.** Config validation requires
  `lease_duration − renew_deadline ≥ 1.5 × lease_ttl` (the grant hold). The dead leader's
  last grant was issued at least that long before the takeover, so all of its grants
  have expired by then.
- **Taking over a released lease warms up.** On SIGTERM the leader stops serving, then
  clears the holder, so a standby takes over on its next tick. Gateways still hold the old
  leader's grants, so for one grant hold the new leader grants each gateway at most the
  unexpired lease it reports (`held_wu_s` in the renewal). A gateway holding none gets no
  lease and keeps its current rate. The old grants summed to at most the entitlement, so
  the new ones do too. A single coordinator without `[election]` warms up the same way
  when it starts, which closes the restart gap.
- **Gateways list every replica** (`[quota] coordinator_url` plus `standby_urls`). A
  standby answers 503 `not_leader`, and the gateway tries the next URL within the same
  renewal, starting from the one that answered last.
- Every replica reports ready (`/healthz`), so rollouts proceed. `/leader` shows which
  one leads, for monitoring.

## Consequences
- ✅ A rolling update or a clean shutdown hands over with no gap: gateways stay on leases
  throughout, and the leases never add up to more than the entitlement (tested end to
  end).
- ✅ A crash costs about `lease_duration` (5 s) of fallback decay, not the whole outage.
  The decay is gradual (30 s to 50%), so a 5 s gap barely reduces admission.
- ✅ The election is pure logic over a small `LeaseBackend` trait. It's tested with an
  in-memory lease, including racing candidates and a leader that can't renew.
- ⚠️ The Kubernetes backend hasn't run against a cluster: there's none on the development
  machine.
- ⚠️ Leadership depends on the regional API server. While the leader can't reach it, the
  leader stops serving after 3 s and gateways fall back, as they did when the single
  coordinator was down. It never oversells.
- ⚠️ Safety assumes bounded clock-rate drift between replicas (not synchronised clocks),
  as client-go does. The 2 s margin also has to cover gateway network delay on grants.
- ⚠️ A gateway that joins during a warm-up waits up to one grant hold (1.5 s) for its
  first lease.
