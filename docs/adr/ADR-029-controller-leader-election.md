# ADR-029: Active/standby capacity controller on a Kubernetes Lease

- **Status:** Accepted. Reuses the election from [ADR-027](ADR-027-quota-coordinator-standby.md).
- **Date:** 2026-09-25

## Context
The Regional Capacity Controller (`pt-operator`) ran as one replica with a `Recreate`
strategy, because two replicas would both reconcile. They would race on every
`DynamoGraphDeployment` and PodDisruptionBudget, and could flap `warmSparesLoaded` and
other status fields. With one replica, a node failure or a rolling update left pools
unreconciled until the pod was rescheduled. That's minutes in which a failover's warm
spares don't load.

The Quota Coordinator already elects a leader through a Kubernetes Lease (ADR-027).
kube-rs has no built-in leader election.

## Decision
- **Share the election.** It moves into its own crate, `pt-election`: the elector, the
  in-memory and Kubernetes Lease backends, and SIGTERM handling. The Quota Coordinator
  and the controller both use it. The coordinator keeps its warm-up on top.
- **Only the leader runs the controller** (`pt_election::lead`). Standbys follow the
  entitlement snapshot, so they're ready to take over, but they don't watch or reconcile
  pools.
- **Losing leadership stops reconciling at once and exits.** The controller future is
  dropped at the renew deadline (10 s after the last successful renewal), 5 s before a
  standby may take over (lease duration 15 s). The process then exits and restarts as a
  standby with empty caches, as client-go-based controllers do.
- **Clean shutdown hands over at once.** On SIGTERM the leader stops the controller and
  releases the lease, and a standby acquires it on its next retry (2 s).
- **Deployment.** Two replicas, preferring different nodes, with a namespaced Role for
  `leases` in `pt-system`. `POD_NAME` is the identity. `PT_LEADER_ELECTION=false` turns
  election off for single-replica local runs. Timings default to client-go's 15/10/2 s and
  can be overridden by environment variables.

## Consequences
- ✅ A crashed or evicted leader is replaced within about 15 s, and a rolling update within
  about 2 s, with no moment where two replicas reconcile.
- ✅ The controller's reconcile logic is unchanged. Only `main.rs` decides when it runs.
- ✅ `lead` is tested with an in-memory lease, including a leader cut off from the lease:
  its work stops before the standby's starts.
- ⚠️ Like the coordinator's, the Kubernetes backend hasn't run against a cluster.
- ⚠️ A write already in flight when leadership lapses can still land. The 5 s margin
  covers any write that completes within it, as in client-go.
- ⚠️ A standby takes over with cold caches. Its first reconcile of every pool happens on
  its initial list, which is a burst of API reads in a large region.
