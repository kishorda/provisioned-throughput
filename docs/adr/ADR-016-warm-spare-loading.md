# ADR-016: Load warm spares from snapshot failover demand

- **Status:** Accepted. Refines [ADR-006](ADR-006-headroom-backfill.md) and [ADR-014](ADR-014-automatic-region-failover.md).
- **Date:** 2026-09-24

## Context
A Multi-region reservation's paired region holds failover headroom (ADR-014). In the pool
it's made of hot spares (loaded, serving PAYG, reclaimed by the router: ADR-015) and warm
spares (weights staged on node-local NVMe, no GPU claim, under 2 minutes to serve, docs/06
§3). Until now, `warmSpares` was informational. Two questions had to be answered:

1. **How the Regional Capacity Controller learns about a failover.** The options were:
   - the signed regional snapshot, which already carries dormant failover shares and
     the active failures;
   - new CRD fields written by the global planner, plus a regional flag;
   - an operator flag on the pool.
2. **How many warm spares to load.** All of them, or only what the demand needs.

## Decision
- **Follow the snapshot.** The controller long-polls the region's signed entitlement
  snapshot (`PT_CONTROL_PLANE_URL`, `PT_REGION`, `PT_REGION_TOKEN`,
  `PT_SNAPSHOT_PUBLIC_KEY`). It verifies the signature and keeps a last-known-good cache
  (`PT_SNAPSHOT_CACHE`). Every new snapshot triggers a reconcile of every pool.
- **Map demand onto allocations.** Each `PoolAllocation` grows by the same fraction as its
  reservation: `wuPerSec × active_failover_cus ÷ cus`. The failed region's activation,
  including the return ramp, is applied, so no WU-per-CU constant is needed.
- **Size for demand.** The pool is sized with the failover demand added. The rise in the
  floor is absorbed first by hot spares, then by loading warm spares, up to `warmSpares`,
  per role. `desiredReplicas` grows by the loaded count. `minAvailable` rises to the
  failover floor + k, so drains can't take the replicas carrying failover traffic.
  Demand beyond hot and warm spares sets `CapacityShortfall`
  (`FailoverHeadroomExhausted`).
- **Hold, then release.** Loaded warm spares aren't reduced while any failover demand
  remains, so the return ramp doesn't churn GPUs. They're released when it reaches 0.
  Pools with failover demand resync every 30 s.
- **Record intent.** The DGD carries `pt.example.com/warm-spares-staged` and
  `…/warm-spares-loaded` annotations for the weight prefetcher. Dynamo has no warm-spare
  concept.

## Consequences
- ✅ One trust path and one source of truth for failover, shared with the gateways. The
  activation ramp comes for free.
- ✅ GPUs are spent only on the demand that the hot spares can't cover.
- ✅ A restart during a control-plane outage still knows an active failover, from the cache.
- ⚠️ Like gateway activation (ADR-014), loading needs the control plane to declare the
  incident.
- ⚠️ "Warm" is only as fast as staging makes it. The prefetch DaemonSet that keeps weights
  on node-local NVMe (docs/06 §4) isn't built, so a loaded spare starts cold without it.
- ⚠️ It assumes `PoolAllocation.reservation` equals the control plane's reservation id.
- ⚠️ Sizing holds the loaded count, but Kubernetes picks which pods to remove on
  scale-down, so the pods removed may not be the ones that were loaded.
