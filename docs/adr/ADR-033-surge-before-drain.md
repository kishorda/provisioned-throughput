# ADR-033: Surge a replica for every pod on a cordoned node

- **Status:** Accepted. Implements docs/06 §6.
- **Date:** 2026-09-26

## Context
Nodes are drained for patching, driver and firmware upgrades, and node-pool upgrades. The
controller sets PodDisruptionBudgets at `floor + failure_k` per role, so a drain can never
take a pool below that. But the budgets only allow as many evictions as the pool has spare,
which is its maintenance slots and hot spares. So every drain used up the headroom meant
for failures and failover, and a drain of a node with more pods than that stalled until
someone intervened.

docs/06 §6 asked for surge-then-drain. The question was how the controller hears about a
drain:

- **Watch for cordoned nodes.** Every drain tool (`kubectl drain`, managed node-pool
  upgrades, the cluster autoscaler) cordons first, then evicts through the Eviction API,
  retrying while a budget refuses.
- **A `Drain` custom resource.** The controller runs the whole workflow itself. This gives
  more control, but every patching tool would have to create `Drain` objects instead of
  draining.

## Decision
- **Surge on cordon.** For every worker pod of a pool on a cordoned node (not already
  terminating), the controller adds one replica of that role to the pool's
  `DynamoGraphDeployment` (`status.drainSurge`, `status.drainingNodes`).
- **The budgets don't change.** They still keep `floor + failure_k` available. So
  evictions are refused until the surge replicas are ready, and then go through, without
  touching the maintenance slots or hot spares.
- **The surge ends by itself.** It goes away when the node no longer hosts the pool's pods,
  or is uncordoned.
- **Expedite.** A node annotated `pt.example.com/drain=expedite` gets no surge. Its pods
  leave through the maintenance slots and hot spares at once, which the budgets already
  allow. This is for urgent security patches.
- **`maxReplicas` caps the surge**, decode replicas first. When it bites, the `Draining`
  condition says `SurgeCapped`, and the rest of the drain waits for the maintenance slots.
- The decision is pure (`drain::assess`, `drain::add_surge`, `plan`). The controller lists
  the pool's worker pods by label and their nodes. It reconciles on pod changes, and on
  node changes filtered to cordon and annotation changes, because node status changes
  every few seconds.

## Consequences
- ✅ Standard drain tools work unchanged, and pools keep their failure and failover
  headroom during maintenance.
- ✅ Concurrency needs no separate rate limit: the budget bounds how many evictions
  proceed, and the surge sets how fast room is made.
- ⚠️ The surge needs free GPUs somewhere in the cluster. Without them, the surge replicas
  stay pending and the drain waits, which is safe. The `Draining` condition shows it, but
  there's no alert yet.
- ⚠️ Between an eviction and the next reconcile, Dynamo may briefly start a replacement
  that the lower surge then removes. The pool never drops below its budget.
- ⚠️ Counts assume one pod per replica. Multi-node replicas (Grove) need a surge per pod
  group.
- ⚠️ "Expedite" doesn't pause new sales on the pool, as docs/06 §6 suggested: the control
  plane doesn't read cluster state.
- ⚠️ Untested against a cluster, like the rest of the controller.
