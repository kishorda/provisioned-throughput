# ADR-044: Hot spares found by routers through labelled headless Services

- **Status:** Accepted. Builds on [ADR-015](ADR-015-failover-payg-preemption.md).
- **Date:** 2026-09-27

## Context
Routers treat hot spares differently. PAYG prefers them in normal times, and during a
region failover PAYG is fenced off them and preempted (ADR-015). But the router learned
its workers, and which were spares, only from `[[workers]]` with a hand-set `hot_spare`
flag. The capacity controller sizes `headroom.hotSpares` into each pool's replicas, but
nothing told the router which pods those were. Pods come and go with every scale-up,
restart and drain, so a static list drifts. A worker marked wrongly either delays
provisioned work behind PAYG, or leaves a spare out of failover.

## Decision
- **Labels.** The controller labels every Ready worker pod of a pool
  `pt.example.com/serving=floor` or `=spare` (`pt_operator::spares::assign`, pure).
  - Per role, spares come only from Ready pods beyond `min_available` (floor plus
    failure-domain headroom), up to `headroom.hotSpares`. A short pool never hides its
    floor behind spares.
  - Current spares stay spares while they're Ready, so reconciles don't move PAYG
    around. New spares are taken from the newest-named floor pods.
  - A spare that stops being Ready goes back to `floor`, and another Ready pod takes
    its place.
  - Labels are merge patches, best effort; a failed patch is retried at the next
    reconcile.
- **Services.** For each pool the controller renders two headless Services over the
  routed role: `<pool>-workers` (`serving=floor`) and `<pool>-spares` (`serving=spare`).
  The routed role is `aggregated`, or `decode` in a disaggregated pool (`render.rs`).
  Kubernetes Service selectors only match by equality, which is why floor pods carry a
  label too, rather than spares being "everything not labelled floor".
- **Discovery.** A router with `[discovery]` resolves both names every `refresh_secs`
  (5). Each address is a worker `http://ip:port` with the configured `slots` and
  `kv_blocks`. Addresses from the spares Service are hot spares.
  - A floor lookup that fails or returns nothing keeps the last known floor workers.
  - A failed spares lookup means "no spares" only if the floor lookup worked in the same
    refresh. A headless Service with no endpoints doesn't resolve, and that's normal
    for spares.
  - An address in both answers counts as floor.
- **Changing the worker set.** `Dispatcher::set_workers` (pure) applies each refresh:
  - Known workers get their spare flag and size updated.
  - A worker that's no longer listed is **retired**: it gets no new work, and its running
    requests finish on it.
  - A new worker takes the slot of a retired worker with nothing running, after that
    slot's prefix and session hints are forgotten. Worker indexes held by running
    requests stay valid.
- `[discovery]` and `[[workers]]` are exclusive. `dedicated_workers` names static worker
  ids, so it can't be combined with discovery.
- The pool report carries the spare pod names, and the dashboard shows spares per pool and
  retiring workers per router (ADR-043).

## Consequences
- ✅ Spares follow the pool as it scales, restarts and drains, with no router config
  changes. The router needs no Kubernetes access.
- ✅ The same label is what a Dynamo `WorkerFilter` would read once placement moves into
  Dynamo's router (docs/13 §4).
- ⚠️ Routers see changes up to one refresh late (5 s), plus the controller's reconcile
  delay. In that window a new pod gets no traffic, and a pod that's gone gets errors
  until retired.
- ⚠️ Routed pods must serve the OpenAI-compatible API on the configured port, for example
  through a per-worker frontend. Dynamo workers normally take requests over Dynamo's own
  request plane, so check this against the deployment (needs a cluster).
- ⚠️ All discovered workers get the same `slots` and `kv_blocks`. A pool is one model on
  one GPU class, so that holds, but mixed pools need a router each.
