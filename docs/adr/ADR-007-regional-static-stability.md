# ADR-007: Regional data planes are statically stable

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
The product is multi-region from v1. A global control plane outage must not take down
inference in any region.

## Decision
The global control plane (Reservation Service, Planner, Entitlement Distributor) is **off
the request path**. Regions pull signed, versioned entitlement snapshots and keep the
last-known-good copy. They also receive pre-distributed but dormant failover entitlements.
Regions serve for ≥ 24 h without global connectivity. Only sales, resizes, rebalancing,
and hourly billing export pause, and billing is buffered locally.

## Consequences
- ✅ Blast radius is regional. Global outages degrade only control operations.
- ✅ Data residency is simple: no prompts cross regions by default.
- ⚠️ Entitlement changes are eventually consistent (target < 60 s).
- ⚠️ Multi-region shares can drift from real demand during a global outage. The burst
  bucket absorbs short-term mismatch.
- ⚠️ Failover entitlements are activated by the control plane ([ADR-014](ADR-014-automatic-region-failover.md)).
  A region failure during a global outage doesn't activate them.
