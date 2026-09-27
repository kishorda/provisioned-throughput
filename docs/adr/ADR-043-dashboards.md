# ADR-043: Dashboards served by the control plane, fed by component reports

- **Status:** Accepted
- **Date:** 2026-09-27

## Context
Operators had no single view of the system. Region health, incidents, sales holds and
capacity lived in separate internal endpoints. Replica counts per model were only in each
cluster's `ModelPool.status`, and router worker load only at each router's
`/v1/router/status`. Customers could query usage, SLA reports and invoices through the API
(docs/09 §3), but had no page to look at them. The control plane knows reservations,
capacity and usage, but not what is actually running in each cluster.

## Decision
- **Hosting.** The control plane serves two static pages, embedded in the binary
  (`include_str!`), with no external scripts, fonts or build step:
  - `/internal/dashboard` for operators, with a System tab and a Customers tab;
  - `/dashboard` for customers.
  They call JSON endpoints with a bearer key the user enters. The key is kept in
  `sessionStorage` and forgotten when the tab closes. Each page sits on its listener
  surface (ADR-034): the operator page on the internal listener, the customer page on
  the customer listener.
- **Component reports.** Regional components push their state with the region token.
  - The capacity controller sends a `PoolReport` after every reconcile
    (`POST /internal/v1/reports/pools`). It holds the spec, the planned status, conditions,
    and Ready worker pods per role counted from pod readiness.
  - Routers with `[report]` send their `/v1/router/status` every `interval_secs`
    (`POST /internal/v1/reports/routers`).
  - Reports are soft state in the store (`component_reports`, keyed by kind, region and
    id), so every control-plane instance sees them (ADR-023). Each report replaces the
    reporter's previous one. The dashboard marks pools stale after 10 minutes and routers
    after 60 seconds. The leader prunes reports older than 24 hours.
  - Reporting is best effort: a failure is logged and never blocks a reconcile or a
    request.
- **System view** (`GET /internal/v1/dashboard/system`, operator key):
  - control plane: entitlement version, leader lease, signing key;
  - regions: health, gateways, snapshot keys, open incidents, sales holds;
  - capacity per region and model: replicas reserved of the pool, CUs sold, free CUs per
    tier, scheduled arrivals;
  - pools and replicas (desired, ready, floor, minimum available, spares, drains,
    conditions);
  - routers and their workers (slots, KV, backfill, hot spares);
  - **alerts**, computed from all of the above and sorted by severity. An alert is
    critical when: there's no leader; a region is down; capacity is oversold; a pool isn't
    Ready, is short of capacity, or has fewer ready replicas than must stay available.
    Reservations below the SLA commitment month to date are warnings.
- **Customer usage** (`GET /v1/dashboard/usage` with the tenant's admin key, and
  `GET /internal/v1/dashboard/usage/{tenant}` for operators) shows for each reservation:
  - the usage report over a chosen window (1 hour to 35 days, at most 300 points);
  - SLA attainment and credit month to date;
  - a resize recommendation from the Quote API;
  - invoices.
  A customer only ever sees their own reservations.

## Consequences
- ✅ One page answers "is the system healthy, and what is running where", and customers
  get a view of what they bought and how it's performing, with no extra service to
  deploy.
- ✅ The dashboard reuses the same rules as the API (usage, SLA, billing and quote
  modules), so the numbers match what customers are billed on.
- ⚠️ Reports show the latest state, not history. Trends for replicas and worker load
  belong in Prometheus (docs/09 §2).
- ⚠️ Views are computed on each request, and the SLA month to date reads a month of
  usage for each reservation. That's fine for tens of reservations per customer. A large
  fleet needs cached rollups.
- ⚠️ Replica counts are only as fresh as the last reconcile (at most 5 minutes when idle,
  15 seconds while draining).
