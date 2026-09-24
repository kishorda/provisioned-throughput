# ADR-011: One customer-facing Provisioned Throughput resource

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
The data model in doc 03 separates reservations (capacity and commitment) from
deployments (endpoints, keys, boundary policy). Exposing both makes customers manage two
lifecycles to get one working endpoint.

## Decision
The control-plane API exposes a single **Provisioned Throughput** resource per model.
Creating it reserves capacity and creates one deployment, with an endpoint per region and
an inference API key. Internally the reservation and deployment stay separate, so more
deployments per reservation can be added later without breaking the API.

Commercial rules apply at the resource level:
- `DELETE` mid-term ends the reservation at `term_end`. It's immediate only before the term starts.
- CU increases apply now. Decreases, tier changes, and region changes apply at renewal.

## Consequences
- ✅ One call gets a customer from nothing to a working endpoint.
- ✅ The commitment model is enforced in one place.
- ✅ Several deployments can share one reservation (for example, prod and staging keys),
  each with an optional cap (docs/12 §3). The single create call still gives one endpoint.
- ✅ The inference key is shown once. Customers rotate keys with a grace period, or revoke
  them (docs/12 §3).
