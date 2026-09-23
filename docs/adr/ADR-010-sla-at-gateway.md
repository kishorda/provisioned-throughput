# ADR-010: SLA latency measured at the regional gateway

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
Engine-measured latency hides queueing and routing. Client-measured latency includes the
customer's network and application behaviour. The SLA needs one precise, reproducible
definition.

## Decision
Measure TTFT and TPOT at the **regional PT Gateway**: request fully received → first byte
sent, and inter-token time to the last byte. Measure p95 per 5-minute window per deployment,
over eligible requests (in-shape, class `provisioned`). Exclusions are published. Customers
get the same raw data we use to compute attainment.

## Consequences
- ✅ Covers everything we control, including admission, routing, and engine queueing.
- ✅ Reproducible: customers can reconcile using `x-pt-*` headers and the telemetry API.
- ⚠️ Does not cover last-mile network latency. Documented, with per-request
  gateway-timing headers so customers can see the split.
