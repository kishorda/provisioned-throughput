# ADR-001: Sell an abstract Capacity Unit backed by a calibrated Work Unit cost model

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
Tokens are not a fixed unit of GPU work. Prefill is compute-bound and decode is
bandwidth-bound. KV residency limits concurrency. Caching and speculative decoding change
the effective cost. The blog lists four options: RPM, raw TPM, weighted tokens, and an
abstract unit. The first three misprice heavy or light workloads or tie the unit to
specific hardware.

## Decision
Sell **Capacity Units (CU)**. 1 CU = a fixed rate of **Work Units (WU)/s** delivered at a
named SLO tier. The WU cost of a request is
`a·uncached_prefill + b·cached_prefill + c·decode·m + d·KV_token_seconds`, with coefficients
calibrated per (model, GPU, engine version, parallelism). Customers buy against a declared
workload shape. A Quote API translates CUs into approximate TPM for their shape.

## Consequences
- ✅ Price tracks real cost, including long-context KV pressure and cache savings.
- ✅ Entitlement does not depend on hardware, so pools can migrate without re-contracting.
- ⚠️ Customers must understand shape. This is mitigated by the Quote API, trace replay,
  and drift recommendations.
- ⚠️ Requires a calibration pipeline and drift monitoring as permanent investments.
- ⚠️ Coefficient changes must be versioned. A usage record carries the `profile_version`.
