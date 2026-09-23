# ADR-004: Three-level fairness: gateway, router, engine

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
Noisy-neighbour effects happen at different timescales. Rate abuse happens over seconds,
batch-slot contention over milliseconds, and KV and prefill interference per iteration.
No single layer can handle all three.

## Decision
- **Gateway:** WU token bucket per reservation (rate).
- **Router:** strict priority classes (`provisioned > burst > spillover > payg`), and WFQ
  weighted by WU within a class. Pull-based dispatch on worker credits keeps engine queues
  shallow.
- **Engine:** priority passthrough, per-tenant KV budgets, chunked prefill, and KVBM
  offload instead of recompute.

## Consequences
- ✅ Each interference mechanism in the blog has a specific control.
- ✅ Ordering decisions happen where tenant context exists (the router), not by FIFO inside
  the engine.
- ⚠️ Requires extending Dynamo's router and a small engine adapter. See ADR-008 for
  upstream strategy.
- ⚠️ Pull-based dispatch can lower peak engine batch size slightly. Credits are tuned per
  profile.
