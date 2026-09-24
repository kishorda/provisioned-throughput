# ADR-013: Tenant scheduling as a tier in front of Dynamo

- **Status:** Accepted. Refines [ADR-004](ADR-004-three-level-fairness.md) and [ADR-008](ADR-008-dynamo-substrate.md).
- **Date:** 2026-09-24

## Context
ADR-004 puts priority classes, WFQ by WU, and pull-based dispatch in "the router", and
ADR-008 planned to add them to Dynamo's KV router as a crate behind a trait. Dynamo's
router now has a plugin registry (a request classifier and `WorkerFilter` /
`WorkerScorer` / `WorkerPicker` selection policies). Those plugins choose a worker for a
request that's already been picked. They have no hook for choosing *which* request goes
next, which is what priority classes and WFQ need. Dynamo also can't be built or run on
the development machine.

## Decision
- Build tenant scheduling as a separate Rust tier, `pt-router`. It's OpenAI-compatible,
  sits between gateways and a pool's workers, and owns ordering and pull-based dispatch.
- Keep its placement logic (eligibility, KV budgets, scoring) as pure functions, so they
  can move into Dynamo `WorkerFilter`/`WorkerScorer` plugins without a rewrite.
- Deploy in three steps (docs/13 §4): a tier in front of Dynamo now, selection plugins
  next, and a proposed upstream ordering hook later.

## Consequences
- ✅ Ordering and fairness work today, without forking Dynamo, and are tested end to end.
- ✅ Placement code is ready to become Dynamo plugins.
- ⚠️ One more network hop per request until ordering moves into Dynamo.
- ⚠️ Until integrated, the tier's view of worker capacity is configured, not measured.
  Sizing each Dynamo pool as a "worker" keeps that coarse but safe.
- ⚠️ One router replica per pool until queue state is shared.
