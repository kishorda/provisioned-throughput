# ADR-025: Run the interference suite against a contention model, with a control run

- **Status:** Accepted. Implements docs/05 §7.
- **Date:** 2026-09-24

## Context
Docs/05 §7 requires every release of the router, engine, or profile to pass a
mixed-tenant soak: tenant A's SLO must hold next to long prompts, KV hogs, and a PAYG
flood. There's no GPU or Dynamo on the development machine, and the existing mock engine
served every request at a fixed TTFT and TPOT, so tenants couldn't interfere at all. A
suite built on it would pass whatever the isolation code did. The options were:

- **Contention in the mock, full stack.** Model continuous batching in the mock, and run
  real clients through the gateway and router.
- **Pure simulation.** Drive the scheduler and dispatcher with virtual time. Fast and
  deterministic, but it skips the gateway, HTTP, and the release guards.
- **Wait for a staging pool.** Realistic, but nothing would gate changes until then.

## Decision
- **The mock models continuous batching** (`pt_mock_engine::contention`, enabled with
  `MockConfig.contention` or `MOCK_CONTENTION=1`). One scheduler task runs the batch in
  steps. A step costs `step_base + step_per_seq × decoding + prefill tokens ÷ prefill
  rate`, and every decoding sequence gets one token per step. Without chunked prefill a
  new prompt is prefilled in one step, which stalls everyone's decode. When the KV cache
  overflows, the newest sequence is evicted and recomputes. These are the three
  interference paths in docs/05. Every constant is a placeholder, not a calibration.
- **Full stack, twice per scenario** (`crates/pt-router/tests/interference.rs`). The
  *protected* run goes clients → gateway → router → engine. The *control* run sends the
  same load straight to an engine without isolation. A must meet its tier in the
  protected run, and must *miss* it in the control run. The control proves the scenario
  really interferes, so a passing suite means something.
- **Scenarios.** In each one, A is steady in-shape Interactive chat at 90% of its
  entitlement, so the gateway doesn't throttle it:
  - Long prompts: B sends 16K-token prompts. This is 128K scaled to the mock's prefill
    rate.
  - KV hog: C holds 8K-token sequences for 300 tokens, under a 25% KV share.
  - PAYG flood: three times the pool's slots.
- **Two lengths.** Each scenario runs for 3 s in `cargo test`. The `soak_*` tests are
  `#[ignore]`d and run for `PT_SOAK_SECS` (default 60) as the release gate. Latency is
  measured at the client.

## Consequences
- ✅ The suite found a real gap on its first run. With no failover, PAYG could fill every
  floor slot, and A waited about 1.9 s for a slot. The fix is ADR-026.
- ✅ Every isolation mechanism the suite relies on runs as it would in production:
  admission, strict priority, WFQ, KV budgets, pull-based dispatch, and release guards.
- ⚠️ The mock is a model, not an engine. Passing the suite shows that the router and
  gateway isolate *given* this engine behaviour. The staging-pool soak on real Dynamo
  workers (docs/05 §7) is still needed, with profiles calibrated per model and GPU.
- ⚠️ The suite is timing-based. Margins are wide (for example, a PAYG-flood TTFT p95 of
  about 65 ms protected against 800 ms allowed), but a heavily loaded CI machine could
  still flake. The scenarios run one at a time for this reason.
- ⚠️ Iteration-level engine priority and KVBM offload aren't modelled, because the engine
  patch doesn't exist yet (docs/05 §4). The KV-hog protection tested here is the router's
  per-reservation KV budget.
