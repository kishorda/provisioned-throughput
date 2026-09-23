# ADR-002: Debt-based WU token bucket with settlement on actuals

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
Admission must decide before output length is known. Conservative estimates reject valid
traffic. Loose estimates let tenants overrun.

## Decision
Admit against a per-reservation token bucket denominated in WU, using an **estimate**:
exact prefill, and decode = `min(max_tokens, per-deployment P90 output)`. On completion,
settle `actual − estimate` into the bucket. The bucket may go **negative, down to a
bounded debt** (default: 2 s of entitlement). New requests are refused while in debt beyond
the bound. Settlement bias is monitored.

## Consequences
- ✅ Requests are admitted optimistically, so latency-sensitive traffic is not rejected
  because of pessimistic `max_tokens`.
- ✅ Long-run consumption converges to entitlement (N6), because debt is repaid from refill.
- ⚠️ Short-term overrun up to the debt bound. It is absorbed by pool `target_util` headroom.
- ⚠️ Requires per-deployment output-length sketches and a settlement path from workers.

## Alternatives rejected
- Reserve `max_tokens` up front: this over-rejects badly, because many clients set
  `max_tokens` to the maximum.
- Post-hoc only (no estimate): this cannot stop a burst of 200K-token prompts before damage.
