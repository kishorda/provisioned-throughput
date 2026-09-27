# ADR-035: Preempted PAYG responses report the tokens they delivered

- **Status:** Accepted. Completes [ADR-015](ADR-015-failover-payg-preemption.md).
- **Date:** 2026-09-26

## Context
During a region failover, the router aborts running PAYG requests on hot spares to make
room for provisioned traffic (ADR-015). A preempted request should be billed only for what
the customer received. But PAYG traffic doesn't pass through the PT gateway, which meters
provisioned traffic. It reaches the router from the PAYG front door, which meters from the
engine's final `usage`. A preempted stream never gets that final usage, so the front door
couldn't tell how much was delivered.

## Decision
The router reports delivery itself, in the response the front door already reads:

- **Streaming.** The router counts content chunks as they pass (one token per chunk, as
  engines stream them). A preempted stream ends with an SSE event carrying the `preempted`
  error and a `usage` object: `prompt_tokens` (the gateway's count from
  `x-pt-prompt-tokens`, or the router's estimate), `completion_tokens` delivered, and
  `total_tokens`.
- **Before the response started.** The 503 `preempted` body carries the same `usage`, with
  `completion_tokens: 0`, and the header `x-pt-delivered-tokens: 0`.
- **Billing rule.** PAYG metering bills `completion_tokens` from that usage and waives the
  prompt: the customer lost the work through no fault of their own.
- The router logs each preemption on the `pt_router::metering` target (request id, prompt
  and completion tokens). `/v1/router/status` totals `preempted_prompt_tokens` and
  `preempted_completion_tokens`.

## Consequences
- ✅ The PAYG front door can bill preempted requests correctly from the response alone,
  with no new pipeline.
- ⚠️ Counting chunks assumes one token per content chunk. Engines that batch several tokens
  into one chunk are undercounted, which is in the customer's favour.
- ⚠️ A non-streaming response preempted after its headers were sent ends with a broken
  body, not a usage object. The router's metering log still records it, with 0 delivered.
- ⚠️ The PAYG front door and its billing aren't in this repository. The rule above is its
  contract.
