# ADR-030: Estimate cache hits from the gateway's own prefix history

- **Status:** Accepted. Implements docs/04 §3, where Dynamo KV events aren't available.
- **Date:** 2026-09-25

## Context
The WU formula prices cached prefill at `b` and uncached prefill at `a`. In the
development profiles, `b` is 8% of `a`. The gateway estimated every input token as
uncached. An agent resends its whole conversation each turn and the engine has nearly
all of it cached, so the estimate for an 80K-token turn was about ten times the real
prefill cost. Settlement refunded the difference, but only after admission had already
queued, throttled, or spilled the request against the debt bucket. Agentic traffic, the
product's main workload, suffered most.

docs/04 §3 designed a Bloom filter per pool fed by Dynamo KV events over NATS. That
depends on a Dynamo integration that can't be built here, and it also answers
"cached anywhere in the pool" rather than "cached for this tenant".

## Decision
- **The gateway remembers the prefixes it sent** (`pt_admission::PrefixCache`). Each
  message boundary gets a cumulative hash over the reservation id and every message up to
  that point. They're recorded when the engine accepts a request, because it has then
  prefilled them. Up to 200,000 prefixes are kept for 10 minutes. The hash keys are random
  per process.
- **Expected cached tokens** are the tokens of the longest remembered prefix (never the
  last prompt token, which engines always compute) times a **hit rate learned per model**.
  The hit rate is the engine's reported cached tokens ÷ the matched tokens, as a moving
  average. It starts at 0.5 (a placeholder, conservative because overestimating hits
  under-prices a request) and is replaced by the first observation. The estimate then
  prices those tokens at `b`.
- **Per reservation.** A prefix matches only the reservation that sent it, so one
  tenant's traffic never lowers another's estimate, even though the engine may share a
  common system prompt's cache between them. Cached tokens the gateway didn't predict are
  counted separately (`unpredicted_cached_tokens`) rather than learned.
- `GET /internal/v1/prefix-cache` shows the index size and each model's hit rate and
  totals. `[prefix_cache] enabled = false` turns it off.

## Consequences
- ✅ An agent's turns are estimated within a few percent of their settled WU once the hit
  rate is learned (tested end to end against the mock engine's prefix cache). Before,
  they were several times too high.
- ✅ It needs nothing from Dynamo. Hashing a 512 KB conversation takes a fraction of a
  millisecond, outside the index's lock.
- ✅ A wrong guess only changes the estimate. Settlement still uses the engine's counts.
- ⚠️ Each gateway replica knows only its own traffic. Without session affinity at the
  load balancer, an agent spread across replicas gets less credit. That shows up as
  `unpredicted_cached_tokens`.
- ⚠️ The hit rate is per model, not per pool or worker. A pool under memory pressure
  evicts more, and the average lags that by a few dozen requests.
- ⚠️ Overestimating hits under-prices a request, and the debt bucket absorbs it until
  settlement. The conservative start and the cap at the matched prefix bound this.
- Revisit the Bloom-filter design (KV events) when the Dynamo integration exists. It
  would see evictions directly.
