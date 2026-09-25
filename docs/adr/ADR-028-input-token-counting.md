# ADR-028: Count input tokens with the model's tokenizer, cached per message, within a byte budget

- **Status:** Accepted. Implements docs/04 §2–3 and amends the N1 target.
- **Date:** 2026-09-25

## Context
The gateway prices a request before admitting it. Prefill is meant to be the exact part of
that estimate (docs/04 §3), but the gateway counted 4 bytes per token for every model.
That's close for English prose on BPE tokenizers. It's far off for code, CJK text, and
unusual tokenizers, and the error scales with prompt length, so long agentic prompts
were the least accurate. Settlement uses the engine's counts, so billing was always
right. The damage was to admission: bad estimates over- or under-admit until settlement
catches up.

docs/04 §2 budgeted "≤ 1 ms for 32K tokens" for tokenisation. Measured with Hugging Face
`tokenizers` 0.22 and GPT-2's `tokenizer.json`, in a release build on this machine:

| Uncached text | Time |
|---------------|------|
| 4 KB | about 2.7 ms |
| 16 KB | about 10 ms |
| 512 KB (135K tokens) | 320 ms whole, 194 ms split at words and encoded in parallel |

Pre-tokenization alone is half of the whole-string cost. The `onig` regex backend was
only about 15% faster. No library meets 1 ms for 32K new tokens. The options were:

- **Exact, cached per message.** Agents resend their whole conversation every turn, so
  almost every message has been counted before.
- **Exact up to a size, ratio beyond.** Bounded, but long prompts, where accuracy matters
  most, are never exact.
- **Learned ratio only.** Cheapest, no tokenizer files, but the per-request error stays.

## Decision
- **Exact counting with the model's `tokenizer.json`** (`crates/pt-tokenize`, HF
  `tokenizers` with the pure-Rust `fancy-regex` backend, no C build). A message costs its
  role and content tokens plus a per-model `message_overhead` for the chat template.
- **Cached per message**, keyed by a per-process random hash of (model, role, content).
  An agent's next turn only tokenizes its new messages. Bounded at `cache_entries`
  (100,000).
- **A byte budget before admission.** At most `inline_bytes` (4 KB, a placeholder) of
  uncached text is tokenized before admission, on the blocking pool. A new message that
  doesn't fit is estimated at the model's bytes-per-token ratio, learned from its own
  exact counts. It's then tokenized in the background, once, so the request that repeats
  it is exact. Text over 16 KB is split just before a space that follows a non-space and
  encoded in parallel.
- **Models without a tokenizer** are counted at a ratio learned from the prompt counts the
  engine reports (starting at 4 bytes per token).
- **One count downstream.** The gateway sends `x-pt-prompt-tokens`. The router uses it for
  the KV footprint and scales its per-prefix estimates to match. The mock engine can count
  with the same tokenizer (`MOCK_TOKENIZER`).
- **Accuracy is visible.** `GET /internal/v1/tokenizers` shows each model's method, its
  ratio, a moving average of engine tokens ÷ estimate, and how many messages were exact
  or estimated.
- **N1 is restated:** p99 < 2 ms of admission overhead for requests whose new content
  fits the inline budget. Repeated context costs a hash lookup.

## Consequences
- ✅ Most agentic requests are counted exactly for the cost of hashing their messages
  (about 0.2 ms for 512 KB).
- ✅ Admission latency is bounded whatever the prompt: about 2.5 ms of tokenizing at most.
- ✅ No tokenizer file is required. A model without one still gets a ratio learned from
  its engine.
- ⚠️ The first request carrying a long new message is estimated, not exact. The ratio is
  learned per model, so the error is usually small, but it's larger for text unlike the
  model's recent traffic.
- ⚠️ `message_overhead` approximates the chat template. Templates add fixed markers, and
  sometimes tokens for tools or images, which aren't counted. Calibrate it per model from
  `engine_to_estimate`, or render the template (for example with minijinja) later.
- ⚠️ Tokenizer files (1–10 MB each) must ship with the gateway: baked into the image or
  mounted from a volume. They exceed a ConfigMap's 1 MB limit.
- ⚠️ Each gateway replica keeps its own cache. Agents spread across replicas are tokenized
  once per replica, unless sessions are sticky.
