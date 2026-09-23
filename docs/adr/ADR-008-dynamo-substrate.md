# ADR-008: NVIDIA Dynamo as the serving substrate

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
We need KV-aware routing, disaggregated prefill/decode, multi-tier KV cache, fast KV
transfer, SLA-driven autoscaling, and support for multiple engine backends. Building
these ourselves would take years.

## Decision
Use **NVIDIA Dynamo** with its Kubernetes operator and Grove, on the KAI Scheduler. Dynamo
provides the KV-aware router, disaggregated serving, KVBM, NIXL, Planner, and backend
support for TRT-LLM, vLLM, and SGLang. PT-specific logic (tenant scheduling, KV quotas,
floors) goes in Rust extensions behind traits, maintained as a thin fork and contributed
upstream where possible.

## Alternatives considered
- **Raw vLLM/SGLang + custom router:** maximum control, but we would rebuild disaggregation,
  KV transfer, and KV tiering.
- **KServe:** strong model-serving lifecycle, but weaker on disaggregation and KV-aware
  routing for LLMs at this scale.
- **llm-d** (vLLM + Gateway API Inference Extension): a credible alternative with a similar
  architecture. We chose Dynamo for its engine-agnostic backends, NIXL, KVBM maturity on
  NVIDIA fabrics, and its Rust core, which matches our language choice. We will revisit
  at P2.

## Consequences
- ✅ Months, not years, to disaggregated, KV-aware serving.
- ⚠️ Vendor coupling to the NVIDIA ecosystem, and exposure to fast upstream API change.
- ⚠️ Fork maintenance cost until upstream plugin interfaces exist.
