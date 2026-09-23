# ADR-005: Disaggregated prefill/decode by default for long-context models

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
Chunked prefill reduces, but does not eliminate, decode stalls caused by long prompts
from other tenants. Long-context and agentic reservations are exactly the workloads that
cause them.

## Decision
Pools that serve reservations with declared p95 input > 8K tokens, or context ceiling
> 32K, use Dynamo **disaggregated serving**. Prefill and decode run in separate worker
pools, KV moves over NIXL, and **conditional disaggregation** keeps short prompts local.
Other pools use aggregated serving with chunked prefill.

## Consequences
- ✅ Interference mechanism #1 (long prompts stall decode) is removed structurally.
- ✅ Prefill and decode capacity scale independently, matching the WU components.
- ⚠️ Requires RDMA-capable fabric and topology-aware placement (same NVLink or IB domain).
- ⚠️ KV transfer adds latency and cost for mid-length prompts. The conditional threshold
  is tuned per profile.
