# ADR-009: Rust for the gateway, router extensions, and controllers

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
The gateway sits on every request. It must tokenise, hash, and admit in under 2 ms at
p99, and stream long responses to many concurrent connections. Router extensions must link
into Dynamo's router. Controllers must be reliable, long-running processes.

## Decision
Use Rust for the PT Gateway (Pingora or hyper + tower), the Quota Coordinator (tonic +
openraft), the tenant-scheduler crate, the Kubernetes controllers (kube-rs), the planner
(good_lp + HiGHS), metering (rdkafka), and control-plane services (axum/tonic,
sqlx on CockroachDB). Use the HF `tokenizers` crate for native tokenisation. Python stays
where the ecosystem requires it: engine backends and calibration notebooks.

## Consequences
- ✅ Predictable tail latency with no GC pauses. Memory safety on an internet-facing hot path.
- ✅ Shares a language with Dynamo's core, so router extensions are native crates.
- ✅ One type system for CRDs, API schemas, and usage records.
- ⚠️ Smaller hiring pool, and longer onboarding than Go for Kubernetes operators.
  Mitigated by kube-rs maturity and internal training.
