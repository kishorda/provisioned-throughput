# Provisioned Throughput

Architecture and code for a provisioned-throughput product for AI inference. Customers
reserve Capacity Units (CUs): a fixed rate of calibrated Work Units (WU) per second at a
named latency tier.

- **Design:** [`docs/`](docs/README.md). Start with the executive summary.
- **Code:** a Rust workspace implementing the P0 admission path (docs/04). The gateway
  counts input tokens, estimates WU, admits against a debt-based bucket with boundary
  policies, streams from the engine, settles on the engine's actual usage, and writes a
  usage record for every request.

## Crates

| Crate | What it does |
|-------|--------------|
| `pt-core` | Shared types: WU cost model and `PerformanceProfile`, tiers and CU pricing, workload shape, token counting, usage records |
| `pt-admission` | Debt-based WU bucket (ADR-002), burst bank, boundary-policy chain (burst → queue → spillover → reject), `continuation` reserve, output-length estimator |
| `pt-gateway` | OpenAI-compatible gateway (axum): auth, estimate, admit, proxy/stream, settle, usage JSONL, `/v1/pt/status` |
| `pt-mock-engine` | Stand-in for a Dynamo frontend: OpenAI chat API with configurable TTFT/TPOT and simulated prefix caching |

## Run locally

```sh
cargo build --workspace

# Engines: provisioned pool on :9000, PAYG pool on :9001
MOCK_ADDR=127.0.0.1:9000 MOCK_NAME=main target/debug/pt-mock-engine &
MOCK_ADDR=127.0.0.1:9001 MOCK_NAME=payg target/debug/pt-mock-engine &

# Gateway on :8080, usage records to ./usage.jsonl
target/debug/pt-gateway config/gateway.toml &

curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'Authorization: Bearer sk-acme-dev' \
  -H 'x-pt-session-id: demo' \
  -H 'Content-Type: application/json' \
  -d '{"model":"llama-4-maverick","stream":true,"max_tokens":16,
       "messages":[{"role":"user","content":"Plan a trip to Lisbon"}]}'

curl http://127.0.0.1:8080/v1/pt/status -H 'Authorization: Bearer sk-acme-dev'
```

Mock engine settings: `MOCK_ADDR`, `MOCK_NAME`, `MOCK_TTFT_MS` (default 50),
`MOCK_TPOT_MS` (10), `MOCK_OUTPUT_TOKENS` (64).

### Request and response headers

| Header | Direction | Meaning |
|--------|-----------|---------|
| `Authorization: Bearer <key>` | request | Selects the deployment |
| `x-pt-session-id` | request | Agent session, recorded on usage and passed to the engine for cache affinity |
| `x-pt-priority: continuation` | request | Mid-chain agent call; may use the reserved share of burst credit |
| `x-pt-class` | response | `provisioned`, `burst`, or `spillover` |
| `x-pt-wu-estimate`, `x-pt-queue-ms`, `x-request-id` | response | Admission details |
| `Retry-After`, `x-pt-reason`, `x-pt-entitlement-remaining` | 429 response | Why the request was rejected and when to retry |

## Test

```sh
cargo test --workspace          # unit tests plus end-to-end gateway tests against the mock engine
cargo clippy --workspace --all-targets
cargo fmt --all --check
```

## Not built yet

These are follow-ups from the P0 plan in docs/11:

- **Quota Coordinator** (ADR-003). Each gateway currently enforces the full entitlement
  locally, so run one gateway replica per reservation until leases exist.
- **Model tokenizer.** Input tokens are approximated (4 bytes per token). Settlement uses
  the engine's counts, so this affects only the admission estimate.
- **Prefix-cache index at the gateway.** Estimates assume no cache hits; settlement
  refunds the difference.
- **Redpanda publisher and metrics.** Usage goes to JSONL, which the ClickHouse schema can ingest.
- **Entitlement snapshots** from the global control plane. Config is a local TOML file.
- **Kubernetes CRDs and controllers**, and Dynamo router extensions (P1).
