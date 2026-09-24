# Provisioned Throughput

Architecture and code for a provisioned-throughput product for AI inference. Customers
reserve Capacity Units (CUs): a fixed rate of calibrated Work Units (WU) per second at a
named latency tier.

- **Design:** [`docs/`](docs/README.md). Start with the executive summary.
- **Code:** a Rust workspace with:
  - The P0 admission path (docs/04). The gateway counts input tokens, estimates WU, admits
    against a debt-based bucket with boundary policies, streams from the engine, settles on
    the engine's actual usage, and writes a usage record for every request.
  - The P1 custom resources and the Regional Capacity Controller (docs/06, docs/08).
  - The control-plane API that lets customers create, update, and delete Provisioned
    Throughput for a model (docs/12).

## Crates

| Crate | What it does |
|-------|--------------|
| `pt-core` | Shared types: WU cost model and `PerformanceProfile`, tiers and CU pricing, workload shape, token counting, usage records |
| `pt-admission` | Debt-based WU bucket (ADR-002), burst bank, boundary-policy chain (burst → queue → spillover → reject), `continuation` reserve, output-length estimator |
| `pt-gateway` | OpenAI-compatible gateway (axum): auth, estimate, admit, proxy/stream, settle, usage JSONL, `/v1/pt/status`. Loads entitlements from signed control-plane snapshots or a static file |
| `pt-quota` | Regional Quota Coordinator: leases that split each reservation's entitlement across gateway replicas without overselling (ADR-012) |
| `pt-telemetry` | Customer usage, latency, session, and monthly SLA reports built from gateway usage records (docs/09 §5), served by the control plane |
| `pt-entitlement` | Snapshot format shared by the control plane and gateways, Ed25519 signing and verification, API-key hashing |
| `pt-mock-engine` | Stand-in for a Dynamo frontend: OpenAI chat API with configurable TTFT/TPOT and simulated prefix caching |
| `pt-crds` | Custom resources (docs/08 §2): `PerformanceProfile`, `ModelPool`, `PoolAllocation`, `CapacityReservation`, and the `crdgen` binary |
| `pt-control-plane` | Customer REST API (docs/12): create, get, list, update, and delete Provisioned Throughput; commercial rules; capacity checks; renewal lifecycle; in-memory store plus a CockroachDB migration |
| `pt-operator` | Regional Capacity Controller: sizes each `ModelPool` from its allocations (docs/06 §2), applies a `DynamoGraphDeployment` and per-role PodDisruptionBudgets, and reports status |

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

## Control-plane API

```sh
target/debug/pt-control-plane config/control-plane.toml &   # :8090, in-memory state

curl -s -X POST http://127.0.0.1:8090/v1/provisioned-throughput \
  -H 'Authorization: Bearer sk-admin-acme-dev' -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1' \
  -d '{"name":"agents-prod","model":"llama-4-maverick","tier":"agentic",
       "regions":[{"region":"eu-west","cus":4}],"term_months":3,
       "shape":{"input_p95":8000,"input_max":16000,"output_p95":500,"context_ceiling":32768}}'
# 201 with the resource, an ETag, and a one-time api_key

curl -s -X PATCH http://127.0.0.1:8090/v1/provisioned-throughput/<id> \
  -H 'Authorization: Bearer sk-admin-acme-dev' -H 'If-Match: "1"' \
  -d '{"regions":[{"region":"eu-west","cus":6}]}'      # increase: applies now, prorated

curl -s -X DELETE http://127.0.0.1:8090/v1/provisioned-throughput/<id> \
  -H 'Authorization: Bearer sk-admin-acme-dev'         # 202: ends at term_end
```

See [docs/12](docs/12-control-plane-api.md) for all rules and error codes.

Usage and SLA reports, from gateways configured with `[usage_export]` (docs/09 §5):

```sh
curl -s "http://127.0.0.1:8090/v1/provisioned-throughput/<id>/usage?granularity=5m" -H 'Authorization: Bearer sk-admin-acme-dev'
curl -s "http://127.0.0.1:8090/v1/provisioned-throughput/<id>/sla?month=2026-10"   -H 'Authorization: Bearer sk-admin-acme-dev'
curl -s "http://127.0.0.1:8090/v1/provisioned-throughput/<id>/sessions/<session>"  -H 'Authorization: Bearer sk-admin-acme-dev'
```

## Control plane → gateway

Gateways configured with `[entitlements]` pull signed snapshots of their region from the
control plane and serve whatever the API has created (docs/12 §6):

```sh
target/debug/pt-control-plane config/control-plane.toml &        # :8090
MOCK_ADDR=127.0.0.1:9000 target/debug/pt-mock-engine &
MOCK_ADDR=127.0.0.1:9001 MOCK_NAME=payg target/debug/pt-mock-engine &
target/debug/pt-gateway config/gateway-eu-west.toml &           # :8081, syncs eu-west

# Create through the control plane; use the returned api_key at the gateway.
curl -s 127.0.0.1:8081/internal/v1/entitlements                 # version, generated_at, counts
```

With `[quota]` set (it is in `config/gateway-eu-west.toml`), gateway replicas share each
reservation's regional entitlement through the Quota Coordinator:

```sh
target/debug/pt-quota-coordinator config/quota.toml &           # :8095
curl -s 127.0.0.1:8095/v1/leases -H 'Authorization: Bearer quota-token-eu-west-dev'
# per reservation: entitlement, total granted (never above it), and each replica's grant
```

`/v1/pt/status` on a gateway shows `entitlement_wu_per_s` (the region's total),
`local_share_wu_per_s` (this replica's lease), and `quota`: `lease`, `fallback`,
`unleased`, or `disabled`. To run a second replica, copy the config with a different
`listen` port and `cache_path`.

The gateway caches the last snapshot in `entitlements-eu-west.json`, and keeps serving from
it if the control plane is down. `pt-control-plane keygen` makes a new signing key pair. The
keys in `config/` are for development only.

## Kubernetes

```sh
cargo run -p pt-crds --bin crdgen                        # regenerate deploy/crds/ after changing pt-crds
kubectl apply -f deploy/crds/                            # install the CRDs
kubectl apply -f deploy/operator/rbac.yaml
docker build -f deploy/operator/Dockerfile -t pt-operator:dev .
kubectl apply -f deploy/operator/deployment.yaml
kubectl create namespace pt-serving
kubectl apply -f deploy/examples/                        # profile, pool, allocation, reservation
kubectl get ptpool -n pt-serving                         # sizing and Ready condition
```

The controller needs Dynamo's `DynamoGraphDeployment` CRD (`nvidia.com/v1alpha1`) installed.
For each `ModelPool` it:

- sets `desiredReplicas` = floor + `failureDomainK` + `maintenanceSlots` + `hotSpares`, per role
- sets a PodDisruptionBudget of `minAvailable` = floor + `failureDomainK` per role
- refuses to touch children, and sets `Ready=False`, when the profile is missing, the
  engine version doesn't match the profile, or a strict-dedicated pool enables PAYG backfill

## Test

```sh
cargo test --workspace          # unit tests, end-to-end gateway tests, CRD drift and example checks
cargo clippy --workspace --all-targets
cargo fmt --all --check
```

## Not built yet

Follow-ups from the roadmap in docs/11:

- **Quota Coordinator high availability.** It's a single instance per region (ADR-012).
  While it's down, gateways fall back to 50% of each entitlement in total. There's also no
  home-gateway routing for small tenants.
- **Model tokenizer.** Input tokens are approximated (4 bytes per token). Settlement uses
  the engine's counts, so this affects only the admission estimate.
- **Prefix-cache index at the gateway.** Estimates assume no cache hits; settlement
  refunds the difference.
- **Redpanda/ClickHouse.** Usage goes to JSONL and/or the control plane's in-memory telemetry store (35-day retention). The SLA's failover and customer-change exclusions aren't applied yet.
- **Signing-key rotation.** Gateways trust a single snapshot public key.
- **Control-plane persistence.** The API keeps state in memory. The CockroachDB schema is in
  `crates/pt-control-plane/migrations/`, but there's no SQL store yet. The capacity planner
  is also in-memory and counts CUs per region and model, regardless of tier.
- **Dynamo router extensions** (tenant WFQ, KV budgets) and the engine KV-budget adapter (P1).
- **Controller gaps:** no leader election (run one replica), no drain workflow beyond
  PDBs, and no Dynamo Planner floor integration. The controller owns `replicas` on the
  DGD, so don't enable Planner autoscaling on PT pools yet.
- **Dynamo DGD schema check.** `crates/pt-operator/src/render.rs` follows Dynamo's
  `v1alpha1` examples. Verify service fields and worker flags against the Dynamo release
  you deploy. The rendering hasn't been run against a live cluster.
- **Operator image.** The Dockerfile hasn't been built here, because this machine has no Docker.
