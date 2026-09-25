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
    Throughput for a model (docs/12), on a durable SQL store, with quotes, monthly
    invoices, and usage and SLA reports.
  - Signed entitlement snapshots that gateways follow, and a Quota Coordinator that
    shares each entitlement across gateway replicas (active/standby).
  - The tenant-aware router tier (docs/13).
  - Automatic region failover, warm spares, and share rebalancing between regions
    (docs/07).
  - An interference test suite that checks tenant isolation against a mock engine that
    models continuous batching (docs/05 §7).

## Crates

| Crate | What it does |
|-------|--------------|
| `pt-core` | Shared types: WU cost model and `PerformanceProfile`, tiers and CU pricing, workload shape, token counting, usage records |
| `pt-admission` | Debt-based WU bucket (ADR-002), burst bank, boundary-policy chain (burst → queue → spillover → reject), `continuation` reserve, output-length estimator |
| `pt-tokenize` | Input token counting: each model's `tokenizer.json` (HF `tokenizers`, pure Rust), a per-message cache, an inline byte budget with background fill, and learned bytes-per-token ratios (ADR-028) |
| `pt-gateway` | OpenAI-compatible gateway (axum): auth, estimate, admit, proxy/stream, settle, usage JSONL, `/v1/pt/status`. Loads entitlements from signed control-plane snapshots or a static file |
| `pt-router` | Tenant-aware router tier (docs/13): priority classes, WFQ by WU, pull-based dispatch on worker slots and KV, dedicated placement, per-reservation KV budgets, prefix and session affinity, hot spares with PAYG preemption during failover |
| `pt-quota` | Regional Quota Coordinator: leases that split each reservation's entitlement across gateway replicas without overselling (ADR-012), active/standby on a Kubernetes Lease (ADR-027) |
| `pt-telemetry` | Customer usage, latency, session, and monthly SLA reports built from gateway usage records (docs/09 §5), served by the control plane |
| `pt-entitlement` | Snapshot format shared by the control plane and gateways, Ed25519 signing and verification, API-key hashing |
| `pt-mock-engine` | Stand-in for a Dynamo frontend: OpenAI chat API with configurable TTFT/TPOT, simulated prefix caching, and an optional continuous-batching contention model |
| `pt-crds` | Custom resources (docs/08 §2): `PerformanceProfile`, `ModelPool`, `PoolAllocation`, `CapacityReservation`, and the `crdgen` binary |
| `pt-control-plane` | Customer REST API (docs/12): create, get, list, update, and delete Provisioned Throughput; commercial rules; capacity checks; renewal lifecycle; durable SQL store (CockroachDB or PostgreSQL) or in-memory |
| `pt-election` | Active/standby leader election on a Kubernetes Lease (or in memory), shared by the Quota Coordinator and the controller (ADR-027, ADR-029) |
| `pt-operator` | Regional Capacity Controller: sizes each `ModelPool` from its allocations (docs/06 §2), applies a `DynamoGraphDeployment` and per-role PodDisruptionBudgets, loads warm spares for failover demand from the regional snapshot, and reports status |

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
curl http://127.0.0.1:8080/internal/v1/tokenizers     # how each model's tokens are counted
```

Input tokens are counted with each model's own `tokenizer.json`, listed under
`[[tokenization.tokenizers]]` in the gateway config (ADR-028). Counts are cached per
message, so an agent's repeated context costs a hash lookup. New text beyond
`inline_bytes` (4 KB) is estimated by a learned bytes-per-token ratio, then tokenized in
the background. Models without a tokenizer use a ratio learned from the engine's counts.
The gateway passes its count to the router in `x-pt-prompt-tokens`.

Mock engine settings: `MOCK_ADDR`, `MOCK_NAME`, `MOCK_TTFT_MS` (default 50),
`MOCK_TPOT_MS` (10), `MOCK_OUTPUT_TOKENS` (64). `MOCK_TOKENIZER=path/to/tokenizer.json`
counts prompt tokens with a real tokenizer. `MOCK_CONTENTION=1` replaces the fixed
timings with a continuous-batching model, so concurrent requests slow each other down.

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
# Durable: uncomment [store] in the config, or set PT_DATABASE_URL=postgres://user@host:5432/db.
# Migrations are applied at startup, and state survives restarts (ADR-017).

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

Rotate inference keys without downtime (docs/12 §3):

```sh
curl -s -X POST http://127.0.0.1:8090/v1/provisioned-throughput/<id>/keys/rotate \
  -H 'Authorization: Bearer sk-admin-acme-dev' -d '{"grace_minutes":60}'   # new api_key; old one works for 60 min
curl -s http://127.0.0.1:8090/v1/provisioned-throughput/<id>/keys -H 'Authorization: Bearer sk-admin-acme-dev'
curl -s -X DELETE http://127.0.0.1:8090/v1/provisioned-throughput/<id>/keys/<key_id> -H 'Authorization: Bearer sk-admin-acme-dev'
```

Add deployments that share the reservation, each with its own keys and an optional cap (docs/12 §3):

```sh
curl -s -X POST http://127.0.0.1:8090/v1/provisioned-throughput/<id>/deployments \
  -H 'Authorization: Bearer sk-admin-acme-dev' -d '{"name":"staging","max_share":0.2}'
# staging's own api_key; at most 20% of the entitlement; 429 deployment_cap_exhausted above it
```

Region incidents open automatically when a region's gateways stop reporting serving
(heartbeats), and close once the region has served for 60 s. Snapshots then activate
failover entitlements in the paired region, and `/internal/v1/steering` gives DNS weights
(docs/07 §4, ADR-014). Operators can also declare incidents, for example to drain a
region. The SLA report excludes both (docs/09 §5):

```sh
curl -s -X POST http://127.0.0.1:8090/internal/v1/incidents -H 'Authorization: Bearer sk-operator-dev' \
  -d '{"region":"eu-west","description":"Network partition in eu-west-1a"}'
curl -s -X POST http://127.0.0.1:8090/internal/v1/incidents/<id>/resolve -H 'Authorization: Bearer sk-operator-dev'
curl -s http://127.0.0.1:8090/internal/v1/regions  -H 'Authorization: Bearer sk-operator-dev'   # health from heartbeats
curl -s http://127.0.0.1:8090/internal/v1/steering -H 'Authorization: Bearer sk-operator-dev'   # DNS weights
```

Invoices are monthly and in arrears (docs/12 §7, ADR-018). They cover reservation fees
prorated by the second, spillover at the model's PAYG price, and SLA credits. The current
month is a draft. Each month is finalised and stored 48 hours after it ends:

```sh
curl -s http://127.0.0.1:8090/v1/invoices -H 'Authorization: Bearer sk-admin-acme-dev'           # finals + drafts
curl -s http://127.0.0.1:8090/v1/invoices/2026-10 -H 'Authorization: Bearer sk-admin-acme-dev'   # one month
```

Reservations with shares in several regions are rebalanced automatically. When demand
doesn't match the split, the control plane moves the effective split (`effective_regions`)
up to 20% toward where traffic is. The contract and price stay the same. Opt out with
`"rebalance": false` (docs/07 §3, ADR-024).

Quotes size a reservation before buying, or recommend a resize from real usage (docs/02 §5):

```sh
curl -s -X POST http://127.0.0.1:8090/v1/quotes -H 'Authorization: Bearer sk-admin-acme-dev' \
  -d '{"model":"llama-4-maverick","requests_per_minute":600,
       "shape":{"input_p95":4000,"input_max":16000,"output_p95":400,"context_ceiling":32768,
                "cache_hit_ratio":0.5,"burst_factor":2}}'
# per region and tier: recommended and peak CUs, "1 CU ≈ … TPM", price, SLO, feasibility
curl -s -X POST http://127.0.0.1:8090/v1/quotes -H 'Authorization: Bearer sk-admin-acme-dev' \
  -d '{"from_reservation":"<id>","lookback_days":7}'     # resize recommendation
```

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

In production, the coordinator runs as two replicas that elect a leader through a
Kubernetes Lease (`[election]` in its config, and `deploy/quota/quota.yaml`). Gateways
list every replica in `coordinator_url` and `standby_urls`. A standby answers 503
`not_leader`, and `GET /leader` shows which replica leads. A clean shutdown hands over at
once. After a crash, the standby takes over within about 5 s, and gateways use their
fallback rate in the meantime (ADR-027). Without `[election]`, a single coordinator
warms up for 1.5 s after it starts, so it never grants on top of leases from before a
restart.

The gateway caches the last snapshot in `entitlements-eu-west.json`, and keeps serving from
it if the control plane is down. `pt-control-plane keygen` makes a new signing key pair and
prints its key id. To rotate without an outage, add the new public key to every gateway's
`extra_public_keys` (and the controller's `PT_SNAPSHOT_PUBLIC_KEY`, comma-separated),
restart the control plane with the new `signing_key`, then remove the old key once
`/internal/v1/regions` shows no gateway on its id (docs/12 §6, ADR-020). The keys in
`config/` are for development only.

## Tenant-aware router

`pt-router` sits between gateways and workers and decides which request runs next and where
(docs/13):

```sh
MOCK_ADDR=127.0.0.1:9000 MOCK_NAME=w0 target/debug/pt-mock-engine &
MOCK_ADDR=127.0.0.1:9002 MOCK_NAME=w1 target/debug/pt-mock-engine &
target/debug/pt-router config/router.toml &                      # :9100
# point the gateway's server.engine_url (and payg_engine_url) at http://127.0.0.1:9100
curl -s 127.0.0.1:9100/v1/router/status                          # queues, dispatches, per-worker load and KV
```

Responses carry `x-pt-router-worker` and `x-pt-router-queue-ms`. If the router can't
serve a request it returns `x-pt-reason`: `router_queue_timeout`,
`router_request_too_large`, or `router_no_worker`.

Workers marked `hot_spare` serve PAYG until a region failover needs them. Gateways mark
provisioned requests that use a failover entitlement with `x-pt-failover`. While that
marker keeps arriving, the router keeps new PAYG off hot spares. If provisioned work
waits 250 ms, it aborts running PAYG, which then gets `x-pt-reason: preempted` (503,
`Retry-After: 1`) or a final SSE error event (docs/13 §2, ADR-015). On other workers,
PAYG and spillover may hold at most `backfill_ratio` (0.5) of the slots and KV, so
provisioned work always finds room (ADR-026).

## Interference suite

A tenant's SLO must hold next to long prompts, KV hogs, and a PAYG flood (docs/05 §7,
ADR-025). The suite runs the gateway and router against a mock engine that models
continuous batching, and runs each scenario again with no isolation as a control, which
must miss the SLO:

```sh
cargo test -p pt-router --test interference -- --nocapture                        # 3 s per scenario
PT_SOAK_SECS=60 cargo test -p pt-router --test interference -- --ignored --nocapture  # soak
MOCK_CONTENTION=1 target/debug/pt-mock-engine &                                     # contention by hand
```

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
It runs as two replicas, and only the holder of the `pt-operator` Lease in `pt-system`
reconciles (ADR-029). Set `PT_LEADER_ELECTION=false` to run a single replica without the
lease, for example from a laptop against a test cluster.
For each `ModelPool` it:

- sets `desiredReplicas` = floor + `failureDomainK` + `maintenanceSlots` + `hotSpares`, per role
- sets a PodDisruptionBudget of `minAvailable` = floor + `failureDomainK` per role
- during a region failover, adds the active failover demand from the regional snapshot
  (set `PT_CONTROL_PLANE_URL`, `PT_REGION`, `PT_REGION_TOKEN`, `PT_SNAPSHOT_PUBLIC_KEY`,
  and `PT_SNAPSHOT_CACHE`, as in `deploy/operator/deployment.yaml`). Hot spares cover the
  first part, and warm spares load for the rest. `kubectl get ptpool -o yaml` shows
  `failoverWuPerSec`, `warmSparesLoaded`, and the `FailoverActive` condition (ADR-016)
- refuses to touch children, and sets `Ready=False`, when the profile is missing, the
  engine version doesn't match the profile, or a strict-dedicated pool enables PAYG backfill

## Test

```sh
cargo test --workspace          # unit tests, end-to-end gateway tests, CRD drift and example checks
cargo clippy --workspace --all-targets
cargo fmt --all --check
```

The interference soak (`--ignored`, above) is a release gate, not part of the default run.

## Not built yet

Follow-ups from the roadmap in docs/11:

- **Quota Coordinator on a cluster.** Active/standby election (ADR-027) is tested with an
  in-memory lease only. The Kubernetes Lease backend and `deploy/quota/` haven't run
  against a cluster. There's also no home-gateway routing for small tenants.
- **Chat templates.** Token counts use each model's tokenizer (ADR-028), but the chat
  template is approximated by a per-model `message_overhead`. Tool definitions and image
  parts aren't counted. Tokenizer files have to be shipped with the gateway.
- **Prefix-cache index at the gateway.** Estimates assume no cache hits; settlement
  refunds the difference.
- **Redpanda.** Gateways push usage to the control plane, which writes it to ClickHouse
  (ADR-019), or keeps it in memory when ClickHouse isn't configured. There's no Redpanda
  stage, and usage and SLA aggregation runs in Rust rather than ClickHouse SQL.
- **Region failover gaps.** Steering is an API; no GeoDNS/anycast controller consumes it.
  Router `hot_spare` flags are configured, not rendered by the capacity controller.
  There's no weight-prefetch DaemonSet, so loaded warm spares start cold. Preempted PAYG isn't metered.
  Failover activation needs the control plane.
- **Signing in a KMS.** The snapshot signing key is read from configuration.
- **Capacity planning by tier.** The shared planner counts CUs per region and model,
  regardless of tier. With the SQL store, run as many control-plane instances as you like
  (ADR-023). The in-memory store is single-instance.
- **Separate listeners.** With `[server.tls] client_ca`, the customer API also requires
  client certificates, because it shares the listener with internal traffic (ADR-022).
  Front the customer API with its own ingress. Gateway → Quota Coordinator is plain HTTP
  within a region.
- **Dynamo router extensions** (tenant WFQ, KV budgets) and the engine KV-budget adapter (P1).
- **Interference soak on real workers.** The suite runs against the mock's contention
  model (ADR-025). The staging-pool soak on Dynamo workers, and a scenario for PAYG
  preemption on hot spares, still need a GPU pool. The backfill ratio isn't tuned per
  model yet (ADR-026).
- **Controller gaps:** no drain workflow beyond PDBs, and no Dynamo Planner floor integration. The controller owns `replicas` on the
  DGD, so don't enable Planner autoscaling on PT pools yet.
- **Dynamo DGD schema check.** `crates/pt-operator/src/render.rs` follows Dynamo's
  `v1alpha1` examples. Verify service fields and worker flags against the Dynamo release
  you deploy. The rendering hasn't been run against a live cluster.
- **Operator image.** The Dockerfile hasn't been built here, because this machine has no Docker.
