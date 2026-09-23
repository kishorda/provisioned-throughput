# 03 · System Architecture

## 1. Context

```mermaid
flowchart LR
  cust[Customer apps / agents] -->|OpenAI-compatible API| pt[(Provisioned Throughput Platform)]
  portal[Customer portal / Terraform] -->|Reservations, deployments, quotes| pt
  pt -->|Usage| billing[Billing system]
  pt -->|Telemetry| custobs[Customer dashboards / metrics API]
  ops[SRE / capacity team] --> pt
  pt --> gpu[(GPU fleet: Kubernetes clusters in N regions)]
```

## 2. Two planes

```mermaid
flowchart TB
  subgraph GCP[Global control plane · active-active in 3 regions]
    RS[Reservation Service]
    CP[Capacity Planner]
    PR[Profile Registry]
    ED[Entitlement Distributor]
    QA[Quote API]
    DB[(CockroachDB)]
    RS --- DB
    CP --- DB
    PR --- DB
    QA --> CP
    RS --> CP
    CP --> ED
  end

  subgraph R1[Region A data plane]
    GW1[PT Gateway fleet]
    QC1[Quota Coordinator]
    RT1[Tenant-aware Router<br/>Dynamo KV router + extensions]
    subgraph C1[GPU cluster A1]
      P1[Prefill workers]
      D1[Decode workers]
      K1[KVBM · NIXL]
    end
    subgraph C2[GPU cluster A2]
      P2[Prefill workers]
      D2[Decode workers]
    end
    RCC1[Regional Capacity Controller]
    MET1[Metering: Kafka → ClickHouse]
    GW1 <--> QC1
    GW1 --> RT1 --> P1 & P2
    P1 -->|KV via NIXL| D1
    P2 --> D2
    GW1 --> MET1
    RCC1 --> C1 & C2
  end

  subgraph R2[Region B data plane]
    GW2[PT Gateway fleet]
    more2[...]
  end

  ED -->|signed entitlement snapshots| QC1
  ED --> GW2
  CP -->|PoolAllocation intents · gRPC| RCC1
  MET1 -->|aggregated usage| RS
```

### 2.1 Global control plane

Rust services (tokio + tonic/axum), deployed active-active in three regions on CockroachDB.
The global plane is **off the request path**.

| Component | Responsibility |
|-----------|----------------|
| **Reservation Service** | CRUD for tenants, reservations, deployments, and keys. Enforces terms (1, 3, or 6 months; minimum 1 CU) and increase-only resizes mid-term. Source of truth for entitlements. |
| **Quote API** | Sizing from a shape or trace ([02 §5](02-capacity-unit-and-cost-model.md#5-sizing--quote-api)). Calls the Planner for feasibility. |
| **Capacity Planner** | Sale-time feasibility. Places reservations into regional pools with N+k headroom. Periodic rebalance ([06](06-capacity-planning-and-reliability.md)). |
| **Profile Registry** | Versioned PerformanceProfiles; CU→replica conversion tables. |
| **Entitlement Distributor** | Builds signed, versioned entitlement snapshots per region. Regions pull them with watch and cache them as last-known-good. |
| **Usage Aggregator** | Receives regional hourly aggregates for billing and invoicing. |

### 2.2 Regional data plane

| Component | Tech | Responsibility |
|-----------|------|----------------|
| **PT Gateway** | Rust; Pingora (or hyper + tower) | TLS, auth, tenant/deployment resolution, tokenisation (HF `tokenizers` crate), prefix-block hashing, WU estimation, admission, boundary policy, streaming proxy, usage emission, SLA timestamps |
| **Quota Coordinator** | Rust; tonic; Raft-replicated (openraft) state | Splits each reservation's WU/s across gateway replicas with short leases; settles debt; enforces region share of multi-region entitlements ([ADR-003](adr/ADR-003-lease-based-distributed-quota.md)) |
| **Tenant-aware Router** | Rust crate extending Dynamo's KV router | Priority classes, WFQ by WU, pool placement constraints, pull-based dispatch, KV-overlap scoring ([05 §3](05-isolation-and-scheduling.md#3-level-2--router)) |
| **Dynamo serving graphs** | NVIDIA Dynamo + TRT-LLM / vLLM / SGLang | Disaggregated prefill/decode, KVBM (GPU→CPU→NVMe), NIXL KV transfer, etcd + NATS discovery and KV events |
| **Regional Capacity Controller** | Rust, kube-rs | Reconciles `ModelPool` / `PoolAllocation` CRDs into `DynamoGraphDeployment`s. Owns provisioned floors. Coordinates Dynamo Planner for PAYG. Runs the capacity-aware drain controller. |
| **Metering pipeline** | Redpanda/Kafka → ClickHouse; Rust aggregator | Per-request usage records; per-tenant metrics; hourly aggregates to the global plane |
| **Telemetry API** | Rust (axum) over ClickHouse + Prometheus | Customer-facing per-deployment metrics ([09](09-metering-observability-and-slas.md)) |

## 3. Regional deployment topology

```mermaid
flowchart TB
  LB[Regional L4 LB / anycast] --> GWs
  subgraph SYS[System cluster · CPU nodes]
    GWs[PT Gateway ×N · HPA on RPS & CPU]
    QC[Quota Coordinator ×3 · Raft]
    RCC[Regional Capacity Controller]
    KAF[Redpanda]
    CH[ClickHouse]
    PROM[Prometheus / Thanos]
  end
  subgraph G1[GPU cluster · H200 / B200 pools]
    FE1[Dynamo frontend + tenant router ×M]
    ETCD1[etcd] --- NATS1[NATS JetStream]
    PF1[Prefill pool]
    DC1[Decode pool]
    SP1[Hot spares / PAYG backfill]
  end
  subgraph G2[GPU cluster · GB200 NVL72 pools]
    FE2[Dynamo frontend + tenant router]
    PF2[Prefill] --> DC2[Decode]
  end
  GWs -->|gRPC / HTTP2, tenant metadata headers| FE1 & FE2
  RCC -->|kube API| G1 & G2
```

- The **system cluster** is separate from GPU clusters. That way, a GPU cluster upgrade
  never takes admission down.
- The gateway chooses the cluster (pool) from the reservation's `PoolAllocation`. The
  router inside the cluster chooses the worker.
- Traffic stays in-cluster from router to worker. KV transfer runs over RDMA (NIXL) inside
  one NVLink or InfiniBand domain.

## 4. Data model (core entities)

```mermaid
erDiagram
  TENANT ||--o{ RESERVATION : owns
  RESERVATION ||--o{ DEPLOYMENT : exposes
  RESERVATION ||--o{ REGION_SHARE : "split across"
  REGION_SHARE ||--o{ POOL_ALLOCATION : "placed on"
  POOL ||--o{ POOL_ALLOCATION : hosts
  POOL }o--|| PERFORMANCE_PROFILE : "calibrated by"
  DEPLOYMENT ||--o{ USAGE_RECORD : produces
  RESERVATION {
    uuid id
    string model
    int cus
    enum tier
    json shape
    enum sku "regional|multi_region|hw_pinned"
    date term_end
  }
  DEPLOYMENT {
    uuid id
    enum boundary_policy "reject|queue|burst|spillover"
    json burst_cfg
  }
  POOL_ALLOCATION {
    uuid pool
    float wu_per_s
    enum isolation "shared|dedicated"
  }
```

## 5. Key interfaces

| Interface | Protocol | Notes |
|-----------|----------|-------|
| Customer inference | HTTPS, OpenAI-compatible (`/v1/chat/completions`, `/v1/responses`) | Extra headers: `x-pt-deployment`, `x-pt-session-id`, `x-pt-priority` (intra-tenant) |
| Gateway → Router | HTTP/2 or gRPC | Adds `tenant`, `reservation`, `class`, `wu_estimate`, `prefix_hashes`, `deadline` |
| Gateway ↔ Quota Coordinator | gRPC streaming | Lease grant/renew, debt report, entitlement version |
| Global → Region | gRPC (mTLS) | Entitlement snapshots (pull + watch); `PoolAllocation` intents |
| Worker → Metering | NATS → Redpanda bridge | Actual token counts, KV-token-seconds, per-phase timings |

## Blog problems addressed
Structural basis for all problems. The system as a whole is the blog's "build product and engine together" (P20).
