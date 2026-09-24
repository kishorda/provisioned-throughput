# 12 · Control-Plane API

> Decision records: [ADR-011](adr/ADR-011-single-pt-resource.md)

## 1. Scope

This is the customer API of the global control plane ([03 §2.1](03-system-architecture.md#21-global-control-plane)).
It lets a customer create, update, and delete Provisioned Throughput for a specific model.
It applies the commercial rules in [11 §4](11-roadmap-risks-open-questions.md#4-product-decisions).
Implementation: `crates/pt-control-plane`.

One **Provisioned Throughput** resource is a capacity reservation plus one data-plane
deployment (an endpoint per region and an inference API key). See [ADR-011](adr/ADR-011-single-pt-resource.md).

## 2. Endpoints

All requests use `Authorization: Bearer <tenant management key>`. Management keys are
separate from the inference keys that the gateway accepts.

| Method | Path | Success | Notes |
|--------|------|---------|-------|
| `GET` | `/v1/models` | 200 | Catalog: tiers offered and, per region, the longest context served |
| `POST` | `/v1/provisioned-throughput` | 201, or 200 on replay | Supports `Idempotency-Key`. Returns `Location`, `ETag`, and a one-time `api_key` |
| `GET` | `/v1/provisioned-throughput` | 200 | `?model=` filter. `?include_inactive=true` includes ended and cancelled reservations |
| `GET` | `/v1/provisioned-throughput/{id}` | 200 | Returns an `ETag` |
| `PATCH` | `/v1/provisioned-throughput/{id}` | 200 | Supports `If-Match`. Only the fields sent change |
| `DELETE` | `/v1/provisioned-throughput/{id}` | 202 mid-term, or 200 | Supports `If-Match` |
| `GET`, `POST` | `/v1/provisioned-throughput/{id}/deployments` | 200, 201 | List, or add a deployment `{"name", "max_share"?}`. Returns its key once |
| `GET`, `PATCH`, `DELETE` | `/v1/provisioned-throughput/{id}/deployments/{deployment}` | 200 | `PATCH {"name"?, "max_share"?}`; `"max_share": null` removes the cap |
| `GET`, `POST`, `DELETE` | `/v1/provisioned-throughput/{id}/deployments/{deployment}/keys`, `…/keys/rotate`, `…/keys/{key_id}` | 200 | The key endpoints below, for one deployment |
| `POST` | `/v1/provisioned-throughput/{id}/keys/rotate` | 200 | Returns a new inference key once. Body `{"grace_minutes"}` is optional. Supports `If-Match` |
| `GET` | `/v1/provisioned-throughput/{id}/keys` | 200 | Key metadata: id, prefix, created, expires. Never secrets or hashes |
| `DELETE` | `/v1/provisioned-throughput/{id}/keys/{key_id}` | 200 | Revoke a rotated-out key now. Supports `If-Match` |
| `POST` | `/v1/quotes` | 200 | CUs needed for a shape, trace, or existing reservation. See [02 §5](02-capacity-unit-and-cost-model.md#5-sizing--quote-api) |
| `GET` | `/v1/provisioned-throughput/{id}/usage`, `/sla`, `/sessions/{session_id}` | 200 | Telemetry. See [09 §5](09-metering-observability-and-slas.md#5-implementation) |

Create request:

```json
{
  "name": "acme-agents-prod",
  "model": "llama-4-maverick",
  "tier": "agentic",
  "regions": [{ "region": "eu-west", "cus": 6 }, { "region": "us-east", "cus": 4 }],
  "sku": "regional",
  "isolation": "shared",
  "term_months": 3,
  "start_at": "2026-10-01T00:00:00Z",
  "auto_renew": true,
  "shape": { "input_p95": 12000, "input_max": 64000, "output_p95": 800,
             "context_ceiling": 128000, "cache_hit_ratio": 0.6, "burst_factor": 3.0 },
  "boundary_policy": { "spillover": true, "burst": { "max_credit_seconds": 60 } }
}
```

`sku`, `isolation`, `start_at` (default: now), `auto_renew` (default: true), and
`boundary_policy` are optional.

Errors use the same shape as the gateway:
`{"error": {"type", "code", "message", "field"?}}`.

| Status | `code` | When |
|--------|--------|------|
| 401 | `invalid_api_key` | Missing or unknown management key |
| 404 | `not_found` | Unknown id, or the id belongs to another tenant |
| 409 | `capacity_unavailable` | Not enough CUs in a region |
| 409 | `name_taken`, `idempotency_key_reused`, `no_next_term`, `inactive`, `concurrent_modification` | State conflicts |
| 412 | `version_mismatch` | `If-Match` doesn't match the current version |
| 422 | `invalid_request` (with `field`), `invalid_json`, `not_offered`, `shape_unsupported` | Validation |

## 3. Rules

**Create**
- The model must exist, the tier must be offered for it, and every region must offer it.
  Each region needs at least 1 CU. `multi_region` needs at least two regions, all in one
  data-residency zone.
- The shape must satisfy `input_p95 ≤ input_max ≤ context_ceiling ≤` the model's
  maximum, and each region's pools must serve `context_ceiling`.
- Capacity is reserved in every region at once, or not at all. For `multi_region`, that
  includes `failover_headroom`: in each region, the largest share that fails over to it
  ([07 §2](07-multi-region.md#2-reservation-geography)). Increases grow the headroom with
  them.
- The term runs from `start_at` for 1, 3, or 6 calendar months (UTC). If `start_at` is
  in the future (up to 90 days), the reservation is `scheduled`.
- The inference API key is returned once. Only its SHA-256 is stored.
- Names are unique per tenant among live reservations.

**Update**

| Change | When it takes effect |
|--------|----------------------|
| CU increase in existing regions | Now, after a capacity check. Charged pro rata for the rest of the term |
| CU decrease, adding or removing regions | At the next renewal (`pending_changes`) |
| Tier | At the next renewal |
| Shape | Now, after checking that the regions can serve it |
| Boundary policy, auto-renew, name | Now |

Before the term starts, every change applies immediately. A request that mixes an increase
with a region change is scheduled as a whole. Send the increase on its own to apply it now.

**Deployments**
- A reservation has 1–10 deployments, for example `prod` and `staging`. The first, named
  `default`, is created with the reservation and is the *primary*. The reservation-level
  key endpoints act on it.
- All deployments draw from the reservation's one entitlement and share its boundary
  policy and burst credit. Each has its own keys and name, unique within the reservation.
- `max_share` (0 < share ≤ 1) caps a deployment at that fraction of the reservation's
  entitlement at each gateway, so a noisy deployment can't starve the others. Requests
  over the cap get 429 `deployment_cap_exhausted`, whatever the boundary policy; the cap
  isn't extended by burst or spillover. A request the cap admits but the shared bucket
  rejects is refunded to the cap.
- Deleting a deployment revokes its keys with the next snapshot. The last deployment
  can't be deleted (409 `last_deployment`); delete the reservation instead.
- Usage reports can be filtered with `?deployment=`. The SLA stays per reservation.

**Inference keys**
- Every reservation has exactly one current key, shown once at creation or rotation. Only
  its SHA-256 is stored.
- **Rotate** issues a new current key. The old key keeps working for `grace_minutes`
  (default 60, at most 7 days), so clients can switch without downtime. `0` revokes it
  immediately, for a compromised key.
- At most two rotated-out keys stay live. Rotating again revokes the oldest.
- **Revoke** removes a rotated-out key now. The current key can't be revoked (409
  `current_key`); rotate with `grace_minutes: 0` instead, so a deployment is never left
  without a key.
- Snapshots carry each rotated-out key's hash and expiry. Gateways stop accepting it at
  that moment by their own clock, without waiting for the next snapshot. The lifecycle
  loop then removes the expired key and records a `key_expired` event.

**Delete**
- `scheduled` → `cancelled` immediately. Capacity is released and nothing is billed.
- `active` → `pending_cancellation` (202). The reservation keeps serving and billing until
  `term_end`, then ends. `PATCH {"auto_renew": true}` withdraws the cancellation.
- `pending_cancellation`, `ended`, `cancelled`: no change (idempotent).

**Lifecycle.** A background loop activates scheduled reservations, and renews or ends
reservations at `term_end`. Renewal applies `pending_changes`. If a scheduled region change
no longer fits the available capacity, the reservation renews unchanged and records a
`scheduled_change_failed` event. Missed renewals catch up.

```mermaid
stateDiagram-v2
  [*] --> scheduled: create (future start)
  [*] --> active: create
  scheduled --> active: start_at reached
  scheduled --> cancelled: DELETE
  active --> active: term_end with auto_renew
  active --> ended: term_end without auto_renew
  active --> pending_cancellation: DELETE
  pending_cancellation --> active: PATCH auto_renew true
  pending_cancellation --> ended: term_end
```

## 4. Concurrency and idempotency

- Every resource has a `version`, returned as the `ETag`. `PATCH` and `DELETE` with
  `If-Match` fail with 412 if the version has changed. Without `If-Match`, a write that
  races another write fails with 409 `concurrent_modification`.
- A `POST` with an `Idempotency-Key` returns the original resource with status 200 when
  replayed with the same body. It returns 409 when the same key is sent with a different body.
- Capacity changes are undone if the resource write fails, so capacity is never reserved
  for a change that wasn't saved.

## 5. Storage

`Store` and `CapacityPlanner` are traits ([ADR-017](adr/ADR-017-sql-control-plane-store.md)).

- **`SqlStore`** (`sql.rs`) is the durable store. It uses the Postgres protocol:
  CockroachDB in production, PostgreSQL for development. Configure it with `[store] url`
  or `PT_DATABASE_URL`. Pending migrations from `migrations/` are applied at startup.
- **`MemoryStore`** backs tests and runs without `[store]`. Its state is lost on restart.

The schema (`0001_initial.sql`) is in the SQL both databases share:

| Table | Holds |
|-------|-------|
| `provisioned_throughput` | One row per reservation. Nested values (regions, headroom, shape, policy, price) are JSONB. `version` is the ETag |
| `deployments` | Ordered deployments per reservation. Ordinal 0 is the primary |
| `api_keys` | SHA-256 hashes, never keys. At most one current key per deployment |
| `provisioned_throughput_events` | The append-only audit and billing trail |
| `idempotency_keys` | Keys older than 7 days are ignored and reusable |
| `region_incidents` | At most one open incident per region |

Partial unique indexes enforce live-name uniqueness and one open incident per region, even
under races the service can't see. An update is one transaction guarded by
`WHERE version = expected`. Reads are fallible: a store failure is 503 `store_unavailable`,
never an empty result. The snapshot endpoint returns 503, so gateways keep their last
snapshot. At startup, the planner's reserved capacity is rebuilt from live reservations.

## 6. Entitlement snapshots

This is the Entitlement Distributor from [03 §2.1](03-system-architecture.md#21-global-control-plane),
and it follows [ADR-007](adr/ADR-007-regional-static-stability.md). The shared format is in
`crates/pt-entitlement` and the gateway side is `crates/pt-gateway/src/sync.rs`.

```mermaid
sequenceDiagram
  participant CP as Control plane
  participant GW as Regional gateway
  participant C as Disk cache
  GW->>C: on start, load last-known-good snapshot and verify it
  loop long-poll
    GW->>CP: GET /internal/v1/entitlements/{region}, If-None-Match version, wait 30s
    alt entitlements changed
      CP-->>GW: 200 snapshot with ETag and x-pt-signature
      GW->>GW: verify Ed25519, check region and newer version, swap view
      GW->>C: write snapshot and signature
    else no change within the wait
      CP-->>GW: 304
    end
  end
```

- **Content.** The snapshot has every `active` or `pending_cancellation` resource with a
  share in the region: the region's CUs, tier, shape, the pool's profile name, and each
  deployment's API-key SHA-256 and boundary policy. Scheduled, ended, and cancelled
  resources are left out, so keys start working at `term_start` and stop at `term_end`.
- **Versioning.** Every committed change (API or lifecycle) bumps a version. The version
  starts from the clock in milliseconds, so it keeps increasing across restarts. The
  version is read before the data, so a change made during generation is never labelled
  as already seen. Gateways apply only newer versions for their own region.
- **Trust.** The control plane signs the exact body with Ed25519, and gateways hold only
  the public key. Pull tokens are per region, and a token for another region gets 403.
- **Gateway apply.** A new view is swapped in atomically. Each reservation's limiter is
  reconfigured in place, so the bucket level, debt, burst credit, and in-flight
  settlements carry over. Deployments keep their output estimators. Reservations whose
  profile the gateway doesn't know are skipped and logged.
- **Static stability.** A gateway serves its cached snapshot through control-plane
  outages and restarts. `GET /internal/v1/entitlements` on the gateway reports the
  version and `generated_at`, so staleness can be alerted on.
- **Latency.** A change reaches a connected gateway in one long-poll round trip.
  Measured locally, a create was served in under 0.3 s (target N3: under 60 s).
- **Failover.** Snapshots also carry each Multi-region reservation's dormant failover
  shares, and the region failures that activate them (`failovers`). Declaring or resolving
  an incident bumps the version ([07 §4](07-multi-region.md#4-region-failure-sequence-multi-region-sku)).

Internal endpoints for regions and operators:

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| `GET` | `/internal/v1/entitlements/{region}` | Region token | Signed snapshot, long-poll |
| `POST` | `/internal/v1/heartbeats` | Region token | Gateway liveness: `{"gateway_id", "serving", "snapshot_version"?}`, 204 |
| `GET` | `/internal/v1/regions` | Operator key | Region health from heartbeats: `serving`, `down`, or `unknown` |
| `GET` | `/internal/v1/steering` | Operator key | DNS weights per region and per reservation |
| `GET`, `POST` | `/internal/v1/incidents`, `…/{id}/resolve` | Operator key | Region incidents. Each has `source`: `operator` or `automatic` |

## 7. Invoices

One invoice per tenant per calendar month (UTC), in arrears
([ADR-018](adr/ADR-018-monthly-invoices.md)). The code is in `billing.rs` and `billing_api.rs`.

| Method | Path | Returns |
|--------|------|---------|
| `GET` | `/v1/invoices` | Final invoices, newest first, plus drafts for the current month and for the previous one until it's final |
| `GET` | `/v1/invoices/{period}` | One month (`2026-10`): the final invoice, or a draft. 404 for future months and for older months that were never finalised |

| Line | Amount |
|------|--------|
| `reservation_fee` | Monthly price × (seconds billable in the month ÷ seconds in the month), one line per rate. A full month bills exactly the monthly price |
| `spillover` | Tokens of spillover requests that ran × the model's PAYG price per million tokens (`[[payg_prices]]`), for input, cached input, and output. Burst is free |
| `sla_credit` | −(that month's fee for the reservation × the SLA credit %). Added once the month has ended |

The rate timeline comes from events (`Activated`, `CapacityIncreased`, `ChangeApplied`,
`Renewed`, `Ended`, `Cancelled`), which record the monthly price in force. Lifecycle events
carry the time they took effect, not when the loop ran. `finalize_grace_hours` (48) after a
month ends, the invoice is stored and never changes. Invoices show `subtotal`, `credits`,
and `total`, in minor units.

## 8. Not yet built

- A remote Capacity Planner client. The in-memory planner counts CUs per region and
  model, regardless of tier, and is rebuilt from the store at startup. It's per-process,
  so run one control-plane instance until planning moves into the database.
- TLS to the database.
- Rotating the snapshot signing key. Gateways trust one public key, so rotation needs
  support for more than one key.
- Invoice adjustments, taxes, and payment collection. Final invoices are immutable, but
  there's no adjustment line for corrections yet.
- Pricing for failover headroom. It's reserved but not billed (open question in
  [11 §4](11-roadmap-risks-open-questions.md#4-product-decisions)).

## Blog problems addressed
P4 (customer-facing unit), P5 (tier), P6 (term, renewal, re-rating at renewal), P12 (boundary policy), P19 (SLA credits applied on invoices). See [traceability](01-requirements-and-traceability.md#2-traceability-matrix).
