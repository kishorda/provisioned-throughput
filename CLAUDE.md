# CLAUDE.md

## What this project is
This is the architecture design and code for a **Provisioned Throughput (PT)** product for AI inference, written from a principal-architect perspective. It answers the problems raised in the PM's blog post:
https://kishoraher.wordpress.com/2026/09/23/provisioned-throughput-for-ai-inference-why-just-reserve-some-capacity-is-harder-than-it-sounds/

**Current state:** design docs are complete. The Rust workspace implements:
- the **P0 admission path** (docs/04), run locally against a mock engine;
- the **P1 CRDs and the Regional Capacity Controller** (docs/06, docs/08), active/standby on a Kubernetes Lease (ADR-029);
- the **control-plane customer API** (docs/12) on a durable SQL store (`sql.rs`, CockroachDB or PostgreSQL, ADR-017) or in memory, running as several instances (ADR-023) and serving HTTPS with optional mTLS (ADR-022);
- **signed entitlement snapshots** that gateways follow (docs/12 §6), with key rotation (ADR-020);
- the **Quota Coordinator**, which shares entitlements across gateway replicas (ADR-012) and runs active/standby on a Kubernetes Lease (ADR-027);
- **customer usage and SLA telemetry** (docs/09 §5) fed by gateway usage export, with **usage records in ClickHouse** (ADR-019);
- the **Quote API** (docs/02 §5) and **monthly invoices** (ADR-018);
- the **tenant-aware router tier** `pt-router` (docs/13, ADR-013), with hot-spare preemption (ADR-015) and a backfill cap (ADR-026);
- **automatic region failover** (docs/07 §4, ADR-014 to ADR-016): gateway heartbeats, automatic incidents, reserved failover headroom, snapshot-activated failover entitlements, warm spares, and a DNS steering feed, plus **share rebalancing** between regions (ADR-024);
- the **interference test suite** (docs/05 §7, ADR-025) against a contention-model mock engine;
- **input token counting** with each model's tokenizer, cached per message within an inline byte budget (ADR-028).

The controller has only been unit-tested: there's no cluster, Docker, or kubectl on this machine. Dynamo integration is designed (docs/13 §3) but not built, because Dynamo can't be compiled or run here. `README.md` lists what's missing. The remote is `origin` = `git@github.com:kishorda/provisioned-throughput.git` (SSH). HTTPS has no credentials on this machine, and there's no `gh` CLI.

## Fixed decisions (don't re-litigate without the user)
- **Sellable unit:** an abstract **Capacity Unit (CU)** = a fixed rate of **Work Units (WU)** per second at a named SLO tier (Interactive / Agentic / Standard).
  WU = `a·uncached_prefill + b·cached_prefill + c·decode·m_decode + d·KV_token_seconds`. Coefficients come from a per-(model, GPU, engine version, parallelism) `PerformanceProfile`.
- **Footprint:** multi-cluster and **multi-region** from v1. The global control plane is off the request path, and regional data planes are statically stable.
- **Stack:** Kubernetes + **NVIDIA Dynamo** (KV router, disaggregated prefill/decode, KVBM, NIXL, Planner, operator, Grove), plus KAI Scheduler. **Rust** for the gateway (Pingora or hyper + tower), Quota Coordinator (active/standby on a Kubernetes Lease, soft state, HTTP/JSON for now: ADR-012, ADR-027), router extensions, kube-rs controllers, planner (good_lp + HiGHS), and metering.
- **Traffic classes (strict priority):** `provisioned > burst > spillover > payg`. Headroom is backfilled with preemptible PAYG.
- **SLA:** measured at the regional PT Gateway, as p95 per 5-minute window per deployment, counting in-shape provisioned traffic only.

## Layout
```
Cargo.toml                      # workspace
crates/
  pt-core/                      # WU cost model, PerformanceProfile, tiers + pricing, Shape, TermMonths, token counting, UsageRecord
  pt-telemetry/                 # usage store (trait, in-memory, clickhouse.rs, UsageBackend), usage.rs (series/summary/advice), sla.rs (windows, attainment, credits), sessions.rs, api.rs; Directory trait implemented by the control plane
  pt-router/                    # tenant scheduling tier: scheduler.rs (priority + SCFQ WFQ), workers.rs (eligibility, KV budget, backfill cap, scoring, prefix index), dispatch.rs (pure), http.rs (proxy + capacity guards); tests/interference.rs
  pt-quota/                     # Quota Coordinator: allocator.rs (pure max-min split), coordinator.rs (leases, never-oversell rule, terms + warm-up), election.rs (coordinator terms on pt-election), wire.rs (shared with gateway), api.rs
  pt-election/                  # leader election on a lease record: Elector, Leadership, run, lead, MemoryLease, kube_lease (feature kube), terminated()
  pt-entitlement/               # Snapshot format, Ed25519 SnapshotSigner/Verifier, sha256_hex for API keys
  pt-tokenize/                  # input token counting: HF tokenizers (fancy-regex, no C), per-message cache, inline budget + background fill, learned ratios
  pt-admission/                 # DebtBucket (ADR-002), BurstBank, ReservationLimiter (burst → queue → spillover → reject), OutputEstimator
  pt-gateway/                   # axum gateway: chat.rs (admission path), sse.rs, state.rs (swappable entitlements), sync.rs (snapshot long-poll + cache), config.rs, usage.rs; tests/gateway.rs, tests/entitlements.rs (control plane → gateway e2e)
  pt-mock-engine/               # OpenAI-compatible mock with TTFT/TPOT, simulated prefix cache, contention.rs (continuous-batching model)
  pt-crds/                      # kube-rs CRD types + crdgen binary; tests/manifests.rs checks deploy/ drift
  pt-operator/                  # capacity controller: sizing.rs, render.rs (DGD + PDB), plan.rs (pure), controller.rs (kube I/O), leader.rs (election settings from env)
  pt-control-plane/             # customer API: service.rs (rules), api.rs (HTTP), quote.rs + quote_api.rs (sizing), telemetry.rs (Directory impl), store.rs (trait + MemoryStore), sql.rs (SqlStore), planner.rs, migrations/ (CockroachDB/PostgreSQL)
config/gateway.toml             # example local config (stands in for the entitlement snapshot)
config/control-plane.toml       # tenants, model catalog, regional capacity, [[profiles]] (Profile Registry stand-in), region tokens, dev signing key; loaded by tests
config/gateway-eu-west.toml     # gateway that syncs eu-west snapshots and uses the quota coordinator
config/gateway-eu-central.toml  # eu-west's failover pair (single replica, no [quota])
config/quota.toml               # eu-west Quota Coordinator
deploy/quota/                   # coordinator StatefulSet (2 replicas), Lease RBAC, Dockerfile (untested)
config/router.toml              # pt-router in front of two local mock workers
deploy/crds/                    # GENERATED by `cargo run -p pt-crds --bin crdgen`; never hand-edit
deploy/examples/                # example CRs; parsed and planned in tests
deploy/operator/                # RBAC (incl. Lease Role), Deployment (2 replicas), Dockerfile (untested)
docs/
  README.md                     # index, executive summary, glossary, ADR table
  01-requirements-and-traceability.md   # blog problems P1–P20 → requirements → sections; NFRs N1–N10
  02 … 11-*.md                  # unit/cost model, system, request path, isolation, capacity,
                                # multi-region, K8s+Dynamo, metering/SLA, lifecycle, roadmap
  adr/ADR-001 … ADR-027-*.md    # Nygard format: Status, Date, Context, Decision, Consequences
```
Published summary page (private Artifact): https://claude.ai/artifact/HMSSEEU8fb8tWSrGE9NmSH
Its source HTML lived in a session scratchpad, not in this repo. To update it, republish with that URL after reading it.

## Conventions for code
- Keep admission logic pure and synchronous in `pt-admission`, taking `now: Instant` explicitly so tests are deterministic. The gateway owns async and I/O.
- Token counting (ADR-028): the gateway counts with `pt_tokenize::Tokenizers` (`AppState.tokens`), never `ApproxTokenCounter`. Uncached work runs on `spawn_blocking`. `count_within` tokenizes at most `inline_bytes` of uncached text, and `deferred` messages go to `fill` in the background (deduplicated through `filling`). Settlement calls `observe` with the engine's prompt tokens. The gateway sends `x-pt-prompt-tokens`, and the router rescales its prefix estimates to it. Keep the `tokenizers` crate on `default-features = false, features = ["fancy-regex"]`: the defaults build C/C++ (`onig`, `esaxx_fast`). A real-tokenizer check runs with `PT_TEST_TOKENIZER=<gpt2 tokenizer.json>` (`pt-tokenize/tests/real.rs`, `--release --nocapture` prints timings).
- Settlement and usage emission happen once, in `Settlement::drop` (`crates/pt-gateway/src/chat.rs`), so every exit path is covered, including client disconnects.
- Engines are reached over plain HTTP (`reqwest` with default features off). Don't add crates that need cmake or TLS C libraries: this machine has no cmake. Where TLS is needed, use rustls with the ring provider: sqlx `tls-rustls-ring-webpki`, reqwest `rustls-tls-webpki-roots` (pt-telemetry only), kube `ring`. Never aws-lc-rs or native-tls.
- Control-plane transport (ADR-022): `[server.tls]` serves HTTPS through `tls::TlsListener` (rustls with ring; handshakes run in their own tasks). ALPN must stay `http/1.1` only, because axum has no HTTP/2 here. Clients build reqwest through `pt_entitlement::client_tls::ControlPlaneTls` (feature `client`), which enforces the policy. Tests generate certificates with `rcgen`, so they always run.
- Database transport (ADR-021): `sql::check_transport` and `clickhouse::check_transport` refuse non-loopback hosts without verified TLS unless `allow_insecure_transport` is set. Keep that default when adding new outbound connections that carry tenant data.
- The machine has 4 cores and about 3 GB of RAM. Build with `CARGO_BUILD_JOBS=2` if the linker runs out of memory.
- Pricing values in code (`pt_core::tier`) must match the product decisions below.
- After changing any `pt-crds` type, run `cargo run -p pt-crds --bin crdgen`. Otherwise `committed_crds_match_generated` fails.
- Keep controller decisions in `pt-operator/src/plan.rs` (pure) and API calls in `controller.rs`. A pool that can't be reconciled keeps its existing children and never scales down.
- Everything specific to a Dynamo version (DGD field names, worker flags) lives in `pt-operator/src/render.rs`. Don't invent Dynamo flags: record intent as annotations instead.
- kube is feature-less at the workspace level. `pt-crds` uses `derive`; `pt-operator` uses `client, runtime, derive, rustls-tls, ring`; `pt-election` (feature `kube`) uses `client, rustls-tls, ring`. Don't use `aws-lc-rs`, which needs cmake.

- Control-plane rules live in `pt-control-plane/src/service.rs`. Validate before touching capacity. Record every planner call as a `PlanOp`, so a failed write undoes it. Take time from the injected `Clock`, never `Timestamp::now()` directly, so lifecycle tests can use `ManualClock`.
- `Store` and `CapacityPlanner` use `impl Future + Send` trait methods, so the service is generic rather than `dyn`. Store reads return `Result`. Never turn a store error into "not found" or an empty list: it becomes `ServiceError::Unavailable` (503), and snapshots must fail rather than publish empty entitlements.
- `SqlStore` (ADR-017): migrations in `migrations/` must run on both CockroachDB and PostgreSQL. Use `TEXT`, separate `CREATE INDEX`, `IF NOT EXISTS`, and no `@` index names, row-level TTL, or `REGIONAL BY ROW`. Never edit a shipped migration; add the next number and list it in `sql.rs` `MIGRATIONS`. When `ProvisionedThroughput` gains a field, add a column and extend `COLUMNS`, `bind_pt!`, and `pt_from_row` together. `SystemClock` truncates to microseconds to match TIMESTAMPTZ. At startup, `with_store` calls `restore_capacity`, which rebuilds the planner from live reservations.
- SLA exclusion windows are derived only in `pt-control-plane/src/telemetry.rs` (`exclusion_windows`), from reservation events and operator-declared `RegionIncident`s, and applied in `pt-telemetry/src/sla.rs`. When you add an event kind, decide whether it opens a grace window.
- SLA rules live only in `pt-telemetry/src/sla.rs`, and per-request latency targets only in `pt_core::Tier::{ttft_target_ms, tpot_target_ms}`. Keep them in line with docs/02 §3 and docs/09 §4, and keep the credit schedule matching the product decisions below.
- Usage records (ADR-019): `UsageStore` methods return `Result`. Never turn a `UsageError` into empty usage: it becomes a 503, and invoice finalisation fails and retries. `UsageBackend` picks memory or `ClickHouseUsageStore` at startup (`app_with_usage`). ClickHouse goes over HTTP with `{name:Type}` query parameters. `record` (JSON) is the source of truth, reads use `LIMIT 1 BY request_id`, and retention is the table TTL set by `migrate()`.
- Telemetry must not depend on control-plane storage. It reads reservations through the `pt_telemetry::Directory` trait (`CpDirectory` in `pt-control-plane/src/telemetry.rs`). `pt_control_plane::app(svc)` merges the customer API, snapshots, and telemetry routes.
- Gateways stamp `received_at_ms` with wall time. Tests that query telemetry through the control plane must use `SystemClock` (or explicit `from`/`to`), or the default window misses the records.
- Quote sizing constants (80% target utilisation, busiest-hour sustained, busiest-10 s peak) are in `pt-control-plane/src/quote.rs`. Requests are priced with the region pool's profile from `config.profiles`, and every `[[capacity]]` entry must name a profile that exists there.
- pt-router: keep `scheduler.rs`, `workers.rs`, and `dispatch.rs` pure and synchronous. `workers.rs` placement functions are meant to become Dynamo `WorkerFilter`/`WorkerScorer` plugins (docs/13 §3). In `http.rs`, a dispatched request's `Go` owns a `Release` guard, so capacity is freed on every exit. Never release capacity outside that guard, except in `pump` (which holds the lock) when the client has already gone.
- `TermMonths` lives in `pt-core` and is shared by the CRDs and the control plane.
- Call `self.bump().await` after every committed change that could alter what a region serves. It bumps the shared counter in the store (`Store::bump_version`). Snapshots read the version with `sync_version()` before listing data. Versions must only increase, and every instance bumps once at startup (`init_version`).
- Several instances (ADR-023): never keep state that other instances need in process memory. Put it in the store (heartbeats, leases, the version) or the planner (`SqlPlanner`, whose counters are reserved by conditional UPDATE). Loops that change shared state run in `background::run`, only on the lease holder, and must stay safe if two briefly overlap. Use SQL `SqlPlanner` with `with_sql`. `MemoryPlanner`/`MemoryStore` are for one instance.
- Deployments: `ProvisionedThroughput.deployments` (1–10; `[0]` is the primary). They share the reservation's limiter and boundary policy. `max_share` becomes a per-deployment cap limiter in the gateway (`Deployment.cap`), checked before the shared bucket and refunded in `Settlement::finish` if the shared bucket rejects. Keep caps at `max_share` × the reservation's *local* rate (`refresh_caps`).
- Inference keys: each `Deployment.api_keys` holds exactly one current key (`expires_at: None`) plus up to two rotated-out keys. Rotation and revocation are in `service.rs` (`rotate_key`, `revoke_key`), and the lifecycle loop prunes expired keys. Snapshots send the current hash plus `previous_keys` with `expires_at_ms`.
- Gateways look up deployments by `sha256_hex(api_key)`, never by plaintext, and reject rotated-out keys past `expires_at_ms` by their own clock. Apply snapshots through `AppState::apply_snapshot`, which reuses limiters (`ReservationLimiter::reconfigure`) and estimators. Never rebuild them, or bucket state resets.
- With `[quota]`, the limiter enforces the gateway's *share*: `Reservation::entitlement_wu_s()` is the regional total (own CUs plus active failover CUs, computed from wall-clock time), and `limiter.config()` is the local rate. Anything that resizes limiters must go through `AppState::local_rate`, including snapshot applies, or it overwrites the lease.
- The coordinator invariant is that the sum of unexpired grants never exceeds the entitlement. Keep `grant = min(target, E − others' unexpired grants)`, and keep the coordinator's hold (1.5 × TTL) longer than the gateway's lease.
- Coordinator HA (ADR-027): only the lease holder grants (`election::Leadership::serving`, checked in `api.rs` right before `renew`). `Elector::tick` takes `now` from before the write. A dead leader is replaced only after `lease_duration` unchanged, and `ElectionConfig::validate` keeps `lease_duration − renew_deadline ≥ grant hold`, so no warm-up is needed then. Taking a *released* lease, or starting without `[election]`, calls `Coordinator::begin_term(now, true)`: for one hold, grants are capped at the gateway-reported `held_wu_s`, and gateways holding nothing get no lease. `Change::Resumed` (our own record renewed after a lapse) must not clear state. Gateways try `coordinator_url` then `standby_urls`, starting from the last one that answered. The binary handles SIGTERM, so a pod releases its lease on shutdown.
- Controller HA (ADR-029): `main.rs` runs `controller::run` inside `pt_election::lead`, which drops it at the renew deadline and returns `Ended::Lost`; main then exits non-zero so the pod restarts as a standby. Election logic lives only in `pt-election` (shared with the Quota Coordinator); keep kube I/O in `kube_lease` and timing rules in `Elector`, and test with `MemoryLease`. Don't reconcile anything outside `lead` when election is on.
- Region failover (docs/07 §4, ADR-014): a Multi-region reservation's planner footprint is `regions` + `failover_headroom` (`failover::footprint`, `held(pt)` in `service.rs`). Every planner reserve or release must use the footprint, never `pt.regions` alone. Region health comes from gateway heartbeats (`RegionHealth`, soft state). `Service::run_failover` opens and resolves only `automatic` incidents, and never declares while no region is serving. Declaring or resolving any incident must `bump()`, because snapshots carry `failovers`. Gateways compute the activation ramp themselves (`RegionFailover::activation`), and `health::run_rate_refresh` applies it to limiters.
- Hot-spare preemption (docs/13 §2, ADR-015): only while the router's failover state is active (set by `x-pt-failover` from gateways, held `failover_hold_ms`). PAYG is fenced off `hot_spare` workers, and `Dispatcher::preempt` names at most what a waiting provisioned head needs (`workers::preemption_victim`, pure). Only `payg` is ever preempted, never spillover. Abort handles live beside the dispatcher in `http.rs` (`Inner.aborts`), and `Dispatcher::release(id)` is the only way capacity is freed. `http::router()` spawns the 50 ms dispatch ticker, so call it inside a Tokio runtime.
- Backfill cap (ADR-026): PAYG and spillover hold at most `Weights.backfill_ratio` (router `backfill_ratio`, default 0.5) of each non-spare worker's slots and KV (`Worker.backfill_slots/backfill_kv`, updated in `Dispatcher::reserve`/`release`). Unit tests about other behaviour use `backfill_ratio: 1.0` so PAYG can fill a worker.
- Interference suite (docs/05 §7, ADR-025): `crates/pt-router/tests/interference.rs`, against the mock's contention model (`pt-mock-engine/src/contention.rs`, `MockConfig.contention` / `MOCK_CONTENTION=1`). Each scenario runs protected (gateway → router → engine) and as a control (engine alone). The control must *miss* the SLO, or the scenario proves nothing. Tenant A is paced at 90% of its entitlement so gateway throttling doesn't mask results. The suite is timing-based, so scenarios hold a global lock. Soak: `PT_SOAK_SECS=60 cargo test -p pt-router --test interference -- --ignored`. If you change the router, the gateway, or the mock's constants, rerun it with `--nocapture` and check the margins.
- Warm-spare loading (ADR-016): the operator follows the regional snapshot (`failover::SnapshotFollower`, env `PT_*`). `failover::failover_extra` gives per-allocation demand (`wuPerSec × active failover CUs ÷ CUs`), and `sizing::size_with_failover` loads warm spares beyond the hot spares. It holds `status.warmSparesLoaded` while any failover demand remains. `plan()` takes `failover_extra` aligned with `allocations`. Keep both pure. The controller only wires in the snapshot.
- Invoicing (ADR-018): `billing.rs` builds calendar-month invoices in arrears from each reservation's event rate timeline (`rate_timeline`, `fee_lines`), usage (`spillover_lines`, PAYG prices from `[[payg_prices]]`), and the SLA report (`credit_line`). Events that change the price must record the resulting `monthly` (and `cus`/`tier` where relevant). Lifecycle events are stamped with the time they took effect (term boundaries). `finalize_due` stores final invoices, which never change. Every model needs a PAYG price, and telemetry retention must cover a month plus the finalisation grace period (both validated in config).
- Share rebalancing (ADR-024): `regions` is the contract (price, renewals). `pt.effective()` is what gateways enforce, and snapshots, failover shares, steering, and `held()` must use it. Any change to `regions` clears `effective_regions`. Change what a reservation holds with `move_capacity` (reserve growth first, then release shrink), never by hand. `rebalance::target_split` is pure and starts from the contract each time.
- A gateway uses either `[entitlements]` or static `[[reservations]]`/`[[deployments]]`, never both. Profiles are always local to the gateway.
- The signing key in `config/control-plane.toml` and the public key in `config/gateway-*.toml` are a matched development pair. If you change one, regenerate both with `pt-control-plane keygen`.
- Snapshot keys (ADR-020): a key's id is `pt_entitlement::key_id` (the first 16 hex characters of the SHA-256 of the public key), sent in `x-pt-key-id` and stored in caches. Verify with `SnapshotVerifier::verify_with(body, sig, key_id)` against a trusted set (`from_hex_list`). Never fall back to accepting an unknown key id. Gateways report their snapshot's key id in heartbeats, and `RegionStatus.snapshot_key_ids` shows when an old key can be removed.

## Conventions for editing docs
- Headings are numbered `## N. Title`. Cross-links use GitHub-style anchors (for example `05-isolation-and-scheduling.md#4-level-3--engine`). If you rename a heading, fix the links that point to it.
- Each design doc starts with a `> Decision record(s):` line linking its ADRs, and ends with a **"Blog problems addressed"** line listing P-numbers.
- When you add or change a design response, update the traceability matrix in `01-requirements-and-traceability.md`. Every blog problem must map to a section.
- New decisions get a new ADR (next number, one decision per file), added to the table in `docs/README.md`.
- Diagrams are Mermaid. In `sequenceDiagram` message text, avoid `;` because it acts as a statement separator.
- Spelling is British (tokenise, behaviour, utilisation). Numeric targets (tier latencies, coefficients, percentages) are placeholders until calibration runs. Keep them labelled as such.

## Verification
- **Code:** `cargo test --workspace` (unit tests plus end-to-end gateway tests against in-process mock engines), `cargo clippy --workspace --all-targets` (expect no warnings), `cargo fmt --all --check`.
- **Smoke run:** start two `pt-mock-engine`s (`MOCK_ADDR=127.0.0.1:9000` and `:9001`) and `pt-gateway config/gateway.toml`, then use the curl commands in `README.md`. Run it from the scratchpad, because `usage_log` is relative to the working directory.
- **Failover smoke run:** copy `config/control-plane.toml` to the scratchpad with `heartbeat_timeout_seconds = 3`, `recovery_seconds = 4`, and `return_ramp_minutes = 1`, and the two regional gateway configs with `heartbeat_interval_ms = 500` and `[quota]` removed. Start mock engines on :9000 (eu-west) and :9010 (eu-central). Create a `multi_region` reservation, kill the eu-west engine, and watch `/internal/v1/incidents`, `/internal/v1/steering`, and `/v1/pt/status` on :8082.
- **SQL store:** `tests/sql_store.rs` runs only with `PT_TEST_DATABASE_URL` set, and skips otherwise. There's no database installed here. Download a theseus-rs `postgresql-*-x86_64-unknown-linux-gnu` build into the scratchpad, symlink `libxml2.so.2` to the system's `libxml2.so.16` in a private lib dir (`LD_LIBRARY_PATH`), and `initdb -A trust`. Start it with `-c unix_socket_directories=''` because the scratchpad path is too long for a socket, and a TCP port such as 55432. Then `PT_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55432/pt`.
- **ClickHouse:** `pt-telemetry/tests/clickhouse.rs` and `pt-control-plane/tests/usage_clickhouse.rs` run only with `CLICKHOUSE_TEST_URL`, and skip otherwise. Download `clickhouse-common-static-*-amd64.tgz` from the ClickHouse GitHub release into the scratchpad and check its `.sha512`. Run `usr/bin/clickhouse server -- --listen_host=127.0.0.1 --http_port=18123 --tcp_port=19000 --mysql_port=19004 --postgresql_port=19005 --interserver_http_port=19009 --max_server_memory_usage=700000000 --mark_cache_size=67108864` from an empty directory; it uses its embedded config. Don't set `max_thread_pool_size` low, or startup deadlocks. Stop it with `pkill -x clickhouse`, because `pkill -f` matches your own shell. It uses about 640 MB of RAM. Then `CLICKHOUSE_TEST_URL=http://127.0.0.1:18123`.
- **Database TLS:** `pt-control-plane/tests/tls.rs` and `https_with_a_private_ca` run with `PT_TEST_POSTGRES_ADDR=127.0.0.1:55432`, `PT_TEST_TLS_DIR`, and `CLICKHOUSE_TEST_TLS_URL=https://127.0.0.1:18443`. Setup:
  - Make a test CA, a rogue CA, a server certificate (SAN `IP:127.0.0.1,DNS:localhost`), and a client certificate (CN=`certuser`, key converted with `openssl pkcs8 -topk8 -nocrypt` to `client.pk8.key`) with openssl in the scratchpad.
  - PostgreSQL: start with `-c ssl=on -c ssl_cert_file=server.crt -c ssl_key_file=server.key -c ssl_ca_file=root.crt` (files copied into the data directory). Prepend these `pg_hba.conf` rules: `hostssl all tlsonly 127.0.0.1/32 trust`, `host all tlsonly … reject`, `hostssl all certuser … cert`, and `host all certuser … reject`. Create both roles.
  - ClickHouse: add `--https_port=18443 --openSSL.server.certificateFile=… --openSSL.server.privateKeyFile=… --openSSL.server.verificationMode=none`.
- **Disk space:** `target/` grows with every rebuild (it reached 57 GB once and filled the disk). If a build fails with "No space left on device" or the linker dies with a bus error, run `cargo clean`.
- **Links and anchors:** extract every relative `](path#anchor)` link from `docs/**/*.md`, and check that the target file exists and that the anchor matches a GitHub slug of a heading (outside code fences). A small inline Python script was used for this. Expect 0 broken links.
- **Mermaid:** extract the ```mermaid blocks and render each one with
  `npx -y @mermaid-js/mermaid-cli -p pp.json -i d.mmd -o d.svg`, where `pp.json` is `{"args":["--no-sandbox"]}`. Chromium's sandbox is unavailable on this machine (Ubuntu AppArmor userns restriction). Put temporary files in the session scratchpad, not in the repo.

## Product decisions (resolved 2026-09-23 and 2026-09-24)
These are recorded in `docs/11-roadmap-risks-open-questions.md` §4. Treat them as fixed:
- Burst credit is free within its cap. Spillover is billed as regular PAYG traffic, at the regular PAYG list price per model (`[[payg_prices]]` mirrors the PAYG price list).
- CU re-rating is infrequent (no fixed cadence) and passes 50% of efficiency gains to customers.
- SLA commitment is 99.8% attainment. Credits are 10% / 20% / 30% / 50% below 99.8 / 99.7 / 99.6 / 99.5. There is no out-of-shape grace margin.
- Minimum reservation is 1 CU, with 1-, 3-, or 6-month terms. Increases are allowed mid-term for the remaining term. Decreases happen only at renewal.
- Strict-dedicated (no backfill) is offered at launch at a surcharge of 0.3× the base (Standard) CU price on top of the tier price (Standard 1.3×, Interactive 1.55×, Agentic 1.8×).
- There is no customer-defined priority beyond `continuation` at launch.
- Tier price multipliers: Standard 1.0× (base), Interactive 1.25×, Agentic 1.5×.
- Multi-region SKU: a surcharge of 0.2× the base CU price on top of the tier price (Standard 1.2×, Interactive 1.45×, Agentic 1.7×). It stacks with strict-dedicated (`pt_core::MULTI_REGION_SURCHARGE`, decided 2026-09-24).

## Open items
No PM pricing questions are open. `[[payg_prices]]` must mirror the regular PAYG price list; the values in `config/control-plane.toml` are development numbers.
Known failover limits (ADR-014): activation needs the control plane, so a region failure during a control-plane outage doesn't fail over. A partition between a healthy region and the control plane makes the reservation over-serve briefly, never under-serve. During the return ramp, a gateway's limiter can run up to 1% above the entitlement, because rate changes under 1% are skipped. Not built: a weight-prefetch DaemonSet (so loaded warm spares start cold), rendering hot spares as separate router workers (router `hot_spare` flags are configured by hand), metering of preempted PAYG, and a GeoDNS/anycast controller for `/internal/v1/steering`. Top technical risks: Dynamo API churn and fork maintenance, and upstream acceptance of the engine KV-budget patch.
