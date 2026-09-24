//! End to end: control plane → signed snapshot → gateway → mock engine.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pt_control_plane::clock::ManualClock;
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::store::MemoryStore;
use pt_control_plane::{api as cp_api, in_memory, ControlPlaneConfig, Service};
use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile};
use pt_gateway::config::{EntitlementSourceConfig, ServerConfig};
use pt_gateway::sync::{SnapshotClient, SyncError};
use pt_gateway::usage::MemorySink;
use pt_gateway::{router, AppState, GatewayConfig};
use pt_mock_engine::{MockConfig, MockEngine};
use serde_json::{json, Value};

type Cp = Arc<Service<MemoryStore, MemoryPlanner, ManualClock>>;

const ADMIN: &str = "sk-admin-acme-dev";
const MODEL: &str = "llama-4-maverick";
const PROFILE: &str = "llama-4-maverick.b200.trtllm-1.2.tp8";
const WU_PER_CU: f64 = 1_000.0;

fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

async fn spawn_control_plane() -> (String, Cp, ManualClock) {
    let config = ControlPlaneConfig::load(repo("config/control-plane.toml")).unwrap();
    let clock = ManualClock::new("2026-10-01T00:00:00Z".parse().unwrap());
    let svc = in_memory(config, clock.clone());
    (serve(cp_api::router(svc.clone())).await, svc, clock)
}

async fn spawn_engine() -> String {
    let engine = MockEngine::new(MockConfig {
        name: "main".into(),
        ttft: Duration::from_millis(1),
        tpot: Duration::from_millis(1),
        default_output_tokens: 4,
    });
    serve(engine.router()).await
}

fn cache_path(name: &str) -> String {
    let p = std::env::temp_dir().join(format!(
        "pt-entitlements-{name}-{}.json",
        uuid::Uuid::new_v4()
    ));
    p.display().to_string()
}

fn gateway_config(
    cp_url: &str,
    engine_url: &str,
    public_key: String,
    cache: &str,
) -> GatewayConfig {
    GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: engine_url.into(),
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: WU_PER_CU,
        },
        profiles: vec![PerformanceProfile {
            name: PROFILE.into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 1.0,
                d: 0.0,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }],
        entitlements: Some(EntitlementSourceConfig {
            control_plane_url: cp_url.into(),
            region: "eu-west".into(),
            token: "region-token-eu-west-dev".into(),
            public_key,
            extra_public_keys: vec![],
            cache_path: Some(cache.into()),
            wait_secs: 5,
            heartbeat_interval_ms: 0,
            engine_health_path: "/healthz".into(),
            tls: Default::default(),
        }),
        quota: None,
        usage_export: None,
        reservations: vec![],
        deployments: vec![],
    }
}

/// Start a gateway that syncs in the background. Returns its URL and state.
async fn spawn_gateway(config: GatewayConfig) -> (String, AppState) {
    let app = AppState::new(&config, Arc::new(MemorySink::default())).unwrap();
    let client = SnapshotClient::new(config.entitlements.clone().unwrap()).unwrap();
    let _ = client.load_cache(&app);
    tokio::spawn(client.run(app.clone()));
    (serve(router(app.clone())).await, app)
}

async fn status(gw: &str, key: &str) -> (u16, Value) {
    let r = reqwest::Client::new()
        .get(format!("{gw}/v1/pt/status"))
        .bearer_auth(key)
        .send()
        .await
        .unwrap();
    let code = r.status().as_u16();
    (code, r.json().await.unwrap_or(Value::Null))
}

/// Poll until `f` holds, up to 5 s.
async fn eventually<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn create_pt(cp: &str, cus_eu_west: u32) -> (String, String) {
    let v: Value = reqwest::Client::new()
        .post(format!("{cp}/v1/provisioned-throughput"))
        .bearer_auth(ADMIN)
        .json(&json!({
            "name": format!("agents-{}", uuid::Uuid::new_v4().simple()),
            "model": MODEL,
            "tier": "agentic",
            "regions": [{ "region": "eu-west", "cus": cus_eu_west }, { "region": "us-east", "cus": 1 }],
            "term_months": 1,
            "shape": { "input_p95": 1000, "input_max": 8000, "output_p95": 100, "context_ceiling": 16000 },
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        v["id"].as_str().unwrap().into(),
        v["api_key"].as_str().unwrap().into(),
    )
}

fn chat(gw: &str, key: &str) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .bearer_auth(key)
        .json(&json!({ "model": MODEL, "max_tokens": 4, "messages": [{ "role": "user", "content": "hi" }] }))
}

#[tokio::test]
async fn control_plane_changes_reach_the_gateway() {
    let (cp, svc, clock) = spawn_control_plane().await;
    let engine = spawn_engine().await;
    let cache = cache_path("e2e");
    let (gw, app) = spawn_gateway(gateway_config(
        &cp,
        &engine,
        svc.signer().public_key_hex(),
        &cache,
    ))
    .await;

    // The first snapshot arrives even with nothing reserved.
    eventually("first snapshot", || async {
        app.entitlements().version > 0
    })
    .await;

    // Create: the new key starts working at the gateway.
    let (id, key) = create_pt(&cp, 2).await;
    eventually("key accepted", || async {
        status(&gw, &key).await.0 == 200
    })
    .await;
    let (_, s) = status(&gw, &key).await;
    assert_eq!(s["reservation"], id);
    assert_eq!(
        s["entitlement_wu_per_s"],
        2.0 * WU_PER_CU,
        "eu-west share only"
    );
    assert_eq!(s["tier"], "agentic");
    let r = chat(&gw, &key).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["x-pt-class"], "provisioned");

    // Increase: the gateway's entitlement follows.
    let r = reqwest::Client::new()
        .patch(format!("{cp}/v1/provisioned-throughput/{id}"))
        .bearer_auth(ADMIN)
        .json(&json!({ "regions": [{ "region": "eu-west", "cus": 5 }, { "region": "us-east", "cus": 1 }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    eventually("resize applied", || async {
        status(&gw, &key).await.1["entitlement_wu_per_s"] == 5.0 * WU_PER_CU
    })
    .await;

    // Delete mid-term: still serving until term end.
    let r = reqwest::Client::new()
        .delete(format!("{cp}/v1/provisioned-throughput/{id}"))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    let v = svc.entitlement_version();
    eventually("delete synced", || async {
        app.entitlements().version >= v
    })
    .await;
    assert_eq!(chat(&gw, &key).send().await.unwrap().status(), 200);

    // Term end: the key stops working.
    let term_end = svc.get("acme", &id).await.unwrap().term_end;
    clock.set(term_end);
    assert_eq!(svc.run_lifecycle().await.ended, 1);
    eventually("key revoked", || async { status(&gw, &key).await.0 == 401 }).await;
    assert_eq!(chat(&gw, &key).send().await.unwrap().status(), 401);

    let _ = std::fs::remove_file(cache);
}

#[tokio::test]
async fn gateway_serves_cached_entitlements_when_the_control_plane_is_down() {
    let (cp, svc, _) = spawn_control_plane().await;
    let engine = spawn_engine().await;
    let cache = cache_path("lkg");
    let public = svc.signer().public_key_hex();
    let (id, key) = create_pt(&cp, 3).await;

    let (gw1, _) = spawn_gateway(gateway_config(&cp, &engine, public.clone(), &cache)).await;
    eventually("first gateway synced", || async {
        status(&gw1, &key).await.0 == 200
    })
    .await;

    // A second gateway starts while the control plane is unreachable.
    let dead = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", l.local_addr().unwrap())
    };
    let (gw2, app2) = spawn_gateway(gateway_config(&dead, &engine, public, &cache)).await;
    let (code, s) = status(&gw2, &key).await;
    assert_eq!(code, 200, "served from the last-known-good cache");
    assert_eq!(s["reservation"], id);
    assert_eq!(chat(&gw2, &key).send().await.unwrap().status(), 200);
    let meta: Value = reqwest::get(format!("{gw2}/internal/v1/entitlements"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(meta["source"], "snapshot");
    assert_eq!(meta["reservations"], 1);
    assert_eq!(
        meta["generated_at"], "2026-10-01T00:00:00Z",
        "staleness survives a cache reload"
    );
    assert!(app2.entitlements().version > 0);

    let _ = std::fs::remove_file(cache);
}

#[tokio::test]
async fn untrusted_snapshots_are_rejected() {
    let (cp, svc, _) = spawn_control_plane().await;
    let engine = spawn_engine().await;
    let (_, key) = create_pt(&cp, 1).await;

    // Signed by the control plane, but the gateway trusts a different key.
    let (_, other_public) = pt_entitlement::SnapshotSigner::generate();
    let config = gateway_config(&cp, &engine, other_public, &cache_path("bad-key"));
    let app = AppState::new(&config, Arc::new(MemorySink::default())).unwrap();
    let client = SnapshotClient::new(config.entitlements.clone().unwrap()).unwrap();
    let err = client.poll_once(&app, false).await.unwrap_err();
    // The key id names the control plane's key, which this gateway doesn't trust.
    assert!(
        matches!(err, SyncError::Verify(pt_entitlement::Error::UnknownKey(ref id)) if *id == svc.signer().key_id()),
        "{err}"
    );
    assert!(app.deployment_for_key(&key).is_none());

    // A token for a different region is refused by the control plane.
    let mut config = gateway_config(
        &cp,
        &engine,
        svc.signer().public_key_hex(),
        &cache_path("bad-token"),
    );
    config.entitlements.as_mut().unwrap().token = "region-token-us-east-dev".into();
    let app = AppState::new(&config, Arc::new(MemorySink::default())).unwrap();
    let client = SnapshotClient::new(config.entitlements.clone().unwrap()).unwrap();
    let err = client.poll_once(&app, false).await.unwrap_err();
    assert!(matches!(err, SyncError::Status(s) if s == 403), "{err}");
}

async fn rotate(cp: &str, id: &str, grace_minutes: u64) -> String {
    let v: Value = reqwest::Client::new()
        .post(format!("{cp}/v1/provisioned-throughput/{id}/keys/rotate"))
        .bearer_auth(ADMIN)
        .json(&json!({ "grace_minutes": grace_minutes }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["api_key"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn key_rotation_reaches_the_gateway() {
    let (cp, svc, _) = spawn_control_plane().await;
    let engine = spawn_engine().await;
    let cache = cache_path("rotation");
    let (gw, _) = spawn_gateway(gateway_config(
        &cp,
        &engine,
        svc.signer().public_key_hex(),
        &cache,
    ))
    .await;
    let (id, first) = create_pt(&cp, 1).await;
    eventually("first key works", || async {
        status(&gw, &first).await.0 == 200
    })
    .await;

    // Rotate with a grace period: both keys work.
    let second = rotate(&cp, &id, 30).await;
    eventually("new key works", || async {
        status(&gw, &second).await.0 == 200
    })
    .await;
    assert_eq!(
        status(&gw, &first).await.0,
        200,
        "old key still in its grace period"
    );
    assert_eq!(chat(&gw, &first).send().await.unwrap().status(), 200);

    // Revoke the old key: it stops working; the new one doesn't.
    let keys: Value = reqwest::Client::new()
        .get(format!("{cp}/v1/provisioned-throughput/{id}/keys"))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let old_id = keys["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|k| k.get("expires_at").is_some())
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let r = reqwest::Client::new()
        .delete(format!("{cp}/v1/provisioned-throughput/{id}/keys/{old_id}"))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    eventually("old key revoked", || async {
        status(&gw, &first).await.0 == 401
    })
    .await;
    assert_eq!(status(&gw, &second).await.0, 200);

    // A compromised key: rotate with no grace period.
    let third = rotate(&cp, &id, 0).await;
    eventually("replaced immediately", || async {
        status(&gw, &second).await.0 == 401 && status(&gw, &third).await.0 == 200
    })
    .await;

    let _ = std::fs::remove_file(cache);
}

#[tokio::test]
async fn gateway_enforces_key_expiry_itself() {
    let (_, svc, _) = spawn_control_plane().await;
    let config = gateway_config(
        "http://127.0.0.1:1",
        "http://127.0.0.1:1",
        svc.signer().public_key_hex(),
        &cache_path("expiry"),
    );
    let app = AppState::new(&config, Arc::new(MemorySink::default())).unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let snapshot = pt_entitlement::Snapshot {
        region: "eu-west".into(),
        version: 1,
        generated_at: "2026-10-01T00:00:00Z".into(),
        reservations: vec![pt_entitlement::ReservationEntitlement {
            id: "pt-1".into(),
            tenant: "acme".into(),
            model: MODEL.into(),
            cus: 1,
            tier: pt_core::Tier::Agentic,
            profile: PROFILE.into(),
            shape: pt_core::Shape {
                input_p95: 1,
                input_max: 1,
                output_p95: 1,
                context_ceiling: 1,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
            failover: vec![],
        }],
        deployments: vec![pt_entitlement::DeploymentEntitlement {
            id: "dep-1".into(),
            reservation: "pt-1".into(),
            api_key_sha256: pt_entitlement::sha256_hex(b"current"),
            previous_keys: vec![
                pt_entitlement::PreviousKey {
                    api_key_sha256: pt_entitlement::sha256_hex(b"expired"),
                    expires_at_ms: now_ms - 1,
                },
                pt_entitlement::PreviousKey {
                    api_key_sha256: pt_entitlement::sha256_hex(b"in-grace"),
                    expires_at_ms: now_ms + 60_000,
                },
            ],
            max_share: None,
            boundary_policy: Default::default(),
        }],
        failovers: vec![],
    };
    app.apply_snapshot(&snapshot).unwrap();
    assert!(app.deployment_for_key("current").is_some());
    assert!(app.deployment_for_key("in-grace").is_some());
    assert!(
        app.deployment_for_key("expired").is_none(),
        "expired before the next snapshot"
    );
    assert!(app.deployment_for_key("unknown").is_none());
}

#[tokio::test]
async fn deployment_cap_protects_the_rest_of_the_reservation() {
    let (cp, svc, _) = spawn_control_plane().await;
    let engine = spawn_engine().await;
    let cache = cache_path("caps");
    let (gw, _) = spawn_gateway(gateway_config(
        &cp,
        &engine,
        svc.signer().public_key_hex(),
        &cache,
    ))
    .await;
    let (id, prod) = create_pt(&cp, 1).await; // 1 CU in eu-west: 1,000 WU/s

    let staging: Value = reqwest::Client::new()
        .post(format!("{cp}/v1/provisioned-throughput/{id}/deployments"))
        .bearer_auth(ADMIN)
        .json(&json!({ "name": "staging", "max_share": 0.2 }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let staging_key = staging["api_key"].as_str().unwrap().to_string();
    eventually("both keys work", || async {
        status(&gw, &prod).await.0 == 200 && status(&gw, &staging_key).await.0 == 200
    })
    .await;
    let (_, s) = status(&gw, &staging_key).await;
    assert_eq!(s["deployment_max_share"], 0.2);
    assert_eq!(s["deployment_cap_wu_per_s"], 200.0);
    assert_eq!(s["reservation"], id, "same reservation as prod");
    assert!(status(&gw, &prod).await.1["deployment_cap_wu_per_s"].is_null());

    // About 508 WU each: 500 prompt tokens plus framing, 4 output tokens.
    let big = |key: &str| {
        reqwest::Client::new()
            .post(format!("{gw}/v1/chat/completions"))
            .bearer_auth(key)
            .json(&json!({
                "model": MODEL,
                "max_tokens": 4,
                "messages": [{ "role": "user", "content": "x".repeat(2_000) }],
            }))
            .send()
    };
    assert_eq!(
        big(&staging_key).await.unwrap().status(),
        200,
        "a full cap admits one"
    );
    let r = big(&staging_key).await.unwrap();
    assert_eq!(r.status(), 429);
    assert_eq!(r.headers()["x-pt-reason"], "deployment_cap_exhausted");
    assert!(r.headers().contains_key("retry-after"));
    // Prod draws on the shared bucket, which staging's cap kept mostly free.
    assert_eq!(big(&prod).await.unwrap().status(), 200);

    let _ = std::fs::remove_file(cache);
}
