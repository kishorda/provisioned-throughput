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
            cache_path: Some(cache.into()),
            wait_secs: 5,
        }),
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
    assert!(
        matches!(err, SyncError::Verify(pt_entitlement::Error::BadSignature)),
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
