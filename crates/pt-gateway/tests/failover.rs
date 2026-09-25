//! End to end: a region fails, and its pair's gateway takes on the failover entitlement.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jiff::{SignedDuration, Timestamp};
use pt_control_plane::clock::ManualClock;
use pt_control_plane::failover::Health;
use pt_control_plane::model::{CreateRequest, DeclareIncident, RegionShare, ResolveIncident, Sku};
use pt_control_plane::{api as cp_api, in_memory, ControlPlaneConfig};
use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile, PoolIsolation, Shape, TermMonths, Tier};
use pt_gateway::config::{EntitlementSourceConfig, ServerConfig};
use pt_gateway::health::HeartbeatClient;
use pt_gateway::sync::SnapshotClient;
use pt_gateway::usage::MemorySink;
use pt_gateway::{router, AppState, GatewayConfig};
use pt_mock_engine::{MockConfig, MockEngine};
use serde_json::Value;

const MODEL: &str = "llama-4-maverick";
const PROFILE: &str = "llama-4-maverick.h200.vllm-0.11.tp8";
const WU_PER_CU: f64 = 1_000.0;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

fn gateway_config(cp: &str, engine: &str, public_key: String) -> GatewayConfig {
    GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: engine.into(),
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
            control_plane_url: cp.into(),
            region: "eu-central".into(),
            token: "region-token-eu-central-dev".into(),
            public_key,
            extra_public_keys: vec![],
            cache_path: None,
            wait_secs: 5,
            heartbeat_interval_ms: 50,
            engine_health_path: "/healthz".into(),
            tls: Default::default(),
        }),
        quota: None,
        usage_export: None,
        reservations: vec![],
        deployments: vec![],
    }
}

async fn status(gw: &str, key: &str) -> Value {
    reqwest::Client::new()
        .get(format!("{gw}/v1/pt/status"))
        .bearer_auth(key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap_or(Value::Null)
}

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

#[tokio::test]
async fn paired_region_takes_over_then_hands_back_gradually() {
    let config = ControlPlaneConfig::load(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/control-plane.toml"),
    )
    .unwrap();
    // Gateways activate failover entitlements by wall-clock time, so start the control
    // plane's clock at now.
    let clock = ManualClock::new(Timestamp::now());
    let svc = in_memory(config, clock.clone());
    let cp = serve(cp_api::router(svc.clone())).await;
    // The engine records each request's x-pt-failover header.
    let seen = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
    let record = seen.clone();
    let engine = serve(
        MockEngine::new(MockConfig {
            name: "eu-central".into(),
            ttft: Duration::from_millis(1),
            tpot: Duration::from_millis(1),
            default_output_tokens: 4,
        })
        .router()
        .layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let record = record.clone();
                async move {
                    if req.uri().path() == "/v1/chat/completions" {
                        let h = req.headers().get("x-pt-failover");
                        record
                            .lock()
                            .unwrap()
                            .push(h.and_then(|v| v.to_str().ok()).map(str::to_owned));
                    }
                    next.run(req).await
                }
            },
        )),
    )
    .await;
    let chat = |gw: String, key: String| async move {
        let r = reqwest::Client::new()
            .post(format!("{gw}/v1/chat/completions"))
            .bearer_auth(key)
            .json(&serde_json::json!({ "model": MODEL, "max_tokens": 4, "messages": [{ "role": "user", "content": "hi" }] }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        r.bytes().await.unwrap();
    };

    let key = svc
        .create(
            "acme",
            None,
            CreateRequest {
                name: "agents".into(),
                model: MODEL.into(),
                tier: Tier::Agentic,
                regions: vec![
                    RegionShare {
                        region: "eu-west".into(),
                        cus: 3,
                    },
                    RegionShare {
                        region: "eu-central".into(),
                        cus: 1,
                    },
                ],
                sku: Sku::MultiRegion,
                isolation: PoolIsolation::Shared,
                term_months: TermMonths::One,
                start_at: None,
                auto_renew: true,
                shape: Shape {
                    input_p95: 1_000,
                    input_max: 8_000,
                    output_p95: 100,
                    context_ceiling: 16_000,
                    cache_hit_ratio: 0.0,
                    burst_factor: 1.0,
                },
                boundary_policy: Default::default(),
            },
        )
        .await
        .unwrap()
        .api_key
        .unwrap();

    let gw_config = gateway_config(&cp, &engine, svc.signer().public_key_hex());
    let source = gw_config.entitlements.clone().unwrap();
    let app = AppState::new(&gw_config, Arc::new(MemorySink::default())).unwrap();
    let heartbeat = HeartbeatClient::new(source.clone(), "gw-central-1".into()).unwrap();
    // Not serving before the first snapshot.
    assert!(!heartbeat.serving(&app).await);
    tokio::spawn(
        SnapshotClient::new(source.clone())
            .unwrap()
            .run(app.clone()),
    );
    tokio::spawn(heartbeat.run(app.clone()));
    tokio::spawn(pt_gateway::health::run_rate_refresh(
        app.clone(),
        Duration::from_millis(20),
    ));
    let gw = serve(router(app.clone())).await;

    eventually("serving heartbeat", || async {
        svc.region_statuses()
            .await
            .unwrap()
            .iter()
            .any(|s| s.region == "eu-central" && s.health == Health::Serving)
    })
    .await;
    let s = status(&gw, &key).await;
    assert_eq!(s["cus"], 1);
    assert_eq!(s["failover_cus"], 0.0);
    assert_eq!(s["entitlement_wu_per_s"], WU_PER_CU);
    chat(gw.clone(), key.clone()).await;
    assert_eq!(seen.lock().unwrap().pop(), Some(None), "no failover marker");

    // eu-west fails 10 minutes ago: eu-central admits its 3 CUs as well.
    let now = Timestamp::now();
    clock.set(now);
    let inc = svc
        .declare_incident(DeclareIncident {
            region: "eu-west".into(),
            started_at: Some(now - SignedDuration::from_mins(10)),
            description: "Region down".into(),
        })
        .await
        .unwrap();
    eventually("failover active", || async {
        status(&gw, &key).await["failover_cus"] == 3.0
    })
    .await;
    let s = status(&gw, &key).await;
    assert_eq!(s["entitlement_wu_per_s"], 4.0 * WU_PER_CU);
    assert_eq!(s["local_share_wu_per_s"], 4.0 * WU_PER_CU);
    // Provisioned traffic now tells the router to fence and preempt PAYG on hot spares.
    chat(gw.clone(), key.clone()).await;
    assert_eq!(seen.lock().unwrap().pop(), Some(Some("active".into())));

    // Recovered 5 minutes ago: half way down the 10-minute ramp.
    svc.resolve_incident(
        &inc.id,
        ResolveIncident {
            ended_at: Some(now - SignedDuration::from_mins(5)),
        },
    )
    .await
    .unwrap();
    eventually("failover ramping down", || async {
        let f = status(&gw, &key).await["failover_cus"].as_f64().unwrap();
        (f - 1.5).abs() < 0.02
    })
    .await;
    let local = status(&gw, &key).await["local_share_wu_per_s"]
        .as_f64()
        .unwrap();
    assert!((local - 2_500.0).abs() < 30.0, "{local}");
}

#[tokio::test]
async fn gateway_with_a_dead_engine_reports_not_serving() {
    let config = ControlPlaneConfig::load(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/control-plane.toml"),
    )
    .unwrap();
    let svc = in_memory(config, ManualClock::new(Timestamp::now()));
    let cp = serve(cp_api::router(svc.clone())).await;
    let gw_config = gateway_config(&cp, "http://127.0.0.1:1", svc.signer().public_key_hex());
    let source = gw_config.entitlements.clone().unwrap();
    let app = AppState::new(&gw_config, Arc::new(MemorySink::default())).unwrap();
    SnapshotClient::new(source.clone())
        .unwrap()
        .poll_once(&app, false)
        .await
        .unwrap();
    let hb = HeartbeatClient::new(source, "gw-1".into()).unwrap();
    assert!(!hb.beat(&app).await.unwrap(), "engine is down");
    let s = svc
        .region_statuses()
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.region == "eu-central")
        .unwrap();
    assert_eq!(s.health, Health::Down);
    assert_eq!(s.gateways, 1);
    assert_eq!(s.serving_gateways, 0);
}
