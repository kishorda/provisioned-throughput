//! End to end: requests through the gateway show up in the customer's usage and SLA reports.

use std::path::PathBuf;

use std::time::{Duration, Instant};

use pt_control_plane::clock::SystemClock;
use pt_control_plane::{app as cp_app, in_memory, ControlPlaneConfig};
use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile};
use pt_gateway::config::{EntitlementSourceConfig, ServerConfig, UsageExportConfig};
use pt_gateway::sync::SnapshotClient;
use pt_gateway::usage::HttpSink;
use pt_gateway::{router, AppState, GatewayConfig};
use pt_mock_engine::{MockConfig, MockEngine};
use serde_json::{json, Value};

const ADMIN: &str = "sk-admin-acme-dev";
const REGION_TOKEN: &str = "region-token-eu-west-dev";
const MODEL: &str = "llama-4-maverick";

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

async fn eventually<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn get(url: &str) -> Value {
    reqwest::Client::new()
        .get(url)
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn gateway_usage_reaches_customer_reports() {
    // The real clock: gateways stamp records with wall time, and the default query range
    // ends at the control plane's now.
    let config = ControlPlaneConfig::load(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/control-plane.toml"),
    )
    .unwrap();
    let svc = in_memory(config, SystemClock);
    let (routes, _) = cp_app(svc.clone());
    let cp = serve(routes).await;

    let engine = serve(
        MockEngine::new(MockConfig {
            name: "main".into(),
            ttft: Duration::from_millis(5),
            tpot: Duration::from_millis(2),
            default_output_tokens: 8,
        })
        .router(),
    )
    .await;

    let gw_config = GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: engine,
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: 1_000.0,
        },
        profiles: vec![PerformanceProfile {
            name: "llama-4-maverick.b200.trtllm-1.2.tp8".into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 1.0,
                d: 0.0,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }],
        entitlements: Some(EntitlementSourceConfig {
            control_plane_url: cp.clone(),
            region: "eu-west".into(),
            token: REGION_TOKEN.into(),
            public_key: svc.signer().public_key_hex(),
            extra_public_keys: vec![],
            cache_path: None,
            wait_secs: 5,
            heartbeat_interval_ms: 0,
            engine_health_path: "/healthz".into(),
        }),
        quota: None,
        usage_export: Some(UsageExportConfig {
            control_plane_url: cp.clone(),
            token: REGION_TOKEN.into(),
            batch_size: 100,
            flush_interval_ms: 50,
            buffer: 1_000,
        }),
        reservations: vec![],
        deployments: vec![],
    };
    let sink = HttpSink::start(gw_config.usage_export.clone().unwrap());
    let app = AppState::new(&gw_config, sink.clone()).unwrap();
    tokio::spawn(
        SnapshotClient::new(gw_config.entitlements.clone().unwrap())
            .unwrap()
            .run(app.clone()),
    );
    let gw = serve(router(app)).await;

    // Create a reservation and wait for the gateway to serve it.
    let created: Value = reqwest::Client::new()
        .post(format!("{cp}/v1/provisioned-throughput"))
        .bearer_auth(ADMIN)
        .json(&json!({
            "name": "agents",
            "model": MODEL,
            "tier": "agentic",
            "regions": [{ "region": "eu-west", "cus": 2 }],
            "term_months": 1,
            "shape": { "input_p95": 1000, "input_max": 8000, "output_p95": 100, "context_ceiling": 16000 },
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();
    let key = created["api_key"].as_str().unwrap().to_string();
    let client = reqwest::Client::new();
    eventually("gateway serves the new key", || async {
        client
            .get(format!("{gw}/v1/pt/status"))
            .bearer_auth(&key)
            .send()
            .await
            .unwrap()
            .status()
            == 200
    })
    .await;

    // Twelve calls in one agent session, streamed so TPOT is measured.
    for i in 0..12 {
        let r = client
            .post(format!("{gw}/v1/chat/completions"))
            .bearer_auth(&key)
            .header("x-pt-session-id", "trip-planner-1")
            .json(&json!({
                "model": MODEL,
                "stream": true,
                "max_tokens": 8,
                "messages": [
                    { "role": "system", "content": "You plan trips. ".repeat(20) },
                    { "role": "user", "content": format!("step {i}") },
                ],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        r.bytes().await.unwrap();
    }

    let base = format!("{cp}/v1/provisioned-throughput/{id}");
    eventually("usage ingested", || async {
        get(&format!("{base}/usage")).await["summary"]["requests"]["provisioned"] == 12
    })
    .await;

    let usage = get(&format!("{base}/usage")).await;
    let s = &usage["summary"];
    assert_eq!(usage["entitlement_wu_per_s"], 2_000.0);
    assert!(s["ttft_ms"]["p95"].as_f64().unwrap() >= 5.0);
    assert!(s["tpot_ms"]["p95"].as_f64().is_some());
    assert_eq!(s["tokens"]["decode"], 96);
    // The long system prompt is cached after the first call.
    assert!(s["cache_hit_rate"].as_f64().unwrap() > 0.5, "{s}");
    assert_eq!(usage["shape"]["in_shape_fraction"], 1.0);

    let session = get(&format!("{base}/sessions/trip-planner-1")).await;
    assert_eq!(session["calls"], 12);
    assert_eq!(session["throttled"], 0);

    let sla = get(&format!("{base}/sla")).await;
    // Every call came within the 10-minute activation grace, so none count yet.
    assert_eq!(
        sla["excluded"]["excluded_periods"]["activation"], 12,
        "{sla}"
    );
    assert_eq!(sla["eligible_requests"], 0);
    assert_eq!(sla["exclusion_windows"][0]["reason"], "activation");
    assert_eq!(sla["attainment_pct"], 100.0);
    assert_eq!(sla["credit_pct"], 0);
    assert_eq!(sink.dropped(), 0);

    // Other tenants can't read it.
    let r = reqwest::Client::new()
        .get(format!("{base}/usage"))
        .bearer_auth("sk-admin-globex-dev")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}
