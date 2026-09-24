//! Gateway → router → worker: the gateway's classes and reservations reach the router.

use std::sync::Arc;
use std::time::Duration;

use pt_admission::BoundaryPolicy;
use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile, Shape, Tier};
use pt_gateway::config::{DeploymentConfig, ReservationConfig, ServerConfig};
use pt_gateway::usage::MemorySink;
use pt_gateway::{router as gateway_router, AppState, GatewayConfig};
use pt_mock_engine::{MockConfig, MockEngine};
use pt_router::config::{RouterConfig, WorkerConfig};
use pt_router::http;
use serde_json::{json, Value};

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

#[tokio::test]
async fn gateway_classes_reach_the_router() {
    let engine = MockEngine::new(MockConfig {
        name: "w".into(),
        ttft: Duration::from_millis(1),
        tpot: Duration::from_millis(1),
        default_output_tokens: 50,
    });
    let worker = serve(engine.router()).await;
    let router_cfg = RouterConfig {
        listen: "127.0.0.1:0".into(),
        block_size: 16,
        default_max_tokens: 64,
        queue_timeout_ms: 5_000,
        payg_guard_every: 50,
        weights: None,
        workers: vec![WorkerConfig {
            id: "w0".into(),
            url: worker,
            slots: 4,
            kv_blocks: 1_000,
        }],
        allocations: vec![],
    };
    let router = serve(http::router(http::Shared::new(&router_cfg))).await;

    // 1 CU of 20 WU/s with spillover: the first 56 WU request is provisioned, the second
    // spills over. Both go through the router (it's also the PAYG engine here).
    let gw_cfg = GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: router.clone(),
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: 20.0,
        },
        profiles: vec![PerformanceProfile {
            name: "p".into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 1.0,
                d: 0.0,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }],
        entitlements: None,
        quota: None,
        usage_export: None,
        reservations: vec![ReservationConfig {
            id: "res-1".into(),
            tenant: "acme".into(),
            model: "m".into(),
            cus: 1,
            tier: Tier::Interactive,
            profile: "p".into(),
            shape: Shape {
                input_p95: 100,
                input_max: 1_000,
                output_p95: 100,
                context_ceiling: 2_000,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
        }],
        deployments: vec![DeploymentConfig {
            id: "dep-1".into(),
            reservation: "res-1".into(),
            api_key: "sk".into(),
            boundary_policy: BoundaryPolicy {
                spillover: true,
                ..Default::default()
            },
            max_share: None,
        }],
    };
    let app = AppState::new(&gw_cfg, Arc::new(MemorySink::default())).unwrap();
    let gw = serve(gateway_router(app)).await;

    let client = reqwest::Client::new();
    let mut classes = Vec::new();
    for _ in 0..2 {
        let r = client
            .post(format!("{gw}/v1/chat/completions"))
            .bearer_auth("sk")
            .json(&json!({ "model": "m", "max_tokens": 50, "messages": [{ "role": "user", "content": "hello" }] }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        classes.push(r.headers()["x-pt-class"].to_str().unwrap().to_string());
        r.bytes().await.unwrap();
    }
    assert_eq!(classes, ["provisioned", "spillover"]);

    let s: Value = reqwest::get(format!("{router}/v1/router/status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(s["dispatched"]["provisioned"], 1);
    assert_eq!(s["dispatched"]["spillover"], 1);
    assert_eq!(s["workers"][0]["slots_used"], 0);
}
