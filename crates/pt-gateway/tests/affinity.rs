//! Sessions and small reservations served by one replica (ADR-040).

use std::sync::Arc;
use std::time::Duration;

use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile, Shape, Tier};
use pt_gateway::affinity::AffinityConfig;
use pt_gateway::config::{DeploymentConfig, ReservationConfig, ServerConfig};
use pt_gateway::usage::MemorySink;
use pt_gateway::{router, AppState, GatewayConfig};
use pt_mock_engine::{MockConfig, MockEngine};
use serde_json::json;

async fn listen() -> (tokio::net::TcpListener, String) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    (l, url)
}

fn reservation(id: &str, cus: u32) -> ReservationConfig {
    ReservationConfig {
        id: id.into(),
        tenant: "acme".into(),
        model: "m".into(),
        cus,
        tier: Tier::Agentic,
        profile: "p".into(),
        shape: Shape {
            input_p95: 1_000,
            input_max: 8_000,
            output_p95: 4,
            context_ceiling: 16_000,
            cache_hit_ratio: 0.0,
            burst_factor: 1.0,
        },
    }
}

fn config(engine: &str, me: &str, peers: &[&str]) -> GatewayConfig {
    GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: engine.into(),
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: 1_000_000.0,
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
        tokenization: Default::default(),
        prefix_cache: Default::default(),
        affinity: Some(AffinityConfig {
            self_url: me.into(),
            peers: peers.iter().map(|p| p.to_string()).collect(),
            home_below_cus: 4,
            home_gateways: 1,
        }),
        usage_export: None,
        reservations: vec![reservation("big", 20), reservation("small", 2)],
        deployments: vec![
            DeploymentConfig {
                id: "dep-big".into(),
                reservation: "big".into(),
                api_key: "sk-big".into(),
                boundary_policy: Default::default(),
                max_share: None,
            },
            DeploymentConfig {
                id: "dep-small".into(),
                reservation: "small".into(),
                api_key: "sk-small".into(),
                boundary_policy: Default::default(),
                max_share: None,
            },
        ],
    }
}

async fn chat(gw: &str, key: &str, session: Option<&str>) {
    let mut req = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .bearer_auth(key)
        .json(&json!({ "model": "m", "max_tokens": 2, "stream": true,
            "messages": [{ "role": "user", "content": "hi" }] }));
    if let Some(s) = session {
        req = req.header("x-pt-session-id", s);
    }
    let resp = req.send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("[DONE]"), "streamed through: {body}");
}

async fn engine() -> String {
    let (l, url) = listen().await;
    let app = MockEngine::new(MockConfig {
        ttft: Duration::from_millis(1),
        tpot: Duration::from_millis(1),
        default_output_tokens: 2,
        ..Default::default()
    })
    .router();
    tokio::spawn(async move { axum::serve(l, app).await });
    url
}

/// Two replicas that know each other. Returns their URLs and usage sinks.
async fn pair() -> ([String; 2], [Arc<MemorySink>; 2]) {
    let engine = engine().await;
    let (la, a) = listen().await;
    let (lb, b) = listen().await;
    let peers = [a.as_str(), b.as_str()];
    let mut sinks = Vec::new();
    for (l, me) in [(la, &a), (lb, &b)] {
        let sink = Arc::new(MemorySink::default());
        let app = AppState::new(&config(&engine, me, &peers), sink.clone()).unwrap();
        tokio::spawn(async move { axum::serve(l, router(app)).await });
        sinks.push(sink);
    }
    ([a, b], [sinks[0].clone(), sinks[1].clone()])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_is_served_by_one_replica_wherever_it_lands() {
    let ([a, b], sinks) = pair().await;
    for n in 0..6 {
        let gw = if n % 2 == 0 { &a } else { &b };
        chat(gw, "sk-big", Some("agent-7")).await;
    }
    let counts = [sinks[0].records().len(), sinks[1].records().len()];
    assert!(counts.contains(&6) && counts.contains(&0), "{counts:?}");

    // Without a session, a large reservation is served where it lands.
    chat(&a, "sk-big", None).await;
    chat(&b, "sk-big", None).await;
    let after = [sinks[0].records().len(), sinks[1].records().len()];
    assert_eq!(after, [counts[0] + 1, counts[1] + 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_small_reservation_has_a_home_gateway() {
    let ([a, b], sinks) = pair().await;
    for n in 0..6 {
        chat(if n % 2 == 0 { &a } else { &b }, "sk-small", None).await;
    }
    let counts = [sinks[0].records().len(), sinks[1].records().len()];
    assert!(counts.contains(&6) && counts.contains(&0), "{counts:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_owner_means_serving_here() {
    let engine = engine().await;
    let (l, me) = listen().await;
    let dead = "http://127.0.0.1:1";
    let sink = Arc::new(MemorySink::default());
    let app = AppState::new(&config(&engine, &me, &[&me, dead]), sink.clone()).unwrap();
    tokio::spawn(async move { axum::serve(l, router(app)).await });
    // Some of these sessions belong to the dead peer; all are still served.
    for n in 0..8 {
        chat(&me, "sk-big", Some(&format!("s{n}"))).await;
    }
    assert_eq!(sink.records().len(), 8);
}
