//! The gateway's prefix-cache index (ADR-030): an agent's repeated context is estimated as
//! cached prefill, at a hit rate learned from the engine, and never across reservations.

use std::sync::Arc;
use std::time::Duration;

use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile, Shape, Tier};
use pt_gateway::config::{DeploymentConfig, PrefixCacheSettings, ReservationConfig, ServerConfig};
use pt_gateway::usage::MemorySink;
use pt_gateway::{router, AppState, GatewayConfig};
use pt_mock_engine::{MockConfig, MockEngine};
use serde_json::{json, Value};

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

fn reservation(id: &str) -> ReservationConfig {
    ReservationConfig {
        id: id.into(),
        tenant: "acme".into(),
        model: "m".into(),
        cus: 10,
        tier: Tier::Agentic,
        profile: "p".into(),
        shape: Shape {
            input_p95: 100_000,
            input_max: 200_000,
            output_p95: 4,
            context_ceiling: 200_000,
            cache_hit_ratio: 0.9,
            burst_factor: 1.0,
        },
    }
}

fn deployment(id: &str, reservation: &str, key: &str) -> DeploymentConfig {
    DeploymentConfig {
        id: id.into(),
        reservation: reservation.into(),
        api_key: key.into(),
        boundary_policy: Default::default(),
        max_share: None,
    }
}

async fn gateway(prefix_cache: PrefixCacheSettings) -> (String, Arc<MemorySink>) {
    let engine = serve(
        MockEngine::new(MockConfig {
            ttft: Duration::from_millis(1),
            tpot: Duration::from_millis(1),
            default_output_tokens: 2,
            ..Default::default()
        })
        .router(),
    )
    .await;
    let config = GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: engine,
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: 1_000_000.0,
        },
        // Prefill only (c = d = 0), so estimated and actual WU compare directly.
        profiles: vec![PerformanceProfile {
            name: "p".into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 0.0,
                d: 0.0,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }],
        entitlements: None,
        quota: None,
        tokenization: Default::default(),
        prefix_cache,
        usage_export: None,
        reservations: vec![reservation("res-a"), reservation("res-b")],
        deployments: vec![
            deployment("dep-a", "res-a", "sk-a"),
            deployment("dep-b", "res-b", "sk-b"),
        ],
    };
    let sink = Arc::new(MemorySink::default());
    let app = AppState::new(&config, sink.clone()).unwrap();
    (serve(router(app)).await, sink)
}

async fn chat(gw: &str, key: &str, messages: &[Value]) {
    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .bearer_auth(key)
        .json(&json!({ "model": "m", "max_tokens": 2, "messages": messages }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.bytes().await.unwrap();
}

/// A long system prompt, then `turns` user/assistant exchanges.
fn conversation(turns: usize) -> Vec<Value> {
    let mut m = vec![json!({ "role": "system", "content": "You are an agent. ".repeat(400) })];
    for t in 0..turns {
        if t > 0 {
            m.push(json!({ "role": "assistant", "content": format!("step {t} done") }));
        }
        m.push(json!({ "role": "user", "content": format!("do step {}", t + 1) }));
    }
    m
}

async fn status(gw: &str) -> Value {
    reqwest::get(format!("{gw}/internal/v1/prefix-cache"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn an_agents_repeated_context_is_estimated_as_cached() {
    let (gw, sink) = gateway(PrefixCacheSettings::default()).await;
    for turn in 1..=4 {
        chat(&gw, "sk-a", &conversation(turn)).await;
    }
    let r = sink.records();
    let (est, actual): (Vec<f64>, Vec<f64>) =
        r.iter().map(|r| (r.wu_estimated, r.wu_actual)).unzip();
    // Turn 1: nothing sent before, so all uncached, which is right.
    assert_eq!(r[0].tokens.cached_prefill, 0);
    assert!((est[0] - actual[0]).abs() < 1e-6);
    // Turn 2: the first turn matched, estimated at the initial 50% hit rate. The engine
    // had it all cached, so the estimate is high, but far closer than all-uncached.
    let uncached_2 = (r[1].tokens.uncached_prefill + r[1].tokens.cached_prefill) as f64;
    assert!(est[1] < 0.6 * uncached_2, "{} vs {uncached_2}", est[1]);
    assert!(est[1] > actual[1]);
    // From turn 3 the learned hit rate (1.0 against this engine) makes it near exact.
    for t in 2..4 {
        let err = (est[t] - actual[t]).abs() / actual[t];
        assert!(
            err < 0.05,
            "turn {}: estimate {} vs actual {}",
            t + 1,
            est[t],
            actual[t]
        );
    }

    let s = status(&gw).await;
    let m = &s["status"]["models"][0];
    assert_eq!(m["model"], "m");
    assert!(m["hit_rate"].as_f64().unwrap() > 0.99);
    assert_eq!(m["observations"], 3);
}

#[tokio::test]
async fn another_reservation_gets_no_credit_for_it() {
    let (gw, sink) = gateway(PrefixCacheSettings::default()).await;
    chat(&gw, "sk-a", &conversation(1)).await;
    // The same conversation from another reservation: the engine shares its cache, but
    // the gateway doesn't predict it, so b's estimate doesn't depend on a's traffic.
    chat(&gw, "sk-b", &conversation(2)).await;
    let r = sink.records();
    assert!(r[1].tokens.cached_prefill > 0, "the engine did hit");
    assert!(
        r[1].wu_estimated
            >= (r[1].tokens.uncached_prefill + r[1].tokens.cached_prefill) as f64 - 1e-6,
        "estimated all uncached"
    );
    let s = status(&gw).await;
    assert!(
        s["status"]["models"][0]["unpredicted_cached_tokens"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[tokio::test]
async fn disabled_estimates_everything_uncached() {
    let (gw, sink) = gateway(PrefixCacheSettings {
        enabled: false,
        ..Default::default()
    })
    .await;
    for turn in 1..=3 {
        chat(&gw, "sk-a", &conversation(turn)).await;
    }
    let r = sink.records();
    let prompt = (r[2].tokens.uncached_prefill + r[2].tokens.cached_prefill) as f64;
    // Within the token-count ratio the gateway learns from the engine (ADR-028).
    let err = (r[2].wu_estimated - prompt).abs() / prompt;
    assert!(
        err < 0.01,
        "estimate {} vs prompt {prompt}",
        r[2].wu_estimated
    );
    assert_eq!(status(&gw).await["enabled"], false);
}
