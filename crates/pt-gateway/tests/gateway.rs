//! End-to-end tests: real gateway and mock engine(s) on ephemeral ports.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use pt_admission::{BoundaryPolicy, QueuePolicy};
use pt_core::cost::TierCapacity;
use pt_core::{
    Coefficients, Outcome, PerformanceProfile, RejectReason, Shape, Tier, TrafficClass, UsageRecord,
};
use pt_gateway::config::{DeploymentConfig, ReservationConfig, ServerConfig};
use pt_gateway::usage::MemorySink;
use pt_gateway::{router, AppState, GatewayConfig};
use pt_mock_engine::{MockConfig, MockEngine};
use serde_json::{json, Value};

const KEY: &str = "sk-test";
const MODEL: &str = "test-model";

struct Engine {
    url: String,
    engine: MockEngine,
}

async fn spawn_engine(name: &str, ttft_ms: u64, tpot_ms: u64, output_tokens: u64) -> Engine {
    let engine = MockEngine::new(MockConfig {
        name: name.into(),
        ttft: Duration::from_millis(ttft_ms),
        tpot: Duration::from_millis(tpot_ms),
        default_output_tokens: output_tokens,
        contention: None,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(engine.clone().serve(listener));
    Engine { url, engine }
}

struct Setup {
    cus: u32,
    wu_per_cu: f64,
    output_p95: u64,
    policy: BoundaryPolicy,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            cus: 100,
            wu_per_cu: 1_000.0,
            output_p95: 100,
            policy: BoundaryPolicy::default(),
        }
    }
}

fn config(setup: Setup, engine_url: &str, payg_url: Option<&str>) -> GatewayConfig {
    GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: engine_url.into(),
            payg_engine_url: payg_url.map(Into::into),
            usage_log: None,
            wu_per_cu: setup.wu_per_cu,
        },
        // Simple weights so WU arithmetic is easy to follow; no KV term.
        profiles: vec![PerformanceProfile {
            name: "test-profile".into(),
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
        usage_export: None,
        reservations: vec![ReservationConfig {
            id: "res-1".into(),
            tenant: "acme".into(),
            model: MODEL.into(),
            cus: setup.cus,
            tier: Tier::Interactive,
            profile: "test-profile".into(),
            shape: Shape {
                input_p95: 4_000,
                input_max: 8_000,
                output_p95: setup.output_p95,
                context_ceiling: 16_000,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
        }],
        deployments: vec![DeploymentConfig {
            id: "dep-1".into(),
            reservation: "res-1".into(),
            api_key: KEY.into(),
            boundary_policy: setup.policy,
            max_share: None,
        }],
    }
}

async fn spawn_gateway(config: GatewayConfig) -> (String, Arc<MemorySink>) {
    let sink = Arc::new(MemorySink::default());
    let app = AppState::new(&config, sink.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router(app)).await });
    (url, sink)
}

/// Usage records are emitted when the response body is dropped, so poll briefly.
async fn wait_for_records(sink: &MemorySink, n: usize) -> Vec<UsageRecord> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let records = sink.records();
        if records.len() >= n || Instant::now() > deadline {
            assert_eq!(records.len(), n, "usage records: {records:#?}");
            return records;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn chat(gateway: &str, body: Value) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .post(format!("{gateway}/v1/chat/completions"))
        .bearer_auth(KEY)
        .json(&body)
}

fn hello(max_tokens: u64, stream: bool) -> Value {
    json!({
        "model": MODEL,
        "messages": [{ "role": "user", "content": "hello" }],
        "max_tokens": max_tokens,
        "stream": stream,
    })
}

fn header<'a>(resp: &'a reqwest::Response, name: &str) -> &'a str {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

#[tokio::test]
async fn streams_tokens_and_settles_on_engine_usage() {
    let e = spawn_engine("main", 20, 2, 64).await;
    let (gw, sink) = spawn_gateway(config(Setup::default(), &e.url, None)).await;

    let resp = chat(&gw, hello(8, true)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(header(&resp, "x-pt-class"), "provisioned");
    assert!(header(&resp, "content-type").starts_with("text/event-stream"));
    let body = resp.text().await.unwrap();
    assert_eq!(body.matches("\"content\":\"tok \"").count(), 8);
    assert!(body.trim_end().ends_with("data: [DONE]"));
    // The gateway asked the engine for usage; the client didn't, so it's stripped.
    assert!(!body.contains("\"usage\""));

    let r = &wait_for_records(&sink, 1).await[0];
    assert_eq!(r.outcome, Outcome::Ok);
    assert_eq!(r.class, Some(TrafficClass::Provisioned));
    // "user" (1) + "hello" (2) + 3 framing = 6 prompt tokens.
    assert_eq!(r.tokens.uncached_prefill, 6);
    assert_eq!(r.tokens.decode, 8);
    assert!(r.timings.ttft_ms.unwrap() >= 20.0);
    assert!(r.timings.tpot_ms.is_some());
    // a·6 + c·8, no KV term. Estimated with max_tokens=8, so the estimate matches.
    assert!((r.wu_actual - 14.0).abs() < 1e-9);
    assert!((r.wu_estimated - 14.0).abs() < 1e-9);
    assert!(r.in_shape);
}

#[tokio::test]
async fn forwards_usage_chunk_when_client_asks() {
    let e = spawn_engine("main", 1, 1, 4).await;
    let (gw, _) = spawn_gateway(config(Setup::default(), &e.url, None)).await;
    let mut body = hello(4, true);
    body["stream_options"] = json!({ "include_usage": true });
    let text = chat(&gw, body).send().await.unwrap().text().await.unwrap();
    assert!(text.contains("\"usage\""));
}

#[tokio::test]
async fn non_streaming_completion() {
    let e = spawn_engine("main", 5, 1, 64).await;
    let (gw, sink) = spawn_gateway(config(Setup::default(), &e.url, None)).await;

    let resp = chat(&gw, hello(5, false)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["usage"]["completion_tokens"], 5);

    let r = &wait_for_records(&sink, 1).await[0];
    assert_eq!(r.outcome, Outcome::Ok);
    assert_eq!(r.tokens.decode, 5);
    assert!(r.timings.tpot_ms.is_none());
}

#[tokio::test]
async fn rejects_unknown_key_and_wrong_model() {
    let e = spawn_engine("main", 1, 1, 4).await;
    let (gw, sink) = spawn_gateway(config(Setup::default(), &e.url, None)).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{gw}/v1/chat/completions"))
        .bearer_auth("sk-wrong")
        .json(&hello(4, false))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    let mut body = hello(4, false);
    body["model"] = json!("other-model");
    let resp = chat(&gw, body).send().await.unwrap();
    assert_eq!(resp.status(), 400);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["error"]["code"], "model_not_found");

    assert_eq!(e.engine.requests_served(), 0);
    assert!(sink.records().is_empty());
}

/// 1 CU of 20 WU/s: the bucket holds 20 WU. A 56 WU request (6 prompt + 50 decode) is
/// admitted from a full bucket and leaves it 36 WU in debt, so the next request is over
/// entitlement.
fn tight(policy: BoundaryPolicy) -> Setup {
    Setup {
        cus: 1,
        wu_per_cu: 20.0,
        policy,
        ..Default::default()
    }
}

#[tokio::test]
async fn over_entitlement_is_rejected_with_retry_after() {
    let e = spawn_engine("main", 1, 1, 50).await;
    let (gw, sink) = spawn_gateway(config(tight(BoundaryPolicy::default()), &e.url, None)).await;

    let first = chat(&gw, hello(50, false)).send().await.unwrap();
    assert_eq!(first.status(), 200);
    first.bytes().await.unwrap();

    let second = chat(&gw, hello(50, false)).send().await.unwrap();
    assert_eq!(second.status(), 429);
    assert_eq!(header(&second, "x-pt-reason"), "entitlement_exhausted");
    let retry: u64 = header(&second, "retry-after").parse().unwrap();
    assert!(retry >= 1);
    assert_eq!(header(&second, "x-pt-entitlement-remaining"), "0");

    let records = wait_for_records(&sink, 2).await;
    let rejected = records.iter().find(|r| r.class.is_none()).unwrap();
    assert_eq!(
        rejected.outcome,
        Outcome::Rejected(RejectReason::EntitlementExhausted)
    );
    assert_eq!(e.engine.requests_served(), 1);
}

#[tokio::test]
async fn spillover_goes_to_the_payg_engine() {
    let main = spawn_engine("main", 1, 1, 50).await;
    let payg = spawn_engine("payg", 1, 1, 50).await;
    let policy = BoundaryPolicy {
        spillover: true,
        ..Default::default()
    };
    let (gw, sink) = spawn_gateway(config(tight(policy), &main.url, Some(&payg.url))).await;

    let first = chat(&gw, hello(50, false)).send().await.unwrap();
    assert_eq!(header(&first, "x-pt-class"), "provisioned");
    first.bytes().await.unwrap();

    let second = chat(&gw, hello(50, false)).send().await.unwrap();
    assert_eq!(second.status(), 200);
    assert_eq!(header(&second, "x-pt-class"), "spillover");
    second.bytes().await.unwrap();

    assert_eq!(main.engine.requests_served(), 1);
    assert_eq!(payg.engine.requests_served(), 1);
    let records = wait_for_records(&sink, 2).await;
    assert!(records
        .iter()
        .any(|r| r.class == Some(TrafficClass::Spillover)));
}

#[tokio::test]
async fn queue_policy_waits_for_refill() {
    // 200 WU/s. The first request (6 + 300 = 306 WU) leaves the bucket 106 WU in debt, so
    // the second waits roughly half a second, within the 2 s deadline.
    let e = spawn_engine("main", 1, 0, 300).await;
    let setup = Setup {
        cus: 1,
        wu_per_cu: 200.0,
        output_p95: 300,
        policy: BoundaryPolicy {
            queue: Some(QueuePolicy::default()),
            ..Default::default()
        },
    };
    let (gw, sink) = spawn_gateway(config(setup, &e.url, None)).await;

    chat(&gw, hello(300, false))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let second = chat(&gw, hello(10, false)).send().await.unwrap();
    assert_eq!(second.status(), 200);
    assert_eq!(header(&second, "x-pt-class"), "provisioned");
    let queue_ms: u64 = header(&second, "x-pt-queue-ms").parse().unwrap();
    assert!(queue_ms >= 100, "queued {queue_ms} ms");
    second.bytes().await.unwrap();

    let records = wait_for_records(&sink, 2).await;
    assert!(records.iter().any(|r| r.timings.queue_ms >= 100.0));
}

#[tokio::test]
async fn cached_prefix_is_charged_at_the_cached_rate() {
    let e = spawn_engine("main", 1, 1, 4).await;
    let (gw, sink) = spawn_gateway(config(Setup::default(), &e.url, None)).await;
    let system = json!({ "role": "system", "content": "You are a planning agent. ".repeat(40) });
    let turn1 = json!({ "role": "user", "content": "Find flights to Lisbon." });

    let first = json!({ "model": MODEL, "messages": [system, turn1], "max_tokens": 4 });
    chat(&gw, first)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let second = json!({
        "model": MODEL,
        "messages": [system, turn1, { "role": "assistant", "content": "tok tok tok tok " },
                     { "role": "user", "content": "Now book the cheapest." }],
        "max_tokens": 4,
    });
    let resp = chat(&gw, second)
        .header("x-pt-session-id", "s-1")
        .send()
        .await
        .unwrap();
    resp.bytes().await.unwrap();

    let records = wait_for_records(&sink, 2).await;
    let (a, b) = (&records[0], &records[1]);
    assert_eq!(a.tokens.cached_prefill, 0);
    assert_eq!(b.tokens.cached_prefill, a.tokens.uncached_prefill);
    assert_eq!(b.session_id.as_deref(), Some("s-1"));
    // The second prompt is longer, but most of it is cached at b = 0.1.
    assert!(b.wu_actual < a.wu_actual);
}

#[tokio::test]
async fn client_disconnect_mid_stream_settles_what_was_sent() {
    let e = spawn_engine("main", 1, 5, 200).await;
    let (gw, sink) = spawn_gateway(config(Setup::default(), &e.url, None)).await;

    let resp = chat(&gw, hello(200, true)).send().await.unwrap();
    let mut stream = resp.bytes_stream();
    stream.next().await.unwrap().unwrap();
    drop(stream);

    let r = &wait_for_records(&sink, 1).await[0];
    assert_eq!(r.outcome, Outcome::ClientCancelled);
    assert!(r.tokens.decode < 200, "decode {}", r.tokens.decode);
    assert_eq!(r.tokens.uncached_prefill, 6);
}

#[tokio::test]
async fn status_reports_entitlement() {
    let e = spawn_engine("main", 1, 1, 4).await;
    let (gw, _) = spawn_gateway(config(Setup::default(), &e.url, None)).await;
    let v: Value = reqwest::Client::new()
        .get(format!("{gw}/v1/pt/status"))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["deployment"], "dep-1");
    assert_eq!(v["tier"], "interactive");
    assert_eq!(v["entitlement_wu_per_s"], 100_000.0);
}

#[tokio::test]
async fn cap_is_refunded_when_the_shared_bucket_rejects() {
    let e = spawn_engine("main", 1, 1, 50).await;
    let mut cfg = config(tight(BoundaryPolicy::default()), &e.url, None);
    cfg.deployments.push(DeploymentConfig {
        id: "dep-staging".into(),
        reservation: "res-1".into(),
        api_key: "sk-staging".into(),
        boundary_policy: BoundaryPolicy::default(),
        max_share: Some(1.0),
    });
    let sink = Arc::new(MemorySink::default());
    let app = AppState::new(&cfg, sink.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gw = format!("http://{}", listener.local_addr().unwrap());
    let routes = router(app.clone());
    tokio::spawn(async move { axum::serve(listener, routes).await });

    // Prod uses up the shared 20 WU/s bucket (one 56 WU request from a full bucket).
    assert_eq!(
        chat(&gw, hello(50, false)).send().await.unwrap().status(),
        200
    );
    let cap_level = || {
        let d = app.deployment_for_key("sk-staging").unwrap();
        d.cap.as_ref().unwrap().status(Instant::now()).level_wu
    };
    let full = cap_level();

    // Staging's own cap has room, but the shared bucket doesn't.
    let r = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .bearer_auth("sk-staging")
        .json(&hello(50, false))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    assert_eq!(r.headers()["x-pt-reason"], "entitlement_exhausted");
    wait_for_records(&sink, 2).await;
    assert!(
        (cap_level() - full).abs() < 1.0,
        "cap refunded: {} vs {full}",
        cap_level()
    );
}
