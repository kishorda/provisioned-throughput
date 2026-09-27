//! Workers and hot spares found through DNS (ADR-044), against real mock workers. IP
//! literals stand in for the pool's headless Services: they resolve without a DNS server,
//! and 127.0.0.2 is loopback too, so floor and spares get different addresses.

use std::time::Duration;

use pt_mock_engine::{MockConfig, MockEngine};
use pt_router::config::{DiscoveryConfig, RouterConfig};
use pt_router::discovery::Discovery;
use pt_router::http::{self, Shared};
use serde_json::{json, Value};

async fn serve_on(ip: &str, app: axum::Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind((ip, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

async fn worker(ip: &str) -> std::net::SocketAddr {
    let e = MockEngine::new(MockConfig {
        name: ip.into(),
        ttft: Duration::from_millis(1),
        tpot: Duration::from_millis(1),
        default_output_tokens: 2,
        contention: None,
    });
    serve_on(ip, e.router()).await
}

fn discovery(workers: &str, spares: &str) -> DiscoveryConfig {
    DiscoveryConfig {
        workers_dns: workers.into(),
        spares_dns: Some(spares.into()),
        slots: 4,
        kv_blocks: 1000,
        refresh_secs: 5,
    }
}

fn config(d: DiscoveryConfig) -> RouterConfig {
    let c: RouterConfig = toml::from_str(r#"listen = "127.0.0.1:0""#).unwrap();
    let c = RouterConfig {
        discovery: Some(d),
        ..c
    };
    c.validate().unwrap();
    c
}

async fn send(router: &str, class: &str) -> String {
    let resp = reqwest::Client::new()
        .post(format!("{router}/v1/chat/completions"))
        .header("x-pt-reservation", "r")
        .header("x-pt-class", class)
        .json(&json!({ "model": "m", "max_tokens": 2,
            "messages": [{ "role": "user", "content": format!("{class} {}", uuid::Uuid::new_v4()) }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.headers()["x-pt-router-worker"]
        .to_str()
        .unwrap()
        .to_string()
}

async fn status(router: &str) -> Vec<(String, bool, bool)> {
    let s: Value = reqwest::get(format!("{router}/v1/router/status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    s["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| {
            (
                w["id"].as_str().unwrap().to_string(),
                w["hot_spare"].as_bool().unwrap(),
                w["retired"].as_bool().unwrap(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovered_spares_take_payg_and_follow_their_label() {
    let floor = worker("127.0.0.1").await;
    let spare = worker("127.0.0.2").await;
    let cfg = config(discovery(&floor.to_string(), &spare.to_string()));
    let shared = Shared::new(&cfg);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let router = format!("http://{}", listener.local_addr().unwrap());
    let app = http::router(shared.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let mut d = Discovery::new(cfg.discovery.clone().unwrap());
    d.refresh(&shared).await;
    assert_eq!(
        status(&router).await,
        [
            (floor.to_string(), false, false),
            (spare.to_string(), true, false)
        ]
    );
    // PAYG prefers the hot spare, provisioned the floor.
    assert_eq!(send(&router, "payg").await, spare.to_string());
    assert_eq!(send(&router, "provisioned").await, floor.to_string());

    // The controller moves the labels: the spare becomes floor and the old floor pod is
    // gone, so the spares Service has no endpoints (its name doesn't resolve). The old
    // worker is retired, and everything goes to the other.
    let mut d = Discovery::new(discovery(&spare.to_string(), "no-spares.invalid:8000"));
    d.refresh(&shared).await;
    assert_eq!(
        status(&router).await,
        [
            (floor.to_string(), false, true),
            (spare.to_string(), false, false)
        ]
    );
    for class in ["payg", "provisioned"] {
        assert_eq!(send(&router, class).await, spare.to_string());
    }
}

#[test]
fn discovery_config_rules() {
    let bad = |toml: &str| {
        let c: RouterConfig = toml::from_str(toml).unwrap();
        c.validate().unwrap_err().to_string()
    };
    let disc = r#"
        [discovery]
        workers_dns = "maverick-workers.pt-serving.svc:8000"
        spares_dns = "maverick-spares.pt-serving.svc:8000"
        slots = 8
        kv_blocks = 4096
    "#;
    let ok: RouterConfig = toml::from_str(&format!("listen = \"x:1\"\n{disc}")).unwrap();
    ok.validate().unwrap();
    assert_eq!(ok.discovery.as_ref().unwrap().refresh_secs, 5);
    assert!(bad(&format!(
        "listen = \"x:1\"\n[[workers]]\nid = \"w\"\nurl = \"http://w\"\nslots = 1\nkv_blocks = 1\n{disc}"
    ))
    .contains("not both"));
    assert!(bad(
        "listen = \"x:1\"\n[discovery]\nworkers_dns = \"nohost\"\nslots = 1\nkv_blocks = 1"
    )
    .contains("host:port"));
    assert!(bad(&format!(
        "listen = \"x:1\"\n[[allocations]]\nreservation = \"r\"\nwu_per_sec = 1.0\ndedicated_workers = [\"w\"]\n{disc}"
    ))
    .contains("dedicated_workers"));
    assert!(bad("listen = \"x:1\"").contains("at least one worker"));
}
