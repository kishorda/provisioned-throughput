//! Two gateway replicas share one reservation through a real Quota Coordinator.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::response::IntoResponse;
use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile, Shape, Tier};
use pt_gateway::config::{DeploymentConfig, QuotaClientConfig, ReservationConfig, ServerConfig};
use pt_gateway::quota::QuotaClient;
use pt_gateway::state::QuotaMode;
use pt_gateway::usage::MemorySink;
use pt_gateway::{router, AppState, GatewayConfig};
use pt_mock_engine::{MockConfig, MockEngine};
use pt_quota::wire::{Lease, RenewResponse};
use pt_quota::{api as quota_api, Coordinator, CoordinatorConfig};
use serde_json::{json, Value};

const KEY: &str = "sk-shared";
const TOKEN: &str = "quota-token";
/// 10 CUs × 100 WU/s.
const ENTITLEMENT: f64 = 1_000.0;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

/// A coordinator that can be switched off (every request gets 503).
async fn spawn_coordinator() -> (String, Arc<Coordinator>, Arc<AtomicBool>) {
    let coordinator = Arc::new(Coordinator::new(CoordinatorConfig {
        lease_ttl: Duration::from_millis(200),
        floor_fraction: 0.1,
    }));
    let down = Arc::new(AtomicBool::new(false));
    let flag = down.clone();
    let app = quota_api::router(coordinator.clone(), TOKEN).layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let flag = flag.clone();
            async move {
                if flag.load(Ordering::SeqCst) {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                next.run(req).await
            }
        },
    ));
    (serve(app).await, coordinator, down)
}

fn config(engine: &str, coordinator: &str, gateway_id: &str) -> GatewayConfig {
    GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: engine.into(),
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: 100.0,
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
        quota: Some(QuotaClientConfig {
            coordinator_url: coordinator.into(),
            token: TOKEN.into(),
            gateway_id: Some(gateway_id.into()),
            renew_interval_ms: 50,
            assumed_gateways: 2,
            fallback_decay_secs: 1.0,
        }),
        usage_export: None,
        reservations: vec![ReservationConfig {
            id: "res-1".into(),
            tenant: "acme".into(),
            model: "m".into(),
            cus: 10,
            tier: Tier::Interactive,
            profile: "p".into(),
            shape: Shape {
                input_p95: 1_000,
                input_max: 4_000,
                output_p95: 4,
                context_ceiling: 8_000,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
        }],
        deployments: vec![DeploymentConfig {
            id: "dep-1".into(),
            reservation: "res-1".into(),
            api_key: KEY.into(),
            boundary_policy: Default::default(),
        }],
    }
}

async fn spawn_gateway(config: GatewayConfig) -> (String, AppState) {
    let app = AppState::new(&config, Arc::new(MemorySink::default())).unwrap();
    let client = QuotaClient::new(config.quota.clone().unwrap()).unwrap();
    tokio::spawn(client.run(app.clone()));
    (serve(router(app.clone())).await, app)
}

async fn status(gw: &str) -> Value {
    reqwest::Client::new()
        .get(format!("{gw}/v1/pt/status"))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn share(gw: &str) -> (f64, String) {
    let s = status(gw).await;
    (
        s["local_share_wu_per_s"].as_f64().unwrap(),
        s["quota"].as_str().unwrap().to_string(),
    )
}

async fn eventually<F, Fut>(what: &str, secs: u64, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Keep `gw` busy with ~500 WU requests until `stop` is set.
fn load(gw: String, stop: Arc<AtomicBool>) -> Vec<tokio::task::JoinHandle<()>> {
    (0..8)
        .map(|_| {
            let gw = gw.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                let body = json!({
                    "model": "m",
                    "max_tokens": 4,
                    "messages": [{ "role": "user", "content": "x".repeat(2_000) }],
                });
                while !stop.load(Ordering::SeqCst) {
                    let _ = client
                        .post(format!("{gw}/v1/chat/completions"))
                        .bearer_auth(KEY)
                        .json(&body)
                        .send()
                        .await;
                }
            })
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicas_share_one_entitlement() {
    let engine = serve(
        MockEngine::new(MockConfig {
            name: "main".into(),
            ttft: Duration::from_millis(1),
            tpot: Duration::from_millis(1),
            default_output_tokens: 4,
        })
        .router(),
    )
    .await;
    let (coord_url, coordinator, down) = spawn_coordinator().await;
    let (a, _) = spawn_gateway(config(&engine, &coord_url, "gw-a")).await;
    let (b, _) = spawn_gateway(config(&engine, &coord_url, "gw-b")).await;

    // Idle: both leased, roughly half each, never more than the entitlement in total.
    eventually("both leased and balanced", 5, || async {
        let ((sa, ma), (sb, mb)) = (share(&a).await, share(&b).await);
        ma == "lease" && mb == "lease" && (sa - 500.0).abs() < 60.0 && (sb - 500.0).abs() < 60.0
    })
    .await;

    // Load on A: capacity moves to A, B keeps its floor.
    let stop = Arc::new(AtomicBool::new(false));
    let workers = load(a.clone(), stop.clone());
    // Sample the coordinator's total grant throughout the rebalance.
    let max_granted = Arc::new(std::sync::Mutex::new(0.0_f64));
    let sampler = {
        let (coordinator, stop, max) = (coordinator.clone(), stop.clone(), max_granted.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                if let Some(r) = coordinator.view(Instant::now()).first() {
                    let mut m = max.lock().unwrap();
                    *m = m.max(r.granted_wu_s);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    eventually("capacity moved to the busy replica", 10, || async {
        let ((sa, _), (sb, _)) = (share(&a).await, share(&b).await);
        sa > 850.0 && sb < 150.0
    })
    .await;
    stop.store(true, Ordering::SeqCst);
    for w in workers {
        let _ = w.await;
    }
    let _ = sampler.await;
    let max_granted = *max_granted.lock().unwrap();
    assert!(max_granted > 0.0, "sampler saw grants");
    assert!(
        max_granted <= ENTITLEMENT + 1e-6,
        "coordinator oversold: {max_granted}"
    );
    let ((sa, _), (sb, _)) = (share(&a).await, share(&b).await);
    assert!(sa + sb <= ENTITLEMENT * 1.01, "local shares {sa} + {sb}");

    // Coordinator down: leases expire and both decay to 50% × 1,000 ÷ 2 = 250.
    down.store(true, Ordering::SeqCst);
    eventually("fallback shares", 5, || async {
        let ((sa, ma), (sb, mb)) = (share(&a).await, share(&b).await);
        ma == "fallback" && mb == "fallback" && (sa - 250.0).abs() < 5.0 && (sb - 250.0).abs() < 5.0
    })
    .await;

    // Back up: leases resume.
    down.store(false, Ordering::SeqCst);
    eventually("leases resumed", 5, || async {
        share(&a).await.1 == "lease" && share(&b).await.1 == "lease"
    })
    .await;
}

#[tokio::test]
async fn local_rate_follows_lease_then_decays() {
    let mut cfg = config("http://127.0.0.1:1", "http://127.0.0.1:1", "gw-x");
    cfg.quota.as_mut().unwrap().fallback_decay_secs = 10.0;
    let app = AppState::new(&cfg, Arc::new(MemorySink::default())).unwrap();
    let t0 = Instant::now();

    // Before any lease: entitlement ÷ assumed gateways (2).
    assert_eq!(
        app.local_rate("res-1", ENTITLEMENT, t0),
        (500.0, QuotaMode::Unleased)
    );
    let res = app.entitlements();
    let r = res.reservations().next().unwrap();
    assert_eq!(r.limiter.config().entitlement_wu_s, 500.0);

    let lease = RenewResponse {
        ttl_ms: 1_000,
        leases: vec![Lease {
            id: "res-1".into(),
            rate_wu_s: 900.0,
            active_gateways: 4,
        }],
    };
    app.apply_leases(&lease, t0);
    assert_eq!(
        app.local_rate("res-1", ENTITLEMENT, t0),
        (900.0, QuotaMode::Lease)
    );
    assert_eq!(
        r.limiter.config().entitlement_wu_s,
        900.0,
        "limiter resized in place"
    );

    // Expired at t0 + 1 s. Halfway through the 10 s decay: between 900 and 125.
    let (mid, mode) = app.local_rate("res-1", ENTITLEMENT, t0 + Duration::from_secs(6));
    assert_eq!(mode, QuotaMode::Fallback);
    assert!(
        (mid - (900.0 + (125.0 - 900.0) * 0.5)).abs() < 1e-6,
        "{mid}"
    );
    // Fully decayed: 50% × 1,000 ÷ 4 gateways.
    let (end, _) = app.local_rate("res-1", ENTITLEMENT, t0 + Duration::from_secs(60));
    assert!((end - 125.0).abs() < 1e-6, "{end}");

    // A lease above the entitlement (after a shrink) is capped.
    let big = RenewResponse {
        ttl_ms: 1_000,
        leases: vec![Lease {
            id: "res-1".into(),
            rate_wu_s: 5_000.0,
            active_gateways: 1,
        }],
    };
    app.apply_leases(&big, t0);
    assert_eq!(app.local_rate("res-1", ENTITLEMENT, t0).0, ENTITLEMENT);
}
