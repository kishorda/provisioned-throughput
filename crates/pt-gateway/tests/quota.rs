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
use pt_quota::election::{self, ElectionConfig, Elector, Leadership, MemoryLease};
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
    let app = quota_api::router(coordinator.clone(), Leadership::always(), TOKEN).layer(
        axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let flag = flag.clone();
                async move {
                    if flag.load(Ordering::SeqCst) {
                        return StatusCode::SERVICE_UNAVAILABLE.into_response();
                    }
                    next.run(req).await
                }
            },
        ),
    );
    (serve(app).await, coordinator, down)
}

fn config(engine: &str, coordinator: &str, gateway_id: &str) -> GatewayConfig {
    config_with(engine, &[coordinator], gateway_id)
}

/// A gateway that knows several coordinator replicas; the first URL is tried first.
fn config_with(engine: &str, coordinators: &[&str], gateway_id: &str) -> GatewayConfig {
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
            coordinator_url: coordinators[0].into(),
            standby_urls: coordinators[1..].iter().map(|u| u.to_string()).collect(),
            token: TOKEN.into(),
            gateway_id: Some(gateway_id.into()),
            renew_interval_ms: 50,
            assumed_gateways: 2,
            fallback_decay_secs: 1.0,
        }),
        tokenization: Default::default(),
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
            max_share: None,
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
            contention: None,
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
        warming_up: false,
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
        warming_up: false,
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

/// One coordinator replica in an active/standby pair.
struct Replica {
    url: String,
    coordinator: Arc<Coordinator>,
    leadership: Arc<Leadership>,
    lease: MemoryLease,
    identity: &'static str,
    down: Arc<AtomicBool>,
    election: Option<(
        tokio::task::JoinHandle<()>,
        tokio::sync::watch::Sender<bool>,
    )>,
}

/// Grants live 400 ms (held 600 ms); a dead leader is replaced after 1.2 s.
const HA_TTL: Duration = Duration::from_millis(400);

impl Replica {
    async fn spawn(identity: &'static str, lease: MemoryLease) -> Self {
        let coordinator = Arc::new(Coordinator::new(CoordinatorConfig {
            lease_ttl: HA_TTL,
            floor_fraction: 0.1,
        }));
        let leadership = Leadership::elected();
        let down = Arc::new(AtomicBool::new(false));
        let flag = down.clone();
        let app = quota_api::router(coordinator.clone(), leadership.clone(), TOKEN).layer(
            axum::middleware::from_fn(
                move |req: axum::extract::Request, next: axum::middleware::Next| {
                    let flag = flag.clone();
                    async move {
                        if flag.load(Ordering::SeqCst) {
                            return StatusCode::BAD_GATEWAY.into_response();
                        }
                        next.run(req).await
                    }
                },
            ),
        );
        let mut r = Self {
            url: serve(app).await,
            coordinator,
            leadership,
            lease,
            identity,
            down,
            election: None,
        };
        r.start();
        r
    }

    fn start(&mut self) {
        let config = ElectionConfig {
            identity: self.identity.into(),
            lease_duration: Duration::from_millis(1_200),
            renew_deadline: Duration::from_millis(500),
            retry_period: Duration::from_millis(100),
        };
        config
            .validate(HA_TTL * 3 / 2)
            .expect("safe election timing");
        let (tx, mut rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(election::run(
            Elector::new(self.lease.clone(), config),
            self.coordinator.clone(),
            self.leadership.clone(),
            async move {
                let _ = rx.wait_for(|s| *s).await;
            },
        ));
        self.down.store(false, Ordering::SeqCst);
        self.election = Some((task, tx));
    }

    fn leading(&self) -> bool {
        self.leadership.serving(Instant::now())
    }

    /// The process dies: no release, no answers.
    fn crash(&mut self) {
        if let Some((task, _)) = self.election.take() {
            task.abort();
        }
        self.down.store(true, Ordering::SeqCst);
    }

    /// A rolling update: stop serving, release the lease, exit.
    async fn shut_down(&mut self) {
        if let Some((task, tx)) = self.election.take() {
            tx.send(true).unwrap();
            task.await.unwrap();
        }
        self.down.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_coordinator_takes_over_without_overselling() {
    let engine = serve(
        MockEngine::new(MockConfig {
            name: "main".into(),
            ttft: Duration::from_millis(1),
            tpot: Duration::from_millis(1),
            default_output_tokens: 4,
            contention: None,
        })
        .router(),
    )
    .await;
    let lease = MemoryLease::default();
    let mut c1 = Replica::spawn("c1", lease.clone()).await;
    eventually("c1 leads", 5, || async { c1.leading() }).await;
    let mut c2 = Replica::spawn("c2", lease).await;
    // Gateways try the standby first, so they must follow not_leader to the leader.
    let urls = [c2.url.as_str(), c1.url.as_str()];
    let (a, app_a) = spawn_gateway(config_with(&engine, &urls, "gw-a")).await;
    let (b, app_b) = spawn_gateway(config_with(&engine, &urls, "gw-b")).await;

    let standby = reqwest::get(format!("{}/leader", c2.url)).await.unwrap();
    assert_eq!(standby.status(), 503);
    let refused = reqwest::Client::new()
        .post(format!("{}/v1/leases/renew", c2.url))
        .bearer_auth(TOKEN)
        .json(&json!({ "gateway_id": "x", "reservations": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 503);
    assert_eq!(
        refused.json::<Value>().await.unwrap()["error"]["code"],
        "not_leader"
    );

    eventually("both leased from c1", 5, || async {
        share(&a).await.1 == "lease" && share(&b).await.1 == "lease"
    })
    .await;

    // Throughout, the leases the gateways hold never add up to more than the entitlement,
    // and we count how often a gateway is left without one.
    let stop = Arc::new(AtomicBool::new(false));
    let unleased = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let max_leased = Arc::new(std::sync::Mutex::new(0.0_f64));
    let sampler = {
        let (stop, unleased, max) = (stop.clone(), unleased.clone(), max_leased.clone());
        let apps = [app_a.clone(), app_b.clone()];
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                let now = Instant::now();
                let mut sum = 0.0;
                for app in &apps {
                    let (rate, mode) = app.local_rate("res-1", ENTITLEMENT, now);
                    if mode == QuotaMode::Lease {
                        sum += rate;
                    } else {
                        unleased.fetch_add(1, Ordering::SeqCst);
                    }
                }
                {
                    let mut m = max.lock().unwrap();
                    *m = m.max(sum);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let load_stop = Arc::new(AtomicBool::new(false));
    let workers = load(a.clone(), load_stop.clone());

    // 1. The leader crashes. Leases lapse into fallback, and c2 takes over once the lease
    // record has been unchanged for 1.2 s.
    c1.crash();
    eventually("c2 leads", 5, || async { c2.leading() }).await;
    assert!(
        !c2.coordinator.warming_up(Instant::now()),
        "a dead leader's grants have all expired: no warm-up needed"
    );
    eventually("leases from c2", 5, || async {
        share(&a).await.1 == "lease" && share(&b).await.1 == "lease"
    })
    .await;
    assert!(
        unleased.load(Ordering::SeqCst) > 0,
        "the crash caused a gap"
    );

    // 2. c1 comes back as a standby, then c2 is shut down for an update. It releases the
    // lease, and c1 takes over at once, warming up from the leases the gateways hold.
    c1.start();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!c1.leading(), "c1 stands by while c2 leads");
    eventually("rebalanced under c2", 5, || async {
        share(&a).await.0 > 700.0
    })
    .await;
    unleased.store(0, Ordering::SeqCst);
    c2.shut_down().await;
    eventually("c1 leads again", 2, || async { c1.leading() }).await;
    assert!(c1.coordinator.warming_up(Instant::now()));
    // Past the warm-up, allocation is normal again.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(!c1.coordinator.warming_up(Instant::now()));
    assert_eq!(share(&a).await.1, "lease");
    assert_eq!(share(&b).await.1, "lease");
    assert_eq!(
        unleased.load(Ordering::SeqCst),
        0,
        "a graceful handover leaves no gateway without a lease"
    );

    load_stop.store(true, Ordering::SeqCst);
    for w in workers {
        let _ = w.await;
    }
    stop.store(true, Ordering::SeqCst);
    sampler.await.unwrap();
    let max = *max_leased.lock().unwrap();
    assert!(max > 0.0);
    assert!(max <= ENTITLEMENT + 1e-6, "leases oversold: {max}");
}
