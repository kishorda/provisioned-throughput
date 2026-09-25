//! The interference suite (docs/05 §7, ADR-025): a well-behaved tenant's SLO must hold
//! next to noisy ones.
//!
//! Every scenario runs twice against a mock engine that models continuous batching
//! (`pt_mock_engine::contention`):
//! - **protected:** clients → gateway (admission) → router (priority, WFQ, KV budgets,
//!   pull-based dispatch) → engine. Tenant A's TTFT and TPOT p95 must stay within its tier.
//! - **control:** the same load straight at the engine, with no isolation. A must miss its
//!   SLO, which proves the scenario really creates interference and the suite can catch it.
//!
//! Each scenario runs for a few seconds in `cargo test`. The `soak_*` tests (`--ignored`)
//! run the same scenarios for `PT_SOAK_SECS` (default 60) as a release gate.
//! Latencies are measured at the client, which is the gateway's vantage point plus
//! loopback.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use pt_admission::BoundaryPolicy;
use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile, Shape, Tier};
use pt_gateway::config::{DeploymentConfig, ReservationConfig, ServerConfig};
use pt_gateway::usage::MemorySink;
use pt_gateway::{router as gateway_router, AppState, GatewayConfig};
use pt_mock_engine::contention::Contention;
use pt_mock_engine::{MockConfig, MockEngine};
use pt_router::config::{AllocationConfig, RouterConfig, WorkerConfig};
use pt_router::http;
use serde_json::json;
use tokio::sync::Mutex;

/// Scenarios measure latency, so they run one at a time.
static SERIAL: Mutex<()> = Mutex::const_new(());

const TIER: Tier = Tier::Interactive;
/// Tenant A: in-shape chat.
const A_PROMPT: usize = 1_000;
const A_OUTPUT: u64 = 16;
const A_CLIENTS: usize = 4;
/// The engine's router-facing size.
const SLOTS: u32 = 16;
const KV_TOKENS: u64 = 64_000;
const BLOCK: u64 = 16;

fn engine_model(chunked: bool) -> Contention {
    Contention {
        max_batch: 256,
        step_base: Duration::from_millis(5),
        step_per_seq: Duration::from_micros(750),
        prefill_tokens_per_ms: 100.0,
        prefill_chunk: chunked.then_some(512),
        kv_capacity_tokens: KV_TOKENS,
    }
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

/// A prompt of about `tokens` tokens, unique so the engine's prefix cache can't help.
fn prompt(tokens: usize, tag: &str, n: u64) -> String {
    let head = format!("{tag}-{n}-{} ", uuid::Uuid::new_v4().simple());
    format!(
        "{head}{}",
        "x".repeat((tokens * 4).saturating_sub(head.len()))
    )
}

#[derive(Clone, Copy)]
struct Tenant {
    tag: &'static str,
    key: &'static str,
    prompt: usize,
    output: u64,
    clients: usize,
    /// Only for PAYG through the router (the PAYG front door isn't the PT gateway).
    payg: bool,
    /// Requests per second across all clients, or as fast as they complete.
    rate: Option<f64>,
    /// Vary output lengths by up to this many tokens, as real traffic does.
    output_jitter: u64,
}

/// A's reservation, and its request cost with the gateway's profile (a = 1, c = 3).
const A_CUS: u32 = 12;
const A_WU: f64 = A_PROMPT as f64 + 3.0 * A_OUTPUT as f64;

const A: Tenant = Tenant {
    tag: "a",
    key: "sk-a",
    prompt: A_PROMPT,
    output: A_OUTPUT,
    clients: A_CLIENTS,
    payg: false,
    // Steady, at 90% of its entitlement.
    rate: Some(0.9 * A_CUS as f64 * 1_000.0 / A_WU),
    output_jitter: 0,
};

/// One request's latencies, measured at the client.
#[derive(Debug, Clone, Copy)]
struct Sample {
    ok: bool,
    ttft_ms: f64,
    tpot_ms: f64,
}

async fn one(url: &str, t: Tenant, n: u64, direct: bool) -> Sample {
    let output = t.output
        + if t.output_jitter > 0 {
            n % (t.output_jitter + 1)
        } else {
            0
        };
    let mut req = reqwest::Client::new()
        .post(format!("{url}/v1/chat/completions"))
        .json(&json!({
            "model": "m", "stream": true, "max_tokens": output,
            "messages": [{ "role": "user", "content": prompt(t.prompt, t.tag, n) }],
        }));
    if t.payg && !direct {
        req = req
            .header("x-pt-class", "payg")
            .header("x-pt-reservation", "payg");
    } else if !direct {
        req = req.bearer_auth(t.key);
    }
    let start = Instant::now();
    let failed = Sample {
        ok: false,
        ttft_ms: 0.0,
        tpot_ms: 0.0,
    };
    let Ok(resp) = req.send().await else {
        return failed;
    };
    if !resp.status().is_success() {
        let _ = resp.bytes().await;
        return failed;
    }
    let mut stream = resp.bytes_stream();
    let (mut first, mut last, mut tokens) = (None, None, 0u64);
    while let Some(Ok(chunk)) = stream.next().await {
        let text = String::from_utf8_lossy(&chunk);
        let n = text.matches("\"content\":\"tok ").count() as u64;
        if n > 0 {
            let now = start.elapsed();
            first.get_or_insert(now);
            last = Some(now);
            tokens += n;
        }
    }
    let (Some(first), Some(last)) = (first, last) else {
        return failed;
    };
    Sample {
        ok: true,
        ttft_ms: first.as_secs_f64() * 1e3,
        tpot_ms: if tokens > 1 {
            (last - first).as_secs_f64() * 1e3 / (tokens - 1) as f64
        } else {
            0.0
        },
    }
}

/// Run `t`'s closed-loop clients against `url` until `until`.
fn load(
    url: String,
    t: Tenant,
    until: Instant,
    direct: bool,
) -> tokio::task::JoinHandle<Vec<Sample>> {
    tokio::spawn(async move {
        let clients = (0..t.clients).map(|c| {
            let url = url.clone();
            tokio::spawn(async move {
                let mut out = Vec::new();
                let mut n = 0;
                // Paced clients start staggered and keep a fixed interval.
                let interval = t
                    .rate
                    .map(|r| Duration::from_secs_f64(t.clients as f64 / r));
                let mut next = Instant::now()
                    + interval.map_or(Duration::ZERO, |i| i * c as u32 / t.clients as u32);
                while Instant::now() < until {
                    if let Some(i) = interval {
                        tokio::time::sleep_until(next.into()).await;
                        next += i;
                    }
                    out.push(one(&url, t, (c as u64) << 32 | n, direct).await);
                    n += 1;
                }
                out
            })
        });
        let mut all = Vec::new();
        for h in futures::future::join_all(clients).await {
            all.extend(h.unwrap());
        }
        all
    })
}

fn p95(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    v[((v.len() as f64 * 0.95).ceil() as usize).clamp(1, v.len()) - 1]
}

#[derive(Debug)]
struct Outcome {
    requests: usize,
    ok: usize,
    ttft_p95_ms: f64,
    tpot_p95_ms: f64,
    preemptions: u64,
}

impl Outcome {
    fn of(samples: &[Sample], preemptions: u64) -> Self {
        let ok: Vec<&Sample> = samples.iter().filter(|s| s.ok).collect();
        Self {
            requests: samples.len(),
            ok: ok.len(),
            ttft_p95_ms: p95(ok.iter().map(|s| s.ttft_ms).collect()),
            tpot_p95_ms: p95(ok.iter().map(|s| s.tpot_ms).collect()),
            preemptions,
        }
    }

    fn ttft_target() -> f64 {
        TIER.ttft_target_ms(A_PROMPT as u64, 0)
    }

    fn tpot_target() -> f64 {
        TIER.tpot_target_ms(A_OUTPUT)
    }

    fn meets_slo(&self) -> bool {
        self.ttft_p95_ms <= Self::ttft_target() && self.tpot_p95_ms <= Self::tpot_target()
    }
}

/// The protected stack: gateway (A, B, C reservations) → router → engine.
struct Stack {
    gateway: String,
    router: String,
    engine: MockEngine,
}

async fn stack(chunked: bool, allocations: Vec<AllocationConfig>) -> Stack {
    let engine = MockEngine::new(MockConfig {
        name: "engine".into(),
        ttft: Duration::ZERO,
        tpot: Duration::ZERO,
        default_output_tokens: 1_000,
        contention: Some(engine_model(chunked)),
    });
    let engine_url = serve(engine.router()).await;
    let router_cfg = RouterConfig {
        listen: "127.0.0.1:0".into(),
        block_size: BLOCK,
        default_max_tokens: 64,
        queue_timeout_ms: 30_000,
        payg_guard_every: 0,
        failover_hold_ms: 30_000,
        preempt_grace_ms: 250,
        backfill_ratio: pt_router::workers::DEFAULT_BACKFILL_RATIO,
        weights: None,
        workers: vec![WorkerConfig {
            id: "w0".into(),
            url: engine_url,
            slots: SLOTS,
            kv_blocks: (KV_TOKENS / BLOCK) as u32,
            hot_spare: false,
        }],
        allocations,
    };
    router_cfg.validate().unwrap();
    let router = serve(http::router(http::Shared::new(&router_cfg))).await;

    let reservation = |id: &str, cus: u32, input: u64, output: u64| ReservationConfig {
        id: id.into(),
        tenant: id.into(),
        model: "m".into(),
        cus,
        tier: TIER,
        profile: "p".into(),
        shape: Shape {
            input_p95: input,
            input_max: input * 2,
            output_p95: output,
            context_ceiling: 32_768,
            cache_hit_ratio: 0.0,
            burst_factor: 1.0,
        },
    };
    let deployment = |id: &str, key: &str| DeploymentConfig {
        id: format!("dep-{id}"),
        reservation: id.into(),
        api_key: key.into(),
        boundary_policy: BoundaryPolicy::default(),
        max_share: None,
    };
    let config = GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: router.clone(),
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: 1_000.0,
        },
        profiles: vec![PerformanceProfile {
            name: "p".into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 3.0,
                d: 0.0,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }],
        entitlements: None,
        quota: None,
        tokenization: Default::default(),
        prefix_cache: Default::default(),
        usage_export: None,
        // A at about its entitlement; B and C with small entitlements for their size.
        reservations: vec![
            reservation("a", A_CUS, A_PROMPT as u64, A_OUTPUT),
            reservation("b", 2, 16_000, 4),
            reservation("c", 20, 8_000, 300),
        ],
        deployments: vec![
            deployment("a", "sk-a"),
            deployment("b", "sk-b"),
            deployment("c", "sk-c"),
        ],
    };
    let app = AppState::new(&config, Arc::new(MemorySink::default())).unwrap();
    Stack {
        gateway: serve(gateway_router(app)).await,
        router,
        engine,
    }
}

/// A bare engine: the control, with no isolation in front.
async fn bare(chunked: bool) -> (String, MockEngine) {
    let engine = MockEngine::new(MockConfig {
        name: "bare".into(),
        ttft: Duration::ZERO,
        tpot: Duration::ZERO,
        default_output_tokens: 1_000,
        contention: Some(engine_model(chunked)),
    });
    (serve(engine.router()).await, engine)
}

fn secs() -> Duration {
    Duration::from_secs(3)
}

fn soak_secs() -> Duration {
    Duration::from_secs(
        std::env::var("PT_SOAK_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60),
    )
}

/// Run A next to `noisy` on the protected stack and on a bare engine.
async fn scenario(
    name: &str,
    noisy: Tenant,
    run: Duration,
    control_chunked: bool,
    allocations: Vec<AllocationConfig>,
) -> (Outcome, Outcome) {
    let _serial = SERIAL.lock().await;

    let s = stack(true, allocations).await;
    let until = Instant::now() + run;
    let noisy_url = if noisy.payg {
        s.router.clone()
    } else {
        s.gateway.clone()
    };
    let noise = load(noisy_url, noisy, until, false);
    tokio::time::sleep(Duration::from_millis(200)).await; // let the noise build up
    let a = load(s.gateway.clone(), A, until, false).await.unwrap();
    noise.await.unwrap();
    let protected = Outcome::of(&a, s.engine.preemptions());

    let (url, engine) = bare(control_chunked).await;
    let until = Instant::now() + run;
    let noise = load(url.clone(), noisy, until, true);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let a = load(url, A, until, true).await.unwrap();
    noise.await.unwrap();
    let control = Outcome::of(&a, engine.preemptions());

    eprintln!(
        "{name}: protected {protected:?}\n{name}: control   {control:?}\n{name}: targets ttft {} ms, tpot {} ms",
        Outcome::ttft_target(),
        Outcome::tpot_target()
    );
    (protected, control)
}

/// B sends bursts of very long prompts (docs/05 §7: 128K tokens; 16K here, scaled to the
/// mock's prefill speed). Protection: B's admission is bounded by its entitlement, and
/// chunked prefill stops a long prompt from stalling everyone's decode.
async fn long_prompts(run: Duration) {
    let b = Tenant {
        tag: "b",
        key: "sk-b",
        prompt: 16_000,
        output: 4,
        clients: 3,
        payg: false,
        rate: None,
        output_jitter: 0,
    };
    let (protected, control) = scenario("long prompts", b, run, false, vec![]).await;
    assert!(protected.meets_slo(), "A's SLO must hold: {protected:?}");
    assert!(
        protected.ok * 100 >= protected.requests * 95,
        "A is served: {protected:?}"
    );
    assert!(
        control.tpot_p95_ms > Outcome::tpot_target(),
        "without isolation, B's prefill must stall A's decode: {control:?}"
    );
}

/// C holds long-lived, KV-heavy sequences. Protection: the router's per-reservation KV
/// budget, so C can't fill the cache and force A to wait or be evicted and recompute.
async fn kv_hog(run: Duration) {
    let c = Tenant {
        tag: "c",
        key: "sk-c",
        prompt: 8_000,
        output: 300,
        clients: 8,
        payg: false,
        rate: None,
        output_jitter: 0,
    };
    let budget = vec![AllocationConfig {
        reservation: "c".into(),
        wu_per_sec: 20_000.0,
        kv_share: Some(0.25),
        dedicated_workers: vec![],
    }];
    let (protected, control) = scenario("KV hog", c, run, true, budget).await;
    assert!(protected.meets_slo(), "A's SLO must hold: {protected:?}");
    assert_eq!(protected.preemptions, 0, "no recompute: {protected:?}");
    assert!(
        protected.ok * 100 >= protected.requests * 95,
        "A is served: {protected:?}"
    );
    assert!(
        control.ttft_p95_ms > Outcome::ttft_target() || control.preemptions > 0,
        "without isolation, C must crowd A out of the KV cache: {control:?}"
    );
}

/// A PAYG flood at 3× the pool's slots. Protection: pull-based dispatch keeps the batch
/// within the pool's slots, strict priority puts A first, and the backfill cap keeps
/// PAYG to half of each floor worker, so A never waits for a PAYG request to finish.
async fn payg_flood(run: Duration) {
    let payg = Tenant {
        tag: "payg",
        key: "",
        prompt: 500,
        output: 100,
        clients: (SLOTS * 3) as usize,
        payg: true,
        rate: None,
        // 100–200 tokens, so completions don't all land in the same step.
        output_jitter: 100,
    };
    let (protected, control) = scenario("PAYG flood", payg, run, true, vec![]).await;
    assert!(protected.meets_slo(), "A's SLO must hold: {protected:?}");
    assert!(
        protected.ok * 100 >= protected.requests * 95,
        "A is served: {protected:?}"
    );
    assert!(
        control.tpot_p95_ms > Outcome::tpot_target(),
        "without isolation, the flood must slow A's decode: {control:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_prompts_cannot_stall_a_reservation() {
    long_prompts(secs()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_hogs_cannot_evict_a_reservation() {
    kv_hog(secs()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payg_floods_cannot_slow_a_reservation() {
    payg_flood(secs()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: PT_SOAK_SECS (default 60) per scenario"]
async fn soak_long_prompts() {
    long_prompts(soak_secs()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: PT_SOAK_SECS (default 60) per scenario"]
async fn soak_kv_hog() {
    kv_hog(soak_secs()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: PT_SOAK_SECS (default 60) per scenario"]
async fn soak_payg_flood() {
    payg_flood(soak_secs()).await;
}
