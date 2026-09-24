//! The router tier against real mock workers.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pt_mock_engine::{MockConfig, MockEngine};
use pt_router::config::{AllocationConfig, RouterConfig, WorkerConfig};
use pt_router::http;
use serde_json::{json, Value};

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

async fn worker(tpot_ms: u64, tokens: u64) -> (String, MockEngine) {
    let e = MockEngine::new(MockConfig {
        name: "w".into(),
        ttft: Duration::from_millis(1),
        tpot: Duration::from_millis(tpot_ms),
        default_output_tokens: tokens,
    });
    (serve(e.router()).await, e)
}

fn config(workers: &[(&str, u32, u32)], allocations: Vec<AllocationConfig>) -> RouterConfig {
    RouterConfig {
        listen: "127.0.0.1:0".into(),
        block_size: 16,
        default_max_tokens: 64,
        queue_timeout_ms: 10_000,
        payg_guard_every: 0,
        failover_hold_ms: 30_000,
        preempt_grace_ms: 250,
        weights: None,
        workers: workers
            .iter()
            .enumerate()
            .map(|(i, (url, slots, kv))| WorkerConfig {
                id: format!("w{i}"),
                url: url.to_string(),
                slots: *slots,
                kv_blocks: *kv,
                hot_spare: false,
            })
            .collect(),
        allocations,
    }
}

async fn spawn_router(c: RouterConfig) -> String {
    c.validate().unwrap();
    serve(http::router(http::Shared::new(&c))).await
}

struct Req<'a> {
    reservation: &'a str,
    class: &'a str,
    wu: f64,
    session: Option<&'a str>,
    system: &'a str,
    max_tokens: u64,
}

impl Default for Req<'_> {
    fn default() -> Self {
        Self {
            reservation: "r",
            class: "provisioned",
            wu: 100.0,
            session: None,
            system: "",
            max_tokens: 20,
        }
    }
}

fn send(router: &str, r: &Req<'_>) -> reqwest::RequestBuilder {
    let mut b = reqwest::Client::new()
        .post(format!("{router}/v1/chat/completions"))
        .header("x-pt-reservation", r.reservation)
        .header("x-pt-class", r.class)
        .header("x-pt-wu-estimate", r.wu.to_string())
        .json(&json!({
            "model": "m",
            "max_tokens": r.max_tokens,
            "messages": [
                { "role": "system", "content": r.system },
                { "role": "user", "content": "hi" },
            ],
        }));
    if let Some(s) = r.session {
        b = b.header("x-pt-session-id", s);
    }
    b
}

async fn status(router: &str) -> Value {
    reqwest::get(format!("{router}/v1/router/status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Start `n` requests after a blocker occupies the only slot; return their completion order.
async fn completion_order(router: &str, reqs: Vec<(String, Req<'static>)>) -> Vec<String> {
    let order = Arc::new(Mutex::new(Vec::new()));
    let blocker = send(
        router,
        &Req {
            max_tokens: 30,
            ..Default::default()
        },
    )
    .send();
    let blocker = tokio::spawn(async move { blocker.await.unwrap().bytes().await.unwrap() });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut tasks = Vec::new();
    for (label, r) in reqs {
        let (router, order) = (router.to_string(), order.clone());
        tasks.push(tokio::spawn(async move {
            let resp = send(&router, &r).send().await.unwrap();
            assert_eq!(resp.status(), 200);
            resp.bytes().await.unwrap();
            order.lock().unwrap().push(label);
        }));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    blocker.await.unwrap();
    for t in tasks {
        t.await.unwrap();
    }
    let out = order.lock().unwrap().clone();
    out
}

#[tokio::test]
async fn provisioned_goes_first_and_slots_are_never_exceeded() {
    let (w, engine) = worker(3, 20).await;
    let router = spawn_router(config(&[(w.as_str(), 1, 1_000)], vec![])).await;
    let mut reqs = Vec::new();
    for i in 0..3 {
        reqs.push((
            format!("payg{i}"),
            Req {
                class: "payg",
                reservation: "p",
                ..Default::default()
            },
        ));
    }
    for i in 0..3 {
        reqs.push((
            format!("prov{i}"),
            Req {
                reservation: "a",
                ..Default::default()
            },
        ));
    }
    let order = completion_order(&router, reqs).await;
    let first3: Vec<_> = order.iter().take(3).map(|s| &s[..4]).collect();
    assert_eq!(first3, ["prov", "prov", "prov"], "{order:?}");
    assert_eq!(
        engine.max_in_flight(),
        1,
        "pull-based: never more than the worker's slots"
    );
    let s = status(&router).await;
    assert_eq!(s["dispatched"]["provisioned"], 4);
    assert_eq!(s["dispatched"]["payg"], 3);
    assert_eq!(s["workers"][0]["slots_used"], 0);
}

#[tokio::test]
async fn wfq_shares_work_between_reservations() {
    let (w, _) = worker(2, 10).await;
    let router = spawn_router(config(&[(w.as_str(), 1, 1_000)], vec![])).await;
    // Equal weights. "heavy" sends 1,000 WU requests, "light" sends 100 WU requests.
    let mut reqs = Vec::new();
    for i in 0..6 {
        reqs.push((
            format!("heavy{i}"),
            Req {
                reservation: "heavy",
                wu: 1_000.0,
                ..Default::default()
            },
        ));
        reqs.push((
            format!("light{i}"),
            Req {
                reservation: "light",
                wu: 100.0,
                ..Default::default()
            },
        ));
    }
    let order = completion_order(&router, reqs).await;
    let light_first = order
        .iter()
        .take(7)
        .filter(|s| s.starts_with("light"))
        .count();
    assert!(
        light_first >= 5,
        "light's small requests get its share of work first: {order:?}"
    );
}

#[tokio::test]
async fn session_and_prefix_affinity() {
    let (w0, _) = worker(1, 2).await;
    let (w1, _) = worker(1, 2).await;
    let (w2, _) = worker(1, 2).await;
    let router = spawn_router(config(
        &[(&w0, 4, 1_000), (&w1, 4, 1_000), (&w2, 4, 1_000)],
        vec![],
    ))
    .await;
    let worker_for = |r: Req<'static>| {
        let router = router.clone();
        async move {
            let resp = send(&router, &r).send().await.unwrap();
            let w = resp.headers()["x-pt-router-worker"]
                .to_str()
                .unwrap()
                .to_string();
            resp.bytes().await.unwrap();
            w
        }
    };
    let first = worker_for(Req {
        session: Some("agent-1"),
        system: "plan trips",
        ..Default::default()
    })
    .await;
    for _ in 0..5 {
        let again = worker_for(Req {
            session: Some("agent-1"),
            system: "plan trips",
            ..Default::default()
        })
        .await;
        assert_eq!(again, first, "same session and prefix stay on one worker");
    }
    // A long shared system prompt attracts other sessions to the worker that has it.
    let long = "You are a very careful travel planner. ".repeat(50);
    let long: &'static str = Box::leak(long.into_boxed_str());
    let a = worker_for(Req {
        system: long,
        ..Default::default()
    })
    .await;
    let b = worker_for(Req {
        system: long,
        session: Some("other"),
        ..Default::default()
    })
    .await;
    assert_eq!(a, b);
}

#[tokio::test]
async fn kv_budget_limits_one_reservation_only() {
    let (w, _) = worker(5, 40).await;
    // Budget for "capped": 0.1 × 100 blocks × 1.2 = 12 blocks. Each request is about
    // (8 prompt + 64 output) ÷ 16 = 5 blocks, so two run at once and the rest wait.
    let allocations = vec![AllocationConfig {
        reservation: "capped".into(),
        wu_per_sec: 1.0,
        kv_share: Some(0.1),
        dedicated_workers: vec![],
    }];
    let router = spawn_router(config(&[(w.as_str(), 8, 100)], allocations)).await;
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let router = router.clone();
        tasks.push(tokio::spawn(async move {
            send(
                &router,
                &Req {
                    reservation: "capped",
                    max_tokens: 64,
                    ..Default::default()
                },
            )
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
        }));
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
    let s = status(&router).await;
    assert!(
        s["workers"][0]["kv_by_reservation"]["capped"]
            .as_u64()
            .unwrap()
            <= 12,
        "{s}"
    );
    assert_eq!(s["queued"]["provisioned"], 2, "{s}");
    // Another reservation still gets in straight away.
    let t = Instant::now();
    let resp = send(
        &router,
        &Req {
            reservation: "free",
            max_tokens: 2,
            ..Default::default()
        },
    )
    .send()
    .await
    .unwrap();
    let queued: u64 = resp.headers()["x-pt-router-queue-ms"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(queued < 50, "queued {queued} ms");
    resp.bytes().await.unwrap();
    assert!(t.elapsed() < Duration::from_millis(500));
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(status(&router).await["workers"][0]["kv_used"], 0);
}

#[tokio::test]
async fn timeouts_disconnects_and_oversized_requests_leak_nothing() {
    let (w, _) = worker(10, 50).await;
    let mut c = config(&[(w.as_str(), 1, 50)], vec![]);
    c.queue_timeout_ms = 100;
    let router = spawn_router(c).await;

    // Too large for any worker: rejected up front.
    let r = send(
        &router,
        &Req {
            max_tokens: 5_000,
            ..Default::default()
        },
    )
    .send()
    .await
    .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(r.headers()["x-pt-reason"], "router_request_too_large");

    // A long request holds the slot; the next one times out in the queue.
    let blocker = send(
        &router,
        &Req {
            max_tokens: 50,
            ..Default::default()
        },
    )
    .send();
    let blocker = tokio::spawn(async move { blocker.await.unwrap().bytes().await.unwrap() });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = send(&router, &Req::default()).send().await.unwrap();
    assert_eq!(r.status(), 503);
    assert_eq!(r.headers()["x-pt-reason"], "router_queue_timeout");
    blocker.await.unwrap();

    // A client that disconnects mid-response frees the slot.
    let mut body = json!({
        "model": "m", "stream": true, "max_tokens": 50,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    body["stream"] = json!(true);
    let resp = reqwest::Client::new()
        .post(format!("{router}/v1/chat/completions"))
        .header("x-pt-class", "provisioned")
        .json(&body)
        .send()
        .await
        .unwrap();
    let mut stream = resp.bytes_stream();
    use futures::StreamExt;
    stream.next().await.unwrap().unwrap();
    drop(stream);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let s = status(&router).await;
        if s["workers"][0]["slots_used"] == 0 && s["workers"][0]["kv_used"] == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "capacity leaked: {s}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(status(&router).await["queued"]["provisioned"], 0);
}

/// A streaming PAYG request that runs until done or preempted. Returns its SSE body.
async fn stream_payg(router: String, tokens: u64) -> String {
    reqwest::Client::new()
        .post(format!("{router}/v1/chat/completions"))
        .header("x-pt-reservation", "payg")
        .header("x-pt-class", "payg")
        .json(&json!({
            "model": "m", "stream": true, "max_tokens": tokens,
            "messages": [{ "role": "user", "content": "hi" }],
        }))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap_or_default()
}

#[tokio::test]
async fn failover_preempts_payg_on_hot_spares() {
    let (floor, _) = worker(10, 200).await;
    let (spare, _) = worker(10, 200).await;
    let mut c = config(&[(&floor, 1, 1_000), (&spare, 1, 1_000)], vec![]);
    c.workers[1].hot_spare = true;
    let router = spawn_router(c).await;

    // Two long PAYG streams fill the spare, then the floor.
    let p1 = tokio::spawn(stream_payg(router.clone(), 200));
    tokio::time::sleep(Duration::from_millis(30)).await;
    let p2 = tokio::spawn(stream_payg(router.clone(), 200));
    tokio::time::sleep(Duration::from_millis(30)).await;
    let s = status(&router).await;
    assert_eq!(s["workers"][1]["slots_used"], 1, "{s}");
    assert_eq!(s["workers"][0]["slots_used"], 1);

    // Without a failover, provisioned work just waits.
    let t = Instant::now();
    let r = send(
        &router,
        &Req {
            max_tokens: 2,
            ..Default::default()
        },
    )
    .timeout(Duration::from_millis(600))
    .send()
    .await;
    assert!(r.is_err(), "no preemption outside a failover");
    assert_eq!(status(&router).await["preempted"], 0);
    assert!(t.elapsed() >= Duration::from_millis(600));

    // A failover request preempts the spare's PAYG after the grace period.
    let t = Instant::now();
    let resp = send(
        &router,
        &Req {
            max_tokens: 2,
            ..Default::default()
        },
    )
    .header("x-pt-failover", "active")
    .send()
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()["x-pt-router-worker"],
        "w1",
        "served on the spare"
    );
    resp.bytes().await.unwrap();
    let waited = t.elapsed();
    assert!(
        waited >= Duration::from_millis(250) && waited < Duration::from_millis(900),
        "{waited:?}"
    );
    let first = p1.await.unwrap();
    assert!(
        first.contains("\"code\":\"preempted\""),
        "SSE error event: {first}"
    );
    let s = status(&router).await;
    assert_eq!(s["preempted"], 1);
    assert_eq!(s["failover_active"], true);

    // The fence keeps new PAYG off the free spare while the failover lasts.
    let r = reqwest::Client::new()
        .post(format!("{router}/v1/chat/completions"))
        .header("x-pt-class", "payg")
        .json(&json!({ "model": "m", "max_tokens": 1, "messages": [{ "role": "user", "content": "hi" }] }))
        .timeout(Duration::from_millis(300))
        .send()
        .await;
    assert!(r.is_err(), "PAYG waits for the floor, not the spare");
    let s = status(&router).await;
    assert_eq!(s["workers"][1]["slots_used"], 0, "{s}");

    // The floor's PAYG finishes normally, and nothing leaks.
    let second = p2.await.unwrap();
    assert!(second.contains("[DONE]") && !second.contains("preempted"));
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let s = status(&router).await;
        if s["workers"][0]["slots_used"] == 0 && s["workers"][1]["slots_used"] == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "capacity leaked: {s}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn preempted_before_the_response_starts_gets_503() {
    // A non-streaming PAYG request is aborted while the worker is still generating.
    let (w, _) = worker(10, 300).await;
    let mut c = config(&[(&w, 1, 1_000)], vec![]);
    c.workers[0].hot_spare = true;
    let router = spawn_router(c).await;
    let payg = {
        let router = router.clone();
        tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("{router}/v1/chat/completions"))
                .header("x-pt-class", "payg")
                .json(&json!({ "model": "m", "max_tokens": 300, "messages": [{ "role": "user", "content": "hi" }] }))
                .send()
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let prov = send(
        &router,
        &Req {
            max_tokens: 2,
            ..Default::default()
        },
    )
    .header("x-pt-failover", "1")
    .send()
    .await
    .unwrap();
    assert_eq!(prov.status(), 200);
    let payg = payg.await.unwrap();
    assert_eq!(payg.status(), 503);
    assert_eq!(payg.headers()["x-pt-reason"], "preempted");
    assert_eq!(payg.headers()["retry-after"], "1");
}
