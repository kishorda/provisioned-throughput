//! `POST /v1/quotes` over HTTP, with the example config.

mod common;

use std::sync::Arc;

use common::*;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::store::MemoryStore;
use pt_control_plane::telemetry::CpTelemetry;
use pt_control_plane::{app, in_memory, Service};
use pt_core::{Outcome, RejectReason, Timings, TokenBreakdown, TrafficClass, UsageRecord};
use pt_telemetry::UsageStore;
use serde_json::{json, Value};

type Svc = Arc<Service<MemoryStore, MemoryPlanner, ManualClock>>;
type Tel = Arc<CpTelemetry<MemoryStore, MemoryPlanner, ManualClock>>;

async fn spawn() -> (String, Svc, Tel) {
    let svc = in_memory(config(), ManualClock::new(t0()));
    let (routes, tel) = app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, routes).await });
    (url, svc, tel)
}

async fn quote(base: &str, key: &str, body: Value) -> (u16, Value) {
    let r = reqwest::Client::new()
        .post(format!("{base}/v1/quotes"))
        .bearer_auth(key)
        .json(&body)
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or(Value::Null))
}

fn shape_body() -> Value {
    json!({
        "model": MAVERICK,
        "tier": "agentic",
        "regions": ["eu-west"],
        "requests_per_minute": 600,
        "shape": {
            "input_p95": 4000, "input_max": 16000, "output_p95": 400,
            "context_ceiling": 32768, "cache_hit_ratio": 0.5, "burst_factor": 2.0
        }
    })
}

fn close(v: &Value, want: f64) -> bool {
    (v.as_f64().unwrap() - want).abs() < 1e-6 * want.max(1.0)
}

#[tokio::test]
async fn shape_quote_sizes_prices_and_translates() {
    let (base, _, _) = spawn().await;
    let (code, v) = quote(&base, ACME_KEY, shape_body()).await;
    assert_eq!(code, 200, "{v}");
    assert_eq!(v["source"], "shape");
    let q = &v["quotes"][0];
    assert_eq!(v["quotes"].as_array().unwrap().len(), 1);

    // Averages default to half the p95s: 2,000 in (1,000 cached), 200 out.
    // WU = 1,000·1 + 1,000·0.08 + 200·3.1 + (2,000 + 100)·200·0.030·0.00021 = 1,702.646
    assert!(close(&q["wu_per_request"], 1_702.646), "{q}");
    // 600 rpm = 10 rps → 17,026.46 WU/s. ÷ (1,000 × 0.8) → 22 CUs. Peak ×2 → 35 CUs.
    assert!(close(&q["sustained_wu_per_s"], 17_026.46));
    assert_eq!(q["recommended_cus"], 22);
    assert_eq!(q["peak_cus"], 35);
    // Peak ÷ recommended entitlement = 34,052.92 ÷ 22,000 = 1.55 → burst 1.6×.
    assert_eq!(
        q["suggested_boundary_policy"]["burst"]["max_rate_multiple"],
        1.6
    );
    // Agentic: 1.5 × 150,000 per CU-month.
    assert_eq!(q["price"]["monthly"], 22 * 225_000);
    // 1 CU = 1,000 WU/s = 60,000 WU/min ÷ 1,702.646.
    assert!(close(
        &q["per_cu"]["requests_per_minute"],
        60_000.0 / 1_702.646
    ));
    assert!(close(
        &q["per_cu"]["input_tokens_per_minute"],
        60_000.0 / 1_702.646 * 2_000.0
    ));
    // Half the p95 prompt is cached, so the cached-prefix Agentic target applies.
    assert_eq!(q["slo"]["ttft_p95_ms"], 500.0);
    assert_eq!(q["slo"]["tpot_p95_ms"], 30.0);
    assert_eq!(q["feasible"], true);
    assert_eq!(q["available_cus"], 200);
    assert_eq!(v["recommendation"]["cus"], 22);
}

#[tokio::test]
async fn defaults_cover_every_region_and_tier() {
    let (base, _, _) = spawn().await;
    let mut body = shape_body();
    body.as_object_mut().unwrap().remove("tier");
    body.as_object_mut().unwrap().remove("regions");
    body["shape"]["context_ceiling"] = json!(65_536); // eu-central serves 32K
    let (code, v) = quote(&base, ACME_KEY, body).await;
    assert_eq!(code, 200, "{v}");
    let quotes = v["quotes"].as_array().unwrap();
    assert_eq!(quotes.len(), 3 * 3, "3 regions × 3 tiers");
    let central: Vec<_> = quotes
        .iter()
        .filter(|q| q["region"] == "eu-central")
        .collect();
    assert!(central
        .iter()
        .all(|q| q["feasible"] == false && q["serves_context"] == false));
    let notes = central[0]["notes"].as_array().unwrap();
    assert!(
        notes.iter().any(|n| n.as_str().unwrap().contains("32768")),
        "{notes:?}"
    );
    // Standard is the cheapest tier; eu-west comes first among equal prices.
    assert_eq!(v["recommendation"]["tier"], "standard");
    assert_eq!(v["recommendation"]["region"], "eu-west");
}

#[tokio::test]
async fn trace_quote_infers_a_shape() {
    let (base, _, _) = spawn().await;
    // Two requests a second for two minutes, with a 5 s burst of 10 per second.
    let mut trace: Vec<Value> = (0..240)
        .map(|i| json!({ "offset_ms": i * 500, "input_tokens": 3000, "cached_tokens": 1500, "output_tokens": 300 }))
        .collect();
    trace.extend((0..50).map(
        |i| json!({ "offset_ms": 60_000 + i * 100, "input_tokens": 3000, "output_tokens": 300 }),
    ));
    let (code, v) = quote(
        &base,
        ACME_KEY,
        json!({ "model": MAVERICK, "tier": "interactive", "regions": ["us-east"], "trace": trace }),
    )
    .await;
    assert_eq!(code, 200, "{v}");
    assert_eq!(v["source"], "trace");
    let s = &v["observed_shape"];
    assert_eq!(s["input_max"], 3000);
    assert_eq!(s["output_p95"], 300);
    assert_eq!(s["context_ceiling"], 4096);
    assert!(s["burst_factor"].as_f64().unwrap() > 1.5, "{s}");
    let q = &v["quotes"][0];
    assert!(q["peak_wu_per_s"].as_f64().unwrap() > q["sustained_wu_per_s"].as_f64().unwrap());
}

fn usage(at_ms: u64, wu: f64, rejected: bool) -> UsageRecord {
    UsageRecord {
        request_id: uuid::Uuid::new_v4(),
        received_at_ms: at_ms,
        tenant: ACME.into(),
        reservation: String::new(),
        deployment: "dep".into(),
        class: (!rejected).then_some(TrafficClass::Provisioned),
        session_id: None,
        tokens: if rejected {
            TokenBreakdown::default()
        } else {
            TokenBreakdown {
                uncached_prefill: 1_000,
                cached_prefill: 1_000,
                decode: 100,
            }
        },
        kv_token_seconds: 0.0,
        wu_estimated: wu,
        wu_actual: wu,
        timings: Timings::default(),
        in_shape: true,
        outcome: if rejected {
            Outcome::Rejected(RejectReason::EntitlementExhausted)
        } else {
            Outcome::Ok
        },
        profile: "p".into(),
    }
}

#[tokio::test]
async fn reservation_history_becomes_a_resize_recommendation() {
    let (base, svc, tel) = spawn().await;
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 2)]))
        .await
        .unwrap()
        .resource;
    // An hour at 10 requests/s of 500 WU: 5,000 WU/s, 40% of which was rejected.
    let now = t0().as_millisecond() as u64 + 3_600_000;
    svc.clock
        .set(jiff::Timestamp::from_millisecond(now as i64).unwrap());
    let records: Vec<_> = (0..36_000u64)
        .map(|i| {
            let mut r = usage(now - 3_600_000 + i * 100, 500.0, i % 5 < 2);
            r.reservation = pt.id.clone();
            r
        })
        .collect();
    tel.store.append("eu-west", records, now).await;

    let (code, v) = quote(&base, ACME_KEY, json!({ "from_reservation": pt.id })).await;
    assert_eq!(code, 200, "{v}");
    assert_eq!(v["source"], "reservation");
    assert_eq!(v["current"]["cus"], 2);
    assert_eq!(v["current"]["requests_seen"], 36_000);
    let q = &v["quotes"][0];
    assert_eq!(q["tier"], "agentic");
    // The span runs from the first to the last request (one 100 ms interval short of the hour).
    let sustained = q["sustained_wu_per_s"].as_f64().unwrap();
    assert!((sustained - 5_000.0).abs() < 1.0, "{q}");
    // 5,000 ÷ 800 → 7 CUs.
    assert_eq!(q["recommended_cus"], 7);
    // Current CUs count towards what's available for this reservation.
    assert_eq!(q["available_cus"], 200);
    assert_eq!(v["recommendation"]["cus"], 7);
    assert!(v["recommendation"]["summary"]
        .as_str()
        .unwrap()
        .starts_with("Increase from 2 to 7"));
}

#[tokio::test]
async fn validation_and_access() {
    let (base, svc, _) = spawn().await;
    let theirs = svc
        .create(GLOBEX, None, request("theirs", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource;
    let mine = svc
        .create(ACME, None, request("mine", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource;

    let cases: Vec<(Value, u16, &str)> = vec![
        (json!({ "model": MAVERICK }), 422, "source"),
        (
            json!({ "model": MAVERICK, "shape": shape_body()["shape"], "trace": [] }),
            422,
            "source",
        ),
        (
            json!({ "model": MAVERICK, "shape": shape_body()["shape"] }),
            422,
            "requests_per_minute",
        ),
        (
            {
                let mut b = shape_body();
                b["regions"] = json!(["mars-1"]);
                b
            },
            422,
            "regions",
        ),
        (
            {
                let mut b = shape_body();
                b["model"] = json!("qwen3-32b");
                b
            },
            422,
            "tier",
        ),
        (json!({ "model": MAVERICK, "trace": [] }), 422, "trace"),
        (
            json!({ "model": MAVERICK, "trace": [{ "offset_ms": 0, "input_tokens": 1, "cached_tokens": 5, "output_tokens": 1 }] }),
            422,
            "trace",
        ),
        (
            json!({ "from_reservation": mine.id, "lookback_days": 0 }),
            422,
            "lookback_days",
        ),
        (
            json!({ "from_reservation": mine.id }),
            422,
            "from_reservation",
        ),
        (json!({ "from_reservation": theirs.id }), 404, ""),
    ];
    for (body, want, field) in cases {
        let (code, v) = quote(&base, ACME_KEY, body.clone()).await;
        assert_eq!(code, want, "{body} → {v}");
        if !field.is_empty() {
            assert_eq!(v["error"]["field"], field, "{body} → {v}");
        }
    }
    assert_eq!(quote(&base, "sk-nope", shape_body()).await.0, 401);
}
