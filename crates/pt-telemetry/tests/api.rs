//! HTTP behaviour of the telemetry API, with a fake directory.

use std::sync::Arc;

use pt_core::{
    Outcome, RejectReason, Shape, Tier, Timings, TokenBreakdown, TrafficClass, UsageRecord,
};
use pt_telemetry::{api, Directory, MemoryUsageStore, ReservationInfo, Telemetry};
use serde_json::{json, Value};

/// 2026-10-15T00:00:00Z
const NOW_MS: u64 = 1_792_022_400_000;

struct FakeDirectory;

impl Directory for FakeDirectory {
    fn tenant_for_key(&self, key: &str) -> Option<String> {
        match key {
            "sk-acme" => Some("acme".into()),
            "sk-globex" => Some("globex".into()),
            _ => None,
        }
    }

    fn region_for_token(&self, token: &str) -> Option<String> {
        (token == "region-eu-west").then(|| "eu-west".into())
    }

    async fn reservation(&self, tenant: &str, id: &str) -> Option<ReservationInfo> {
        (tenant == "acme" && id == "pt-1").then(|| ReservationInfo {
            id: "pt-1".into(),
            tenant: "acme".into(),
            tier: Tier::Agentic,
            cus: 1,
            entitlement_wu_s: 1_000.0,
            monthly_price: 225_000,
            currency: "USD".into(),
            exclusions: vec![],
            shape: Shape {
                input_p95: 2_000,
                input_max: 8_000,
                output_p95: 200,
                context_ceiling: 16_000,
                cache_hit_ratio: 0.5,
                burst_factor: 2.0,
            },
        })
    }

    fn now_ms(&self) -> u64 {
        NOW_MS
    }
}

async fn spawn() -> String {
    let tel = Arc::new(Telemetry {
        store: MemoryUsageStore::default(),
        directory: FakeDirectory,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, api::router(tel)).await });
    url
}

fn record(at_ms: u64, session: Option<&str>) -> UsageRecord {
    UsageRecord {
        request_id: uuid::Uuid::new_v4(),
        received_at_ms: at_ms,
        tenant: "acme".into(),
        reservation: "pt-1".into(),
        deployment: "dep-1".into(),
        class: Some(TrafficClass::Provisioned),
        session_id: session.map(Into::into),
        tokens: TokenBreakdown {
            uncached_prefill: 500,
            cached_prefill: 1_500,
            decode: 50,
        },
        kv_token_seconds: 0.0,
        wu_estimated: 700.0,
        wu_actual: 650.0,
        timings: Timings {
            queue_ms: 0.0,
            ttft_ms: Some(120.0),
            total_ms: 900.0,
            tpot_ms: Some(15.0),
        },
        in_shape: true,
        outcome: Outcome::Ok,
        profile: "p".into(),
    }
}

async fn ingest(base: &str, token: &str, records: &[UsageRecord]) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}/internal/v1/usage"))
        .bearer_auth(token)
        .json(&json!({ "records": records }))
        .send()
        .await
        .unwrap()
}

async fn get(base: &str, key: &str, path: &str) -> (u16, Value) {
    let r = reqwest::Client::new()
        .get(format!("{base}{path}"))
        .bearer_auth(key)
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or(Value::Null))
}

#[tokio::test]
async fn ingest_is_authenticated_and_idempotent() {
    let base = spawn().await;
    let recs = vec![record(NOW_MS - 1_000, None), record(NOW_MS - 2_000, None)];

    assert_eq!(
        ingest(&base, "sk-acme", &recs).await.status(),
        401,
        "tenant keys can't ingest"
    );
    let r = ingest(&base, "region-eu-west", &recs).await;
    assert_eq!(r.status(), 202);
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({ "accepted": 2, "duplicates": 0 })
    );
    // A retry of the same batch is harmless.
    let r = ingest(&base, "region-eu-west", &recs).await;
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({ "accepted": 0, "duplicates": 2 })
    );

    let (_, v) = get(&base, "sk-acme", "/v1/provisioned-throughput/pt-1/usage").await;
    assert_eq!(v["summary"]["requests"]["provisioned"], 2);
}

#[tokio::test]
async fn usage_report() {
    let base = spawn().await;
    let mut recs: Vec<_> = (0..10)
        .map(|i| record(NOW_MS - 3_600_000 + i * 1_000, None))
        .collect();
    let mut rejected = record(NOW_MS - 3_600_000, None);
    rejected.class = None;
    rejected.outcome = Outcome::Rejected(RejectReason::EntitlementExhausted);
    recs.push(rejected);
    ingest(&base, "region-eu-west", &recs).await;

    let (code, v) = get(
        &base,
        "sk-acme",
        "/v1/provisioned-throughput/pt-1/usage?from=2026-10-14T22:00:00Z&to=2026-10-15T00:00:00Z&granularity=1h",
    )
    .await;
    assert_eq!(code, 200);
    assert_eq!(v["granularity"], "1h");
    assert_eq!(v["series"].as_array().unwrap().len(), 2);
    let s = &v["summary"];
    assert_eq!(s["requests"]["provisioned"], 10);
    assert_eq!(s["rejected"]["entitlement_exhausted"], 1);
    assert_eq!(s["cache_hit_rate"], 0.75);
    assert_eq!(s["ttft_ms"]["p95"], 120.0);
    assert_eq!(s["wu_provisioned"], 6_500.0);
    assert_eq!(
        v["series"][1]["requests"]["provisioned"], 10,
        "the last hour"
    );
    let advice = v["advice"].as_array().unwrap();
    assert!(advice
        .iter()
        .any(|a| a.as_str().unwrap().contains("rejected")));

    // Validation.
    let bad = [
        "?granularity=2h",
        "?from=yesterday",
        "?from=2026-10-15T00:00:00Z&to=2026-10-14T00:00:00Z",
        "?from=2026-08-01T00:00:00Z&to=2026-10-15T00:00:00Z",
        "?from=2026-10-01T00:00:00Z&to=2026-10-15T00:00:00Z&granularity=1m",
    ];
    for q in bad {
        let (code, _) = get(
            &base,
            "sk-acme",
            &format!("/v1/provisioned-throughput/pt-1/usage{q}"),
        )
        .await;
        assert_eq!(code, 422, "{q}");
    }
}

#[tokio::test]
async fn sla_report_uses_the_published_rules() {
    let base = spawn().await;
    // 200 fast requests in one window, then 100 slow ones in the next: 50% attainment.
    let start = NOW_MS - 3_600_000;
    let mut recs: Vec<_> = (0..200).map(|i| record(start + i, None)).collect();
    for i in 0..100 {
        let mut r = record(start + 300_000 + i, None);
        r.timings.ttft_ms = Some(5_000.0); // cached-prefix Agentic target is 500 ms
        recs.push(r);
    }
    ingest(&base, "region-eu-west", &recs).await;

    let (code, v) = get(
        &base,
        "sk-acme",
        "/v1/provisioned-throughput/pt-1/sla?month=2026-10",
    )
    .await;
    assert_eq!(code, 200);
    assert_eq!(v["month"], "2026-10");
    assert_eq!(v["complete"], false);
    assert_eq!(v["windows"], 2);
    assert_eq!(v["windows_met"], 1);
    assert_eq!(v["attainment_pct"], 50.0);
    assert_eq!(v["credit_pct"], 50);
    assert_eq!(v["credit_amount"], 112_500);
    assert_eq!(v["target_attainment_pct"], 99.8);
    assert_eq!(v["missed_windows"].as_array().unwrap().len(), 1);

    let (code, v) = get(
        &base,
        "sk-acme",
        "/v1/provisioned-throughput/pt-1/sla?month=2026-09",
    )
    .await;
    assert_eq!(
        (code, v["windows"].clone(), v["complete"].clone()),
        (200, json!(0), json!(true))
    );
    let (code, _) = get(
        &base,
        "sk-acme",
        "/v1/provisioned-throughput/pt-1/sla?month=Sept",
    )
    .await;
    assert_eq!(code, 422);
}

#[tokio::test]
async fn sessions_and_tenant_isolation() {
    let base = spawn().await;
    let recs: Vec<_> = (0..3)
        .map(|i| record(NOW_MS - 10_000 + i, Some("s-42")))
        .collect();
    ingest(&base, "region-eu-west", &recs).await;

    let (code, v) = get(
        &base,
        "sk-acme",
        "/v1/provisioned-throughput/pt-1/sessions/s-42",
    )
    .await;
    assert_eq!(code, 200);
    assert_eq!(v["calls"], 3);
    assert_eq!(v["cache_reuse"], 0.75);
    assert_eq!(v["total_ms"], 2_700.0);
    assert_eq!(v["events"].as_array().unwrap().len(), 3);

    let (code, _) = get(
        &base,
        "sk-acme",
        "/v1/provisioned-throughput/pt-1/sessions/nope",
    )
    .await;
    assert_eq!(code, 404);
    // Another tenant sees nothing, not even that the reservation exists.
    for path in ["usage", "sla", "sessions/s-42"] {
        let (code, v) = get(
            &base,
            "sk-globex",
            &format!("/v1/provisioned-throughput/pt-1/{path}"),
        )
        .await;
        assert_eq!(code, 404, "{path}");
        assert_eq!(v["error"]["code"], "not_found");
    }
    let (code, _) = get(&base, "sk-bad", "/v1/provisioned-throughput/pt-1/usage").await;
    assert_eq!(code, 401);
}
