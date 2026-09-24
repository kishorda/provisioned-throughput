//! Region incidents and customer-initiated changes are excluded from the SLA.

mod common;

use std::sync::Arc;

use common::*;
use jiff::{SignedDuration, Timestamp};
use pt_control_plane::clock::ManualClock;
use pt_control_plane::model::{Sku, UpdateRequest};
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::store::MemoryStore;
use pt_control_plane::telemetry::CpTelemetry;
use pt_control_plane::{app, in_memory, Service};
use pt_core::{Outcome, Timings, TokenBreakdown, TrafficClass, UsageRecord};
use pt_telemetry::UsageStore;
use serde_json::{json, Value};

const OPERATOR: &str = "sk-operator-dev";

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

fn hours(h: i64) -> Timestamp {
    t0() + SignedDuration::from_hours(h)
}

fn ms(t: Timestamp) -> u64 {
    t.as_millisecond() as u64
}

/// 100 in-shape provisioned requests over 100 s from `start`, fast or slow.
/// Agentic with no cache hits: TTFT target 1,500 ms.
fn requests(reservation: &str, start: Timestamp, slow: bool) -> Vec<UsageRecord> {
    (0..100u64)
        .map(|i| UsageRecord {
            request_id: uuid::Uuid::new_v4(),
            received_at_ms: ms(start) + i * 1_000,
            tenant: ACME.into(),
            reservation: reservation.into(),
            deployment: "dep".into(),
            class: Some(TrafficClass::Provisioned),
            session_id: None,
            tokens: TokenBreakdown {
                uncached_prefill: 1_000,
                cached_prefill: 0,
                decode: 100,
            },
            kv_token_seconds: 0.0,
            wu_estimated: 100.0,
            wu_actual: 100.0,
            timings: Timings {
                queue_ms: 0.0,
                ttft_ms: Some(if slow { 9_000.0 } else { 100.0 }),
                total_ms: 1_000.0,
                tpot_ms: Some(10.0),
            },
            in_shape: true,
            outcome: Outcome::Ok,
            profile: "p".into(),
        })
        .collect()
}

async fn post(url: String, key: &str, body: Value) -> (u16, Value) {
    let r = reqwest::Client::new()
        .post(url)
        .bearer_auth(key)
        .json(&body)
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or(Value::Null))
}

async fn sla(base: &str, id: &str) -> Value {
    reqwest::Client::new()
        .get(format!(
            "{base}/v1/provisioned-throughput/{id}/sla?month=2026-10"
        ))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn incident_api() {
    let (base, svc, _) = spawn().await;
    let url = format!("{base}/internal/v1/incidents");
    let body = json!({ "region": "eu-west", "description": "Network partition in eu-west-1a" });

    assert_eq!(
        post(url.clone(), ACME_KEY, body.clone()).await.0,
        401,
        "tenants can't declare"
    );
    let (code, inc) = post(url.clone(), OPERATOR, body.clone()).await;
    assert_eq!(code, 201, "{inc}");
    assert_eq!(inc["region"], "eu-west");
    assert!(inc.get("ended_at").is_none());
    let (code, v) = post(url.clone(), OPERATOR, body.clone()).await;
    assert_eq!(
        (code, v["error"]["code"].as_str()),
        (409, Some("incident_open"))
    );

    let bad = [
        json!({ "region": "mars-1", "description": "x" }),
        json!({ "region": "eu-west", "description": "  " }),
        json!({ "region": "us-east", "description": "x", "started_at": "2026-09-29T00:00:00Z" }),
    ];
    for b in bad {
        assert_eq!(post(url.clone(), OPERATOR, b.clone()).await.0, 422, "{b}");
    }

    svc.clock.advance(SignedDuration::from_mins(30));
    let resolve = format!("{url}/{}/resolve", inc["id"].as_str().unwrap());
    let (code, done) = post(resolve.clone(), OPERATOR, json!({})).await;
    assert_eq!(code, 200);
    assert_eq!(done["ended_at"], "2026-10-01T00:30:00Z");
    assert_eq!(post(resolve, OPERATOR, json!({})).await.0, 409);

    let list: Value = reqwest::Client::new()
        .get(&url)
        .bearer_auth(OPERATOR)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["data"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn multi_region_failover_and_resize_are_excluded() {
    let (base, svc, tel) = spawn().await;
    let mut req = request("agents", &[("eu-west", 4), ("eu-central", 4)]);
    req.sku = Sku::MultiRegion;
    let id = svc.create(ACME, None, req).await.unwrap().resource.id;

    // Hour 1: fast. Hour 2: slow, during an eu-west failover. Hour 4: slow, just after a resize.
    let mut recs = requests(&id, hours(1), false);
    recs.extend(requests(&id, hours(2), true));
    recs.extend(requests(&id, hours(4), true));
    tel.store
        .append("eu-central", recs, ms(hours(5)))
        .await
        .unwrap();

    let before = sla(&base, &id).await;
    assert_eq!(before["windows_met"], 1, "{before}");
    assert!(before["attainment_pct"].as_f64().unwrap() < 99.5);

    svc.clock.set(hours(3));
    let (code, inc) = post(
        format!("{base}/internal/v1/incidents"),
        OPERATOR,
        json!({ "region": "eu-west", "description": "Region down", "started_at": hours(2) }),
    )
    .await;
    assert_eq!(code, 201, "{inc}");
    post(
        format!(
            "{base}/internal/v1/incidents/{}/resolve",
            inc["id"].as_str().unwrap()
        ),
        OPERATOR,
        json!({ "ended_at": hours(2) + SignedDuration::from_mins(40) }),
    )
    .await;

    svc.clock.set(hours(4));
    let increase = UpdateRequest {
        regions: Some(shares(&[("eu-west", 6), ("eu-central", 4)])),
        ..Default::default()
    };
    svc.update(ACME, &id, None, increase).await.unwrap();
    svc.clock.set(hours(5));

    let after = sla(&base, &id).await;
    assert_eq!(after["windows"], 1, "{after}");
    assert_eq!(after["attainment_pct"], 100.0);
    assert_eq!(after["credit_pct"], 0);
    assert_eq!(after["excluded"]["excluded_periods"]["failover"], 100);
    assert_eq!(after["excluded"]["excluded_periods"]["resize"], 100);
    let reasons: Vec<&str> = after["exclusion_windows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["reason"].as_str().unwrap())
        .collect();
    assert_eq!(reasons, ["activation", "failover", "resize"]);
    // The failover exclusion covers 5 minutes, not the whole 40-minute incident.
    assert_eq!(after["exclusion_windows"][1]["end"], "2026-10-01T02:05:00Z");
}

#[tokio::test]
async fn regional_outage_excludes_only_the_failed_region() {
    let (base, svc, tel) = spawn().await;
    let id = svc
        .create(
            ACME,
            None,
            request("agents", &[("eu-west", 4), ("us-east", 4)]),
        )
        .await
        .unwrap()
        .resource
        .id;
    // Slow in both regions during an eu-west outage.
    tel.store
        .append("eu-west", requests(&id, hours(2), true), ms(hours(3)))
        .await
        .unwrap();
    tel.store
        .append(
            "us-east",
            requests(&id, hours(2) + SignedDuration::from_mins(10), true),
            ms(hours(3)),
        )
        .await
        .unwrap();

    svc.clock.set(hours(3));
    post(
        format!("{base}/internal/v1/incidents"),
        OPERATOR,
        json!({ "region": "eu-west", "description": "Region down", "started_at": hours(2) }),
    )
    .await;

    let r = sla(&base, &id).await;
    // eu-west requests are excluded for the whole (still open) incident; us-east ones count.
    assert_eq!(
        r["excluded"]["excluded_periods"]["region_outage"], 100,
        "{r}"
    );
    assert_eq!(r["eligible_requests"], 100);
    assert_eq!(r["windows_met"], 0);
    assert_eq!(r["exclusion_windows"][1]["region"], "eu-west");
}
