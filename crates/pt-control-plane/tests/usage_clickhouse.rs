//! Usage in ClickHouse, end to end: ingest over HTTP, a restart, and invoices built from the
//! stored usage. Set `CLICKHOUSE_TEST_URL` to run; skipped otherwise.

mod common;

use common::*;
use jiff::Timestamp;
use pt_control_plane::billing;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::store::Store;
use pt_control_plane::{app_with_usage, in_memory};
use pt_core::{Outcome, Timings, TokenBreakdown, TrafficClass, UsageRecord};
use pt_telemetry::clickhouse::{ClickHouseConfig, ClickHouseUsageStore};
use pt_telemetry::UsageBackend;
use serde_json::{json, Value};

fn at(s: &str) -> Timestamp {
    s.parse().unwrap()
}

fn spillover(id: &str, at_ms: u64) -> UsageRecord {
    UsageRecord {
        request_id: uuid::Uuid::new_v4(),
        received_at_ms: at_ms,
        tenant: ACME.into(),
        reservation: id.into(),
        deployment: "dep".into(),
        class: Some(TrafficClass::Spillover),
        session_id: None,
        tokens: TokenBreakdown {
            uncached_prefill: 1_000_000,
            cached_prefill: 0,
            decode: 1_000_000,
        },
        kv_token_seconds: 0.0,
        wu_estimated: 1.0,
        wu_actual: 1.0,
        timings: Timings {
            queue_ms: 0.0,
            ttft_ms: Some(100.0),
            total_ms: 1_000.0,
            tpot_ms: Some(10.0),
        },
        in_shape: true,
        outcome: Outcome::Ok,
        profile: "p".into(),
    }
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

async fn ingest(base: &str, records: &[UsageRecord]) -> (u16, Value) {
    let r = reqwest::Client::new()
        .post(format!("{base}/internal/v1/usage"))
        .bearer_auth("region-token-eu-west-dev")
        .json(&json!({ "records": records }))
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or(Value::Null))
}

#[tokio::test]
async fn usage_survives_a_restart_and_is_invoiced() {
    let Ok(url) = std::env::var("CLICKHOUSE_TEST_URL") else {
        eprintln!("CLICKHOUSE_TEST_URL not set; skipping");
        return;
    };
    let ch = ClickHouseConfig {
        url,
        database: format!("pt_test_cp_{}", uuid::Uuid::new_v4().simple()),
        user: "default".into(),
        password: None,
    };
    let clickhouse = || {
        let s = ClickHouseUsageStore::new(ch.clone(), 35).unwrap();
        async move {
            s.migrate().await.unwrap();
            UsageBackend::ClickHouse(s)
        }
    };

    let clock = ManualClock::new(at("2026-10-01T00:00:00Z"));
    let svc = in_memory(config(), clock.clone());
    let id = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource
        .id;

    // Gateways push a batch, then retry it.
    let (routes, _) = app_with_usage(svc.clone(), clickhouse().await);
    let base = serve(routes).await;
    let batch: Vec<_> = (0..3)
        .map(|i| spillover(&id, at("2026-10-15T00:00:00Z").as_millisecond() as u64 + i))
        .collect();
    let (code, r) = ingest(&base, &batch).await;
    assert_eq!(code, 202, "{r}");
    assert_eq!(r["accepted"], 3);
    let (_, r) = ingest(&base, &batch).await;
    assert_eq!(
        (r["accepted"].as_u64(), r["duplicates"].as_u64()),
        (Some(0), Some(3))
    );

    // A restarted control plane: a new store instance over the same database.
    clock.set(at("2026-11-01T12:00:00Z"));
    let (routes, tel) = app_with_usage(svc.clone(), clickhouse().await);
    let base = serve(routes).await;
    let invoice: Value = reqwest::Client::new()
        .get(format!("{base}/v1/invoices/2026-10"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let spill: Vec<i64> = invoice["lines"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|l| l["kind"] == "spillover")
        .map(|l| l["amount"].as_i64().unwrap())
        .collect();
    // 3M input × 27 + 3M output × 85 cents: counted once despite the retry.
    assert_eq!(spill, [81, 255], "{invoice}");

    clock.set(at("2026-11-03T01:00:00Z"));
    let stored = billing::finalize_due(&svc, &tel).await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].total, invoice["total"].as_i64().unwrap());
}

#[tokio::test]
async fn an_unreachable_usage_store_fails_loudly() {
    let clock = ManualClock::new(at("2026-10-01T00:00:00Z"));
    let svc = in_memory(config(), clock.clone());
    let id = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource
        .id;
    let down = ClickHouseUsageStore::new(
        ClickHouseConfig {
            url: "http://127.0.0.1:1".into(),
            database: "pt".into(),
            user: "default".into(),
            password: None,
        },
        35,
    )
    .unwrap();
    let (routes, tel) = app_with_usage(svc.clone(), UsageBackend::ClickHouse(down));
    let base = serve(routes).await;

    // Gateways get 503 and keep the batch for a retry.
    let (code, body) = ingest(&base, &[spillover(&id, 1_790_000_000_000)]).await;
    assert_eq!(code, 503);
    assert_eq!(body["error"]["code"], "store_unavailable");
    // Reports and invoices fail rather than showing no usage.
    let r = reqwest::Client::new()
        .get(format!("{base}/v1/invoices/2026-10"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    // Finalisation stores nothing; it will retry.
    clock.set(at("2026-11-03T01:00:00Z"));
    assert!(billing::finalize_due(&svc, &tel).await.is_err());
    assert!(svc.store.list_invoices(ACME).await.unwrap().is_empty());
}
