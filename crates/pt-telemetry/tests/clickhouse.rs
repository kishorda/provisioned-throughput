//! The ClickHouse usage store against a real server. Set `CLICKHOUSE_TEST_URL` (for example
//! `http://127.0.0.1:18123`) to run these. Without it they're skipped. Each test uses its own
//! database.

use pt_core::{Outcome, RejectReason, Timings, TokenBreakdown, TrafficClass, UsageRecord};
use pt_telemetry::clickhouse::{ClickHouseConfig, ClickHouseUsageStore};
use pt_telemetry::{UsageError, UsageStore};

fn config(name: &str) -> Option<ClickHouseConfig> {
    let Ok(url) = std::env::var("CLICKHOUSE_TEST_URL") else {
        eprintln!("CLICKHOUSE_TEST_URL not set; skipping {name}");
        return None;
    };
    Some(ClickHouseConfig {
        url,
        database: format!("pt_test_{name}_{}", uuid::Uuid::new_v4().simple()),
        user: "default".into(),
        password: None,
        ..Default::default()
    })
}

async fn store(name: &str) -> Option<ClickHouseUsageStore> {
    let s = ClickHouseUsageStore::new(config(name)?, 35).unwrap();
    s.migrate().await.unwrap();
    s.migrate().await.unwrap();
    Some(s)
}

fn record(tenant: &str, reservation: &str, at_ms: u64, class: TrafficClass) -> UsageRecord {
    UsageRecord {
        request_id: uuid::Uuid::new_v4(),
        received_at_ms: at_ms,
        tenant: tenant.into(),
        reservation: reservation.into(),
        deployment: "dep-1".into(),
        class: Some(class),
        session_id: Some("s-1".into()),
        tokens: TokenBreakdown {
            uncached_prefill: 1_000,
            cached_prefill: 250,
            decode: 120,
        },
        kv_token_seconds: 12.5,
        wu_estimated: 480.25,
        wu_actual: 471.125,
        timings: Timings {
            queue_ms: 1.5,
            ttft_ms: Some(210.75),
            total_ms: 1_900.0,
            tpot_ms: Some(14.25),
        },
        in_shape: true,
        outcome: Outcome::Ok,
        profile: "p".into(),
    }
}

#[tokio::test]
async fn records_round_trip_exactly_once() {
    let Some(s) = store("roundtrip").await else {
        return;
    };
    let base = 1_790_000_000_000u64;
    let a = record("acme", "pt-1", base + 1_000, TrafficClass::Provisioned);
    let mut b = record("acme", "pt-1", base, TrafficClass::Spillover);
    b.outcome = Outcome::Rejected(RejectReason::EntitlementExhausted);
    b.session_id = None;
    b.timings.ttft_ms = None;
    let other_res = record("acme", "pt-2", base, TrafficClass::Provisioned);
    let other_tenant = record("globex", "pt-1", base, TrafficClass::Provisioned);
    let late = record("acme", "pt-1", base + 10_000, TrafficClass::Provisioned);
    let mut undated = record("acme", "pt-1", 0, TrafficClass::Burst);
    undated.received_at_ms = 0;

    // A batch with a duplicate inside it.
    let r = s
        .append(
            "eu-west",
            vec![a.clone(), b.clone(), a.clone(), other_res, other_tenant],
            base,
        )
        .await
        .unwrap();
    assert_eq!((r.accepted, r.duplicates), (4, 1));
    // A retried batch: all duplicates.
    let r = s
        .append("eu-west", vec![a.clone(), b.clone()], base)
        .await
        .unwrap();
    assert_eq!((r.accepted, r.duplicates), (0, 2));
    // Records without a receive time are stamped with the ingest time.
    s.append(
        "eu-central",
        vec![late.clone(), undated.clone()],
        base + 5_000,
    )
    .await
    .unwrap();

    let got = s.range("acme", "pt-1", base, base + 10_000).await.unwrap();
    let ids: Vec<_> = got.iter().map(|r| r.record.request_id).collect();
    assert_eq!(
        ids,
        [b.request_id, a.request_id, undated.request_id],
        "ordered by time"
    );
    assert_eq!(
        got[0].record, b,
        "exact round trip, including enums and options"
    );
    assert_eq!(got[1].record, a);
    assert_eq!(got[1].region, "eu-west");
    assert_eq!(got[1].at_ms, base + 1_000);
    assert_eq!(got[2].at_ms, base + 5_000);
    assert_eq!(got[2].region, "eu-central");
    // The end is exclusive, and other tenants and reservations are separate.
    assert_eq!(
        s.range("acme", "pt-1", base, base + 10_001)
            .await
            .unwrap()
            .len(),
        4
    );
    assert!(s
        .range("globex", "pt-2", 0, u64::MAX / 2)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        s.range("globex", "pt-1", 0, base * 2).await.unwrap().len(),
        1
    );
    assert_eq!(
        s.prune(base * 2).await.unwrap(),
        0,
        "retention is the table TTL"
    );
}

#[tokio::test]
async fn concurrent_duplicates_are_read_once() {
    let Some(s) = store("race").await else {
        return;
    };
    let base = 1_790_000_000_000u64;
    let r = record("acme", "pt-1", base, TrafficClass::Provisioned);
    // Both may pass the existence check and insert: reads still see one.
    let (x, y) = tokio::join!(
        s.append("eu-west", vec![r.clone()], base),
        s.append("eu-west", vec![r.clone()], base),
    );
    let (x, y) = (x.unwrap(), y.unwrap());
    assert_eq!(x.accepted + x.duplicates + y.accepted + y.duplicates, 2);
    let got = s.range("acme", "pt-1", 0, base * 2).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].record, r);
}

#[tokio::test]
async fn retention_follows_configuration() {
    let Some(cfg) = config("ttl") else {
        return;
    };
    let url = cfg.url.clone();
    let db = cfg.database.clone();
    ClickHouseUsageStore::new(cfg.clone(), 35)
        .unwrap()
        .migrate()
        .await
        .unwrap();
    ClickHouseUsageStore::new(cfg, 90)
        .unwrap()
        .migrate()
        .await
        .unwrap();
    let create = reqwest::Client::new()
        .post(format!("{url}/"))
        .body(format!("SHOW CREATE TABLE {db}.usage_records"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(create.contains("toIntervalDay(90)"), "{create}");
}

#[tokio::test]
async fn an_unreachable_server_is_an_error_not_empty_usage() {
    let s = ClickHouseUsageStore::new(
        ClickHouseConfig {
            url: "http://127.0.0.1:1".into(),
            database: "pt".into(),
            user: "default".into(),
            password: None,
            ..Default::default()
        },
        35,
    )
    .unwrap();
    assert!(matches!(
        s.range("acme", "pt-1", 0, 1).await,
        Err(UsageError(_))
    ));
    let r = record("acme", "pt-1", 1, TrafficClass::Provisioned);
    assert!(s.append("eu-west", vec![r], 1).await.is_err());
    assert!(s.migrate().await.is_err());
}

/// HTTPS to ClickHouse with a private CA (ADR-021). Set `CLICKHOUSE_TEST_TLS_URL` (for
/// example `https://127.0.0.1:18443`) and `PT_TEST_TLS_DIR` (with `ca.pem` and `rogue.pem`).
#[tokio::test]
async fn https_with_a_private_ca() {
    let (Ok(url), Ok(dir)) = (
        std::env::var("CLICKHOUSE_TEST_TLS_URL"),
        std::env::var("PT_TEST_TLS_DIR"),
    ) else {
        eprintln!("CLICKHOUSE_TEST_TLS_URL or PT_TEST_TLS_DIR not set; skipping");
        return;
    };
    let config = |ca: Option<&str>| ClickHouseConfig {
        url: url.clone(),
        database: format!("pt_test_tls_{}", uuid::Uuid::new_v4().simple()),
        ca_cert: ca.map(|f| format!("{dir}/{f}")),
        ..Default::default()
    };

    let s = ClickHouseUsageStore::new(config(Some("ca.pem")), 35).unwrap();
    s.migrate().await.unwrap();
    let r = record("acme", "pt-1", 1_790_000_000_000, TrafficClass::Provisioned);
    s.append("eu-west", vec![r.clone()], 0).await.unwrap();
    let got = s.range("acme", "pt-1", 0, u64::MAX / 2).await.unwrap();
    assert_eq!(got[0].record, r);

    // Without the private CA, or with the wrong one, the server isn't trusted.
    for ca in [None, Some("rogue.pem")] {
        let s = ClickHouseUsageStore::new(config(ca), 35).unwrap();
        let err = s.migrate().await.unwrap_err();
        assert!(err.0.contains("clickhouse"), "{err}");
    }
}
