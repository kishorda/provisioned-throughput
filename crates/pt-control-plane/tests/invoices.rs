//! Monthly invoices: fees from the rate timeline, spillover at PAYG prices, SLA credits,
//! drafts, and finalisation.

mod common;

use std::sync::Arc;

use common::*;
use jiff::{SignedDuration, Timestamp};
use pt_control_plane::billing::{self, InvoiceStatus};
use pt_control_plane::clock::ManualClock;
use pt_control_plane::model::UpdateRequest;
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::store::MemoryStore;
use pt_control_plane::telemetry::CpTelemetry;
use pt_control_plane::{app, in_memory, Service};
use pt_core::{Outcome, Timings, TokenBreakdown, TrafficClass, UsageRecord};
use pt_telemetry::UsageStore;
use serde_json::Value;

type Svc = Arc<Service<MemoryStore, MemoryPlanner, ManualClock>>;
type Tel = Arc<CpTelemetry<MemoryStore, MemoryPlanner, ManualClock>>;

fn at(s: &str) -> Timestamp {
    s.parse().unwrap()
}

fn ms(t: Timestamp) -> u64 {
    t.as_millisecond() as u64
}

async fn spawn() -> (String, Svc, Tel) {
    let svc = in_memory(config(), ManualClock::new(at("2026-10-01T00:00:00Z")));
    let (routes, tel) = app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, routes).await });
    (url, svc, tel)
}

/// `n` requests of `class`, one a second from `start`. Provisioned ones are fast or slow
/// (agentic TTFT target 1,500 ms).
fn records(
    id: &str,
    start: Timestamp,
    n: u64,
    class: TrafficClass,
    slow: bool,
    tokens: TokenBreakdown,
) -> Vec<UsageRecord> {
    (0..n)
        .map(|i| UsageRecord {
            request_id: uuid::Uuid::new_v4(),
            received_at_ms: ms(start) + i * 1_000,
            tenant: ACME.into(),
            reservation: id.into(),
            deployment: "dep".into(),
            class: Some(class),
            session_id: None,
            tokens,
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

async fn get(base: &str, path: &str, key: &str) -> (u16, Value) {
    let r = reqwest::Client::new()
        .get(format!("{base}{path}"))
        .bearer_auth(key)
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or(Value::Null))
}

fn amounts(inv: &Value, kind: &str) -> Vec<i64> {
    inv["lines"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|l| l["kind"] == kind)
        .map(|l| l["amount"].as_i64().unwrap())
        .collect()
}

#[tokio::test]
async fn a_month_of_fees_spillover_and_credits_is_invoiced_and_finalised() {
    let (base, svc, tel) = spawn().await;
    // Agentic: 150,000 × 1.5 = 225,000 cents per CU per month.
    svc.clock.set(at("2026-10-11T00:00:00Z"));
    let id = svc
        .create(ACME, None, request("agents", &[("eu-west", 4)]))
        .await
        .unwrap()
        .resource
        .id;
    svc.clock.set(at("2026-10-21T00:00:00Z"));
    svc.update(
        ACME,
        &id,
        None,
        UpdateRequest {
            regions: Some(shares(&[("eu-west", 6)])),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Usage: one fast and one slow SLA window (50% attainment → 50% credit), and
    // spillover: 2M input, 0.5M cached, 1M output tokens.
    let t = at("2026-10-12T00:00:00Z");
    let mut usage = records(
        &id,
        t,
        100,
        TrafficClass::Provisioned,
        false,
        TokenBreakdown::default(),
    );
    usage.extend(records(
        &id,
        t + SignedDuration::from_hours(1),
        100,
        TrafficClass::Provisioned,
        true,
        TokenBreakdown::default(),
    ));
    usage.extend(records(
        &id,
        t + SignedDuration::from_hours(2),
        10,
        TrafficClass::Spillover,
        false,
        TokenBreakdown {
            uncached_prefill: 200_000,
            cached_prefill: 50_000,
            decode: 100_000,
        },
    ));
    // Burst is free, and rejected spillover never ran.
    usage.extend(records(
        &id,
        t + SignedDuration::from_hours(3),
        5,
        TrafficClass::Burst,
        false,
        TokenBreakdown {
            uncached_prefill: 1_000_000,
            cached_prefill: 0,
            decode: 0,
        },
    ));
    let mut rejected = records(
        &id,
        t + SignedDuration::from_hours(4),
        1,
        TrafficClass::Spillover,
        false,
        TokenBreakdown {
            uncached_prefill: 5_000_000,
            cached_prefill: 0,
            decode: 0,
        },
    );
    rejected[0].outcome = Outcome::Rejected(pt_core::RejectReason::EntitlementExhausted);
    usage.extend(rejected);
    tel.store.append("eu-west", usage, ms(t)).await.unwrap();

    // Mid-month: a draft up to now, without a credit yet.
    svc.clock.set(at("2026-10-16T00:00:00Z"));
    let (code, draft) = get(&base, "/v1/invoices/2026-10", ACME_KEY).await;
    assert_eq!(code, 200, "{draft}");
    assert_eq!(draft["status"], "draft");
    assert_eq!(
        amounts(&draft, "reservation_fee"),
        [(900_000.0 * 5.0 / 31.0_f64).round() as i64]
    );
    assert!(amounts(&draft, "sla_credit").is_empty());

    // After the month: fees for 10 days at 4 CU and 11 days at 6 CU, spillover, and credit.
    svc.clock.set(at("2026-11-01T12:00:00Z"));
    let (_, inv) = get(&base, "/v1/invoices/2026-10", ACME_KEY).await;
    assert_eq!(inv["status"], "draft", "still in its grace period");
    let fees = [
        (900_000.0 * 10.0 / 31.0_f64).round() as i64,
        (1_350_000.0 * 11.0 / 31.0_f64).round() as i64,
    ];
    assert_eq!(amounts(&inv, "reservation_fee"), fees);
    // 2M × 27 + 0.5M × 7 + 1M × 85 cents.
    assert_eq!(amounts(&inv, "spillover"), [54, 4, 85]);
    let fee: i64 = fees.iter().sum();
    assert_eq!(amounts(&inv, "sla_credit"), [-(fee * 50 / 100)]);
    assert_eq!(inv["subtotal"], fee + 143);
    assert_eq!(inv["credits"], -(fee * 50 / 100));
    assert_eq!(inv["total"], fee + 143 - fee * 50 / 100);
    assert_eq!(inv["currency"], "USD");

    // Not final within the 48 h grace; final after it, once only.
    assert!(billing::finalize_due(&svc, &tel).await.unwrap().is_empty());
    svc.clock.set(at("2026-11-03T01:00:00Z"));
    let stored = billing::finalize_due(&svc, &tel).await.unwrap();
    assert_eq!(stored.len(), 1, "globex had nothing to bill");
    assert_eq!(stored[0].status, InvoiceStatus::Final);
    assert!(billing::finalize_due(&svc, &tel).await.unwrap().is_empty());

    // Final invoices don't change when late data arrives.
    tel.store
        .append(
            "eu-west",
            records(
                &id,
                at("2026-10-30T00:00:00Z"),
                10,
                TrafficClass::Spillover,
                false,
                TokenBreakdown {
                    uncached_prefill: 1_000_000,
                    cached_prefill: 0,
                    decode: 0,
                },
            ),
            ms(at("2026-11-03T01:00:00Z")),
        )
        .await
        .unwrap();
    let (_, fin) = get(&base, "/v1/invoices/2026-10", ACME_KEY).await;
    assert_eq!(fin["status"], "final");
    assert_eq!(fin["total"], inv["total"]);
    assert!(fin["finalized_at"].is_string());

    // The list: November's draft and October's final invoice, newest first.
    let (_, list) = get(&base, "/v1/invoices", ACME_KEY).await;
    let periods: Vec<(&str, &str)> = list["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| (i["period"].as_str().unwrap(), i["status"].as_str().unwrap()))
        .collect();
    assert_eq!(periods, [("2026-11", "draft"), ("2026-10", "final")]);
}

#[tokio::test]
async fn invoice_periods_and_tenants() {
    let (base, svc, _) = spawn().await;
    svc.create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap();
    svc.clock.set(at("2026-12-15T00:00:00Z"));
    assert_eq!(get(&base, "/v1/invoices/2026-13", ACME_KEY).await.0, 422);
    // Older than the previous month and never finalised: its usage may be gone.
    assert_eq!(get(&base, "/v1/invoices/2026-10", ACME_KEY).await.0, 404);
    assert_eq!(get(&base, "/v1/invoices/2027-01", ACME_KEY).await.0, 404);
    assert_eq!(get(&base, "/v1/invoices/2026-12", "nope").await.0, 401);
    // Another tenant sees nothing of acme's.
    let (_, other) = get(&base, "/v1/invoices/2026-11", "sk-admin-globex-dev").await;
    assert!(other["lines"].as_array().unwrap().is_empty());
    let (_, mine) = get(&base, "/v1/invoices/2026-11", ACME_KEY).await;
    assert_eq!(
        amounts(&mine, "reservation_fee"),
        [225_000],
        "a full month of 1 CU"
    );
}
