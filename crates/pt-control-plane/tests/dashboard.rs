//! Dashboards (ADR-043): reports from controllers and routers, the system view with its
//! alerts, and per-customer usage for operators and for the customer.

mod common;

use std::sync::Arc;

use common::*;
use jiff::{SignedDuration, Timestamp};
use pt_control_plane::clock::{Clock, ManualClock};
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::store::MemoryStore;
use pt_control_plane::telemetry::CpTelemetry;
use pt_control_plane::{app, in_memory, Service};
use pt_core::{Outcome, Timings, TokenBreakdown, TrafficClass, UsageRecord};
use pt_entitlement::report::{PoolReport, ReportCondition, Roles};
use pt_telemetry::UsageStore;
use serde_json::{json, Value};

type Svc = Arc<Service<MemoryStore, MemoryPlanner, ManualClock>>;
type Tel = Arc<CpTelemetry<MemoryStore, MemoryPlanner, ManualClock>>;

const OPERATOR: &str = "sk-operator-dev";
const EU_WEST: &str = "region-token-eu-west-dev";
const GLOBEX_KEY: &str = "sk-admin-globex-dev";

fn at(s: &str) -> Timestamp {
    s.parse().unwrap()
}

async fn spawn() -> (String, Svc, Tel) {
    let svc = in_memory(config(), ManualClock::new(at("2026-10-01T00:00:00Z")));
    let (routes, tel) = app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, routes).await });
    (url, svc, tel)
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

async fn post(base: &str, path: &str, key: &str, body: Value) -> u16 {
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .bearer_auth(key)
        .json(&body)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

fn roles(aggregated: u32) -> Roles {
    Roles {
        aggregated,
        total: aggregated,
        ..Default::default()
    }
}

fn pool(ready: u32, conditions: &[(&str, &str, &str)]) -> PoolReport {
    PoolReport {
        namespace: "pt".into(),
        name: "maverick-h200".into(),
        model: "meta-llama/Llama-4-Maverick".into(),
        catalog_model: Some(MAVERICK.into()),
        profile: "maverick-h200-trtllm".into(),
        engine: "trtllm 1.2".into(),
        desired: roles(8),
        ready: roles(ready),
        floor: roles(6),
        min_available: roles(6),
        hot_spares: 1,
        warm_spares_loaded: Roles::default(),
        drain_surge: Roles::default(),
        draining_nodes: vec![],
        allocations: 1,
        allocated_wu_per_sec: 1_000.0,
        failover_wu_per_sec: 0.0,
        conditions: conditions
            .iter()
            .map(|(t, s, r)| ReportCondition {
                type_: t.to_string(),
                status: s.to_string(),
                reason: r.to_string(),
                message: String::new(),
            })
            .collect(),
        spare_pods: vec![],
    }
}

fn record(id: &str, t: Timestamp, i: u64) -> UsageRecord {
    UsageRecord {
        request_id: uuid::Uuid::new_v4(),
        received_at_ms: t.as_millisecond() as u64 + i * 1_000,
        tenant: ACME.into(),
        reservation: id.into(),
        deployment: "dep".into(),
        class: Some(TrafficClass::Provisioned),
        session_id: None,
        tokens: TokenBreakdown {
            uncached_prefill: 800,
            cached_prefill: 200,
            decode: 100,
        },
        kv_token_seconds: 0.0,
        wu_estimated: 100.0,
        wu_actual: 100.0,
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

fn alerts(view: &Value) -> Vec<String> {
    view["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            let s = |k: &str| a[k].as_str().unwrap().to_string();
            format!("{} {}: {}", s("severity"), s("scope"), s("message"))
        })
        .collect()
}

#[tokio::test]
async fn reports_feed_the_system_view_and_its_alerts() {
    let (base, svc, _tel) = spawn().await;
    svc.create(ACME, None, request("agents", &[("eu-west", 4)]))
        .await
        .unwrap();

    // Only region tokens may report, and a report must parse.
    let body = serde_json::to_value(pool(5, &[("Ready", "False", "Scaling")])).unwrap();
    assert_eq!(
        post(&base, "/internal/v1/reports/pools", OPERATOR, body.clone()).await,
        401
    );
    assert_eq!(
        post(&base, "/internal/v1/reports/pools", EU_WEST, json!({})).await,
        422
    );
    assert_eq!(
        post(&base, "/internal/v1/reports/pools", EU_WEST, body).await,
        204
    );
    let router = json!({ "router_id": "router-0", "status": { "workers": [] } });
    assert_eq!(
        post(&base, "/internal/v1/reports/routers", EU_WEST, router).await,
        204
    );

    assert_eq!(
        get(&base, "/internal/v1/dashboard/system", ACME_KEY)
            .await
            .0,
        401
    );
    let (code, view) = get(&base, "/internal/v1/dashboard/system", OPERATOR).await;
    assert_eq!(code, 200, "{view}");
    let pools = view["pools"].as_array().unwrap();
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0]["region"], "eu-west");
    assert_eq!(pools[0]["id"], "pt/maverick-h200");
    assert_eq!(pools[0]["report"]["ready"]["total"], 5);
    assert_eq!(pools[0]["stale"], false);
    assert_eq!(view["routers"][0]["id"], "router-0");
    let cap = view["capacity"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["region"] == "eu-west" && c["model"] == MAVERICK)
        .unwrap();
    assert_eq!(cap["reservations"], 1);
    assert_eq!(cap["cus_sold"], 4);
    let a = alerts(&view);
    assert!(
        a.iter()
            .any(|m| m.starts_with("critical") && m.contains("maverick-h200")),
        "ready 5 < min available 6 is critical: {a:?}"
    );

    // A pool that stops reporting goes stale, and so does the router.
    svc.clock
        .set(svc.clock.now() + SignedDuration::from_mins(11));
    let (_, view) = get(&base, "/internal/v1/dashboard/system", OPERATOR).await;
    assert_eq!(view["pools"][0]["stale"], true);
    assert_eq!(view["routers"][0]["stale"], true);
    let a = alerts(&view);
    assert!(a.iter().any(|m| m.contains("Stopped reporting")), "{a:?}");

    // A fresh healthy report clears the pool alerts.
    let healthy = serde_json::to_value(pool(8, &[("Ready", "True", "")])).unwrap();
    assert_eq!(
        post(&base, "/internal/v1/reports/pools", EU_WEST, healthy).await,
        204
    );
    let (_, view) = get(&base, "/internal/v1/dashboard/system", OPERATOR).await;
    let a = alerts(&view);
    assert!(!a.iter().any(|m| m.contains("maverick-h200")), "{a:?}");
}

#[tokio::test]
async fn customers_see_their_own_usage_and_operators_see_everyone() {
    let (base, svc, tel) = spawn().await;
    let t = at("2026-10-01T00:00:00Z");
    svc.clock.set(t);
    let id = svc
        .create(ACME, None, request("agents", &[("eu-west", 4)]))
        .await
        .unwrap()
        .resource
        .id;
    let usage: Vec<_> = (0..50)
        .map(|i| record(&id, t + SignedDuration::from_mins(10), i))
        .collect();
    tel.store
        .append("eu-west", usage, t.as_millisecond() as u64)
        .await
        .unwrap();
    svc.clock.set(t + SignedDuration::from_hours(2));

    let (code, list) = get(&base, "/internal/v1/dashboard/customers", OPERATOR).await;
    assert_eq!(code, 200);
    let acme = list["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["tenant"] == ACME)
        .unwrap();
    assert_eq!(acme["reservations"], 1);
    assert_eq!(acme["cus"], 4);

    let (code, own) = get(&base, "/v1/dashboard/usage?hours=6", ACME_KEY).await;
    assert_eq!(code, 200, "{own}");
    assert_eq!(own["tenant"], ACME);
    let r = &own["reservations"][0];
    assert_eq!(r["reservation"]["id"], id.as_str());
    assert_eq!(r["usage"]["summary"]["requests"]["provisioned"], 50);
    assert!(!r["usage"]["series"].as_array().unwrap().is_empty());
    assert!(r["sla"]["attainment_pct"].is_number(), "{}", r["sla"]);
    assert!(own["invoices"].is_array());

    // Another customer sees only their own reservations, never acme's.
    let (code, other) = get(&base, "/v1/dashboard/usage", GLOBEX_KEY).await;
    assert_eq!(code, 200);
    assert_eq!(other["tenant"], GLOBEX);
    assert_eq!(other["reservations"].as_array().unwrap().len(), 0);
    // The operator route needs the operator key.
    assert_eq!(
        get(&base, "/internal/v1/dashboard/usage/acme", ACME_KEY)
            .await
            .0,
        401
    );
    let (code, op) = get(&base, "/internal/v1/dashboard/usage/acme?hours=6", OPERATOR).await;
    assert_eq!(code, 200);
    assert_eq!(op["reservations"][0]["reservation"]["id"], id.as_str());
    assert_eq!(
        get(&base, "/internal/v1/dashboard/usage/nobody", OPERATOR)
            .await
            .0,
        404
    );

    if std::env::var("PT_DUMP_DASHBOARD").is_ok() {
        let (_, sys) = get(&base, "/internal/v1/dashboard/system", OPERATOR).await;
        println!("SYSTEM {}", serde_json::to_string_pretty(&sys).unwrap());
        println!("USAGE {}", serde_json::to_string_pretty(&own).unwrap());
    }
}

#[tokio::test]
async fn pages_and_assets_are_served() {
    let (base, _svc, _tel) = spawn().await;
    for (path, kind) in [
        ("/dashboard", "text/html"),
        ("/internal/dashboard", "text/html"),
        ("/dashboard/app.js", "text/javascript"),
        ("/internal/dashboard/app.css", "text/css"),
    ] {
        let r = reqwest::get(format!("{base}{path}")).await.unwrap();
        assert_eq!(r.status(), 200, "{path}");
        let ct = r.headers()["content-type"].to_str().unwrap().to_string();
        assert!(ct.starts_with(kind), "{path}: {ct}");
    }
}
