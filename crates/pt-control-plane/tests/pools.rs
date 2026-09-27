//! Several pools per model in a region (ADR-045): best-fit placement, relocation when a
//! share outgrows its pool, the operator move API, snapshots priced on the placed pool, and
//! restarts.

mod common;

use std::sync::Arc;

use common::*;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::config::{CapacityConfig, ControlPlaneConfig};
use pt_control_plane::model::{EventKind, UpdateRequest};
use pt_control_plane::planner::{MemoryPlanner, PoolShare};
use pt_control_plane::service::ServiceError;
use pt_control_plane::store::MemoryStore;
use pt_control_plane::{app, with_store, Service};
use serde_json::{json, Value};

const OPERATOR: &str = "sk-operator-dev";
const B200: &str = "llama-4-maverick.b200.trtllm-1.2.tp8";
const H200: &str = "llama-4-maverick.h200.vllm-0.11.tp8";

type Svc = Arc<Service<Arc<MemoryStore>, MemoryPlanner, ManualClock>>;

/// eu-west gets a second Maverick pool: 6 H200 replicas (100 Agentic CUs), beside the
/// default 8 B200 replicas (227 Agentic CUs).
fn two_pools() -> ControlPlaneConfig {
    let mut c = config();
    c.capacity.push(CapacityConfig {
        region: "eu-west".into(),
        model: MAVERICK.into(),
        pool: Some("maverick-h200".into()),
        replicas: 6,
        max_context: 131_072,
        profile: H200.into(),
    });
    c.validate().unwrap();
    c
}

async fn service(store: Arc<MemoryStore>) -> Svc {
    with_store(two_pools(), store, ManualClock::new(t0()))
        .await
        .unwrap()
}

async fn pools(svc: &Svc, id: &str) -> Vec<(String, String)> {
    let pt = svc.get(ACME, id).await.unwrap();
    pt.regions
        .iter()
        .map(|r| (r.region.clone(), svc.pool_of(&pt, &r.region).unwrap()))
        .collect()
}

fn eu_west(pool: &str) -> Vec<(String, String)> {
    vec![("eu-west".into(), pool.into())]
}

async fn create(svc: &Svc, name: &str, cus: u32) -> String {
    svc.create(ACME, None, request(name, &[("eu-west", cus)]))
        .await
        .unwrap()
        .resource
        .id
}

#[tokio::test]
async fn best_fit_then_relocation_when_a_share_outgrows_its_pool() {
    let svc = service(Arc::new(MemoryStore::default())).await;
    // 50 fits both pools; the H200 pool has less room left over, so it's the best fit.
    let small = create(&svc, "small", 50).await;
    assert_eq!(pools(&svc, &small).await, eu_west("maverick-h200"));
    // 100 fits only the B200 pool now.
    let big = create(&svc, "big", 100).await;
    assert_eq!(pools(&svc, &big).await, eu_west(MAVERICK));
    // The customer never sees pools.
    let body = serde_json::to_value(svc.get(ACME, &small).await.unwrap()).unwrap();
    assert!(body.get("placements").is_none(), "{body}");

    // Snapshots price each share with its pool's profile.
    let snap = svc.snapshot("eu-west").await.unwrap().unwrap();
    let profile = |id: &str| {
        snap.reservations
            .iter()
            .find(|r| r.id == id)
            .unwrap()
            .profile
            .clone()
    };
    assert_eq!((profile(&small), profile(&big)), (H200.into(), B200.into()));

    // Growing "small" to 120 doesn't fit the H200 pool (50 left), so the whole share
    // moves to the B200 pool, which has 127 left.
    let pt = svc
        .update(
            ACME,
            &small,
            None,
            UpdateRequest {
                regions: Some(shares(&[("eu-west", 120)])),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(pools(&svc, &small).await, eu_west(MAVERICK));
    assert!(pt
        .events
        .iter()
        .any(|e| matches!(&e.kind, EventKind::Relocated { region } if region == "eu-west")));
    // The move opens an SLA grace window while the new pool catches up.
    let windows = pt_control_plane::telemetry::exclusion_windows(
        &pt,
        &[],
        &svc.config.telemetry,
        t0().as_millisecond() as u64,
    );
    assert!(
        windows.iter().any(|w| w.reason == "relocation"),
        "{windows:?}"
    );
    assert_eq!(
        svc.planner.available_micro("eu-west", "maverick-h200"),
        Some(6_000_000)
    );
    assert_eq!(
        svc.planner
            .available("eu-west", MAVERICK, pt_core::Tier::Agentic),
        Some(100),
        "the H200 pool is empty again and has the most room"
    );

    // Growth that fits no single pool fails and changes nothing.
    let err = svc
        .update(
            ACME,
            &big,
            None,
            UpdateRequest {
                regions: Some(shares(&[("eu-west", 210)])),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ServiceError::Capacity(_)), "{err:?}");
    assert_eq!(pools(&svc, &big).await, eu_west(MAVERICK));
    assert_eq!(
        svc.planner.available_micro("eu-west", "maverick-h200"),
        Some(6_000_000)
    );
}

#[tokio::test]
async fn operators_move_shares_between_pools() {
    let svc = service(Arc::new(MemoryStore::default())).await;
    let (routes, _) = app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, routes).await });
    let http = reqwest::Client::new();
    let post = |id: &str, key: &str, body: Value| {
        http.post(format!("{base}/internal/v1/reservations/{id}/move"))
            .bearer_auth(key)
            .json(&body)
            .send()
    };

    let id = create(&svc, "agents", 150).await; // only the B200 pool fits
    let placements: Value = http
        .get(format!("{base}/internal/v1/reservations/{id}/placements"))
        .bearer_auth(OPERATOR)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        placements["placements"],
        json!([{ "region": "eu-west", "pool": MAVERICK, "cus": 150 }])
    );

    // 150 doesn't fit the H200 pool: 409, and nothing moves.
    let to_h200 = json!({ "region": "eu-west", "pool": "maverick-h200" });
    let r = post(&id, OPERATOR, to_h200.clone()).await.unwrap();
    assert_eq!(r.status(), 409);
    assert_eq!(pools(&svc, &id).await, eu_west(MAVERICK));

    // A smaller one moves, and the customer sees a relocation, not the pool.
    let small = create(&svc, "small", 60).await;
    assert_eq!(
        pools(&svc, &small).await,
        eu_west(MAVERICK),
        "best fit: 77 left beats 100"
    );
    assert_eq!(
        post(&small, ACME_KEY, to_h200.clone())
            .await
            .unwrap()
            .status(),
        401
    );
    let r = post(&small, OPERATOR, to_h200.clone()).await.unwrap();
    assert_eq!(r.status(), 200);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["placements"][0]["pool"], "maverick-h200");
    assert_eq!(pools(&svc, &small).await, eu_west("maverick-h200"));
    let pt = svc.get(ACME, &small).await.unwrap();
    assert!(matches!(
        pt.events.last().unwrap().kind,
        EventKind::Relocated { .. }
    ));
    // Moving to where it already is changes nothing.
    let version = pt.version;
    assert_eq!(post(&small, OPERATOR, to_h200).await.unwrap().status(), 200);
    assert_eq!(svc.get(ACME, &small).await.unwrap().version, version);

    // Bad requests.
    let bad = |id: &str, body: Value| post(id, OPERATOR, body);
    assert_eq!(
        bad(&small, json!({ "region": "eu-west", "pool": "nope" }))
            .await
            .unwrap()
            .status(),
        422
    );
    assert_eq!(
        bad(&small, json!({ "region": "us-east", "pool": MAVERICK }))
            .await
            .unwrap()
            .status(),
        422
    );
    assert_eq!(
        bad(
            "pt-missing",
            json!({ "region": "eu-west", "pool": MAVERICK })
        )
        .await
        .unwrap()
        .status(),
        404
    );
}

#[tokio::test]
async fn placements_survive_a_restart_and_old_reservations_use_the_default_pool() {
    let store = Arc::new(MemoryStore::default());
    let svc = service(store.clone()).await;
    let h200 = create(&svc, "on-h200", 40).await;
    let b200 = create(&svc, "on-b200", 150).await;
    assert_eq!(pools(&svc, &h200).await, eu_west("maverick-h200"));

    // A reservation sold before pools had ids has no placements: it lives on the default
    // pool, whose id is the model id.
    let mut legacy = svc.get(ACME, &b200).await.unwrap();
    let expected = legacy.version;
    legacy.placements.clear();
    legacy.version += 1;
    pt_control_plane::store::Store::update(&*store, legacy, expected)
        .await
        .unwrap();
    assert_eq!(pools(&svc, &b200).await, eu_west(MAVERICK));

    // A new instance rebuilds each pool's counters from the placements.
    let again = service(store).await;
    assert_eq!(
        again.planner.available_micro("eu-west", "maverick-h200"),
        svc.planner.available_micro("eu-west", "maverick-h200")
    );
    assert_eq!(
        again.planner.available_micro("eu-west", MAVERICK),
        svc.planner.available_micro("eu-west", MAVERICK)
    );
    assert!(again.reconcile_capacity().await.unwrap().is_empty());
    let placed: Vec<PoolShare> = again.placements(&b200).await.unwrap();
    assert_eq!(placed[0].pool, MAVERICK);
}

#[test]
fn pool_config_rules() {
    let mut dup = config();
    dup.capacity.push(CapacityConfig {
        region: "eu-west".into(),
        model: MAVERICK.into(),
        pool: None, // defaults to the model id, which the first pool already uses
        replicas: 2,
        max_context: 32_768,
        profile: H200.into(),
    });
    assert!(dup
        .validate()
        .unwrap_err()
        .to_string()
        .contains("duplicate capacity pool"));

    let mut unnamed = two_pools();
    unnamed
        .capacity_changes
        .push(pt_control_plane::config::CapacityChange {
            region: "eu-west".into(),
            model: MAVERICK.into(),
            pool: None,
            add_replicas: 2,
            from: t0(),
        });
    assert!(unnamed
        .validate()
        .unwrap_err()
        .to_string()
        .contains("must name a pool"));
    let mut named = two_pools();
    named
        .capacity_changes
        .push(pt_control_plane::config::CapacityChange {
            region: "eu-west".into(),
            model: MAVERICK.into(),
            pool: Some("maverick-h200".into()),
            add_replicas: 2,
            from: t0(),
        });
    named.validate().unwrap();
}
