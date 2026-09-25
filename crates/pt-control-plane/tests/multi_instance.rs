//! Several control-plane instances over one database (ADR-023). Set `PT_TEST_DATABASE_URL`
//! to run these; skipped otherwise. Each test uses its own schema.

mod common;

use std::str::FromStr;
use std::sync::Arc;

use common::*;
use jiff::SignedDuration;
use pt_control_plane::background::{lead, LEASE_TTL};
use pt_control_plane::clock::ManualClock;
use pt_control_plane::failover::Health;
use pt_control_plane::model::Heartbeat;
use pt_control_plane::planner::{CapacityPlanner, PlanError};
use pt_control_plane::service::ServiceError;
use pt_control_plane::sql::SqlStore;
use pt_control_plane::sql_planner::SqlPlanner;
use pt_control_plane::store::Store;
use pt_control_plane::{with_sql, Service};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

type Svc = Arc<Service<SqlStore, SqlPlanner, ManualClock>>;

async fn store(name: &str) -> Option<SqlStore> {
    let Ok(url) = std::env::var("PT_TEST_DATABASE_URL") else {
        eprintln!("PT_TEST_DATABASE_URL not set; skipping {name}");
        return None;
    };
    let schema = format!("t_mi_{name}_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    let opts = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(opts)
        .await
        .unwrap();
    let store = SqlStore::from_pool(pool);
    store.migrate().await.unwrap();
    Some(store)
}

/// Two instances sharing one database and one clock.
async fn pair(name: &str) -> Option<(Svc, Svc, ManualClock)> {
    let db = store(name).await?;
    let clock = ManualClock::new(t0());
    let a = with_sql(config(), db.clone(), clock.clone()).await.unwrap();
    let b = with_sql(config(), db, clock.clone()).await.unwrap();
    Some((a, b, clock))
}

#[tokio::test]
async fn instances_cannot_oversell_together() {
    let Some((a, b, _)) = pair("oversell").await else {
        return;
    };
    // eu-central has 100 CUs. Two instances each try to sell 60 at the same moment.
    let (x, y) = tokio::join!(
        a.create(ACME, None, request("a", &[("eu-central", 60)])),
        b.create(GLOBEX, None, request("b", &[("eu-central", 60)])),
    );
    let won = [&x, &y].iter().filter(|r| r.is_ok()).count();
    assert_eq!(won, 1, "{x:?} {y:?}");
    let lost = if x.is_err() { x } else { y };
    assert!(matches!(
        lost,
        Err(ServiceError::Capacity(PlanError::CapacityUnavailable {
            available: 40,
            ..
        }))
    ));
    // Both instances see the same counters.
    assert_eq!(
        a.planner.available("eu-central", MAVERICK).await.unwrap(),
        Some(40)
    );
    assert_eq!(
        b.planner.available("eu-central", MAVERICK).await.unwrap(),
        Some(40)
    );
}

#[tokio::test]
async fn a_change_through_one_instance_reaches_snapshots_from_the_other() {
    let Some((a, b, _)) = pair("version").await else {
        return;
    };
    let before = b.snapshot("eu-west").await.unwrap().unwrap();
    let mut wake = b.subscribe();
    wake.borrow_and_update();

    let pt = a
        .create(ACME, None, request("agents", &[("eu-west", 2)]))
        .await
        .unwrap()
        .resource;

    // B's snapshot has A's change under a newer version.
    let after = b.snapshot("eu-west").await.unwrap().unwrap();
    assert!(after.version > before.version);
    assert!(after.reservations.iter().any(|r| r.id == pt.id));
    // B's long-polls wake: its version poller picks up the shared counter.
    b.sync_version().await.unwrap();
    assert!(wake.has_changed().unwrap());
    assert_eq!(*wake.borrow(), after.version);

    // A new instance starts with a newer version still (restarts always republish).
    let c = with_sql(config(), a.store.clone(), a.clock.clone())
        .await
        .unwrap();
    assert!(c.snapshot("eu-west").await.unwrap().unwrap().version > after.version);
}

#[tokio::test]
async fn heartbeats_through_any_instance_count() {
    let Some((a, b, clock)) = pair("heartbeats").await else {
        return;
    };
    let hb = |gw: &str| Heartbeat {
        gateway_id: gw.into(),
        serving: true,
        snapshot_version: 1,
        key_id: Some("k1".into()),
    };
    a.heartbeat("eu-west", &hb("gw-1")).await.unwrap();
    b.heartbeat("eu-west", &hb("gw-2")).await.unwrap();
    let west = |s: Vec<pt_control_plane::failover::RegionStatus>| {
        s.into_iter().find(|r| r.region == "eu-west").unwrap()
    };
    let seen_by_b = west(b.region_statuses().await.unwrap());
    assert_eq!(seen_by_b.health, Health::Serving);
    assert_eq!(
        seen_by_b.gateways, 2,
        "gateways reached different instances"
    );
    assert_eq!(seen_by_b.snapshot_key_ids.get("k1"), Some(&2));
    // Silent for longer than the timeout: down, as judged by either instance.
    clock.advance(SignedDuration::from_secs(31));
    assert_eq!(
        west(a.region_statuses().await.unwrap()).health,
        Health::Down
    );
}

#[tokio::test]
async fn leadership_hands_over_when_the_leader_stops_renewing() {
    let Some((a, b, clock)) = pair("lease").await else {
        return;
    };
    assert!(lead(&a, "cp-a").await);
    assert!(!lead(&b, "cp-b").await, "one leader at a time");
    // A renews: still A.
    clock.advance(SignedDuration::from_secs(5));
    assert!(lead(&a, "cp-a").await);
    assert!(!lead(&b, "cp-b").await);
    // A stops renewing (crashed). Once the lease expires, B takes over.
    clock.advance(LEASE_TTL - SignedDuration::from_secs(1));
    assert!(!lead(&b, "cp-b").await, "not before expiry");
    clock.advance(SignedDuration::from_secs(1));
    assert!(lead(&b, "cp-b").await);
    assert!(
        !lead(&a, "cp-a").await,
        "a returning instance doesn't steal it back"
    );
    let lease = b
        .store
        .lease(pt_control_plane::service::LEADER_LEASE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.holder, "cp-b");
}

#[tokio::test]
async fn reconcile_repairs_a_leak_but_never_an_in_flight_sale() {
    let Some((a, b, _)) = pair("reconcile").await else {
        return;
    };
    a.create(ACME, None, request("agents", &[("eu-west", 4)]))
        .await
        .unwrap();
    // An instance reserved capacity, then died before saving the reservation.
    a.planner
        .reserve(MAVERICK, &shares(&[("eu-west", 3)]), &shape(32_768))
        .await
        .unwrap();
    assert_eq!(
        a.planner.available("eu-west", MAVERICK).await.unwrap(),
        Some(193)
    );

    // First run: seen, not corrected (it could be a sale in flight).
    let drift = b.reconcile_capacity().await.unwrap();
    assert_eq!(drift.len(), 1);
    assert_eq!(
        (drift[0].reserved, drift[0].expected, drift[0].corrected),
        (7, 4, false)
    );
    assert_eq!(
        a.planner.available("eu-west", MAVERICK).await.unwrap(),
        Some(193)
    );
    // Second run, same drift: corrected.
    let drift = b.reconcile_capacity().await.unwrap();
    assert!(drift[0].corrected);
    assert_eq!(
        a.planner.available("eu-west", MAVERICK).await.unwrap(),
        Some(196)
    );
    assert!(b.reconcile_capacity().await.unwrap().is_empty());

    // A sale in flight between runs changes the drift, so it isn't undone.
    a.planner
        .reserve(MAVERICK, &shares(&[("eu-west", 2)]), &shape(32_768))
        .await
        .unwrap();
    assert!(!b.reconcile_capacity().await.unwrap()[0].corrected);
    a.create(ACME, None, request("second", &[("eu-west", 2)]))
        .await
        .unwrap(); // this reserves 2 more; the stray 2 above stays a leak
    let drift = b.reconcile_capacity().await.unwrap();
    assert_eq!((drift[0].reserved, drift[0].expected), (8, 6));
    assert!(
        !drift[0].corrected,
        "the drift changed, so it's judged afresh"
    );
}
