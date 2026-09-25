//! The SQL store against a real database. Set `PT_TEST_DATABASE_URL` (for example
//! `postgres://postgres@127.0.0.1:55432/pt`) to run these. Without it they're skipped.
//! Each test works in its own schema, so tests don't interfere.

mod common;

use std::str::FromStr;
use std::sync::Arc;

use common::*;
use jiff::SignedDuration;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::model::{
    CreateDeploymentRequest, DeclareIncident, RotateKeyRequest, Sku, UpdateRequest,
};
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::service::ServiceError;
use pt_control_plane::sql::SqlStore;
use pt_control_plane::store::{IdempotencyRecord, Store, StoreError};
use pt_control_plane::{app, with_store, Service};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

type Svc = Arc<Service<SqlStore, MemoryPlanner, ManualClock>>;

/// A store in a fresh schema, or `None` when no test database is configured.
async fn store(name: &str) -> Option<SqlStore> {
    let Ok(url) = std::env::var("PT_TEST_DATABASE_URL") else {
        eprintln!("PT_TEST_DATABASE_URL not set; skipping {name}");
        return None;
    };
    let schema = format!("t_{name}_{}", uuid::Uuid::new_v4().simple());
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
        .max_connections(4)
        .connect_with(opts)
        .await
        .unwrap();
    let store = SqlStore::from_pool(pool);
    assert_eq!(store.migrate().await.unwrap(), [1, 2, 3]);
    assert!(store.migrate().await.unwrap().is_empty(), "idempotent");
    Some(store)
}

async fn service(store: SqlStore, clock: &ManualClock) -> Svc {
    with_store(config(), store, clock.clone()).await.unwrap()
}

#[tokio::test]
async fn state_survives_a_restart() {
    let Some(db) = store("restart").await else {
        return;
    };
    let clock = ManualClock::new(t0());
    let svc = service(db.clone(), &clock).await;

    // A multi-region reservation with a second deployment, a rotated key, a resize, and an
    // incident: every kind of child row.
    let mut req = request("agents", &[("eu-west", 6), ("eu-central", 2)]);
    req.sku = Sku::MultiRegion;
    let created = svc.create(ACME, Some("idem-1"), req.clone()).await.unwrap();
    let id = created.resource.id.clone();
    let (_, staging, _) = svc
        .create_deployment(
            ACME,
            &id,
            None,
            CreateDeploymentRequest {
                name: "staging".into(),
                max_share: Some(0.25),
            },
        )
        .await
        .unwrap();
    clock.advance(SignedDuration::from_mins(3));
    svc.rotate_key(ACME, &id, None, None, RotateKeyRequest::default())
        .await
        .unwrap();
    svc.update(
        ACME,
        &id,
        None,
        UpdateRequest {
            regions: Some(shares(&[("eu-west", 8), ("eu-central", 2)])),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let incident = svc
        .declare_incident(DeclareIncident {
            region: "eu-west".into(),
            started_at: None,
            description: "Region down".into(),
        })
        .await
        .unwrap();
    let before = svc.get(ACME, &id).await.unwrap();
    let snapshot_before = svc.snapshot("eu-central").await.unwrap().unwrap();
    let avail = |s: &Svc, r| s.planner.available(r, MAVERICK).unwrap();
    let (west, central) = (avail(&svc, "eu-west"), avail(&svc, "eu-central"));
    assert_eq!(west, 200 - 8 - 2, "shares plus headroom");
    drop(svc);

    // A new process over the same database.
    let svc = service(db, &clock).await;
    let after = svc.get(ACME, &id).await.unwrap();
    assert_eq!(after, before, "the resource reads back exactly");
    assert_eq!(after.deployments.len(), 2);
    assert_eq!(after.deployments[1].id, staging.id);
    assert_eq!(
        after.primary().api_keys.len(),
        2,
        "current and rotated-out keys"
    );
    assert!(after.events.len() >= 5);
    assert_eq!(
        (avail(&svc, "eu-west"), avail(&svc, "eu-central")),
        (west, central),
        "reserved capacity rebuilt"
    );
    let snapshot_after = svc.snapshot("eu-central").await.unwrap().unwrap();
    assert_eq!(snapshot_after.reservations, snapshot_before.reservations);
    assert_eq!(
        snapshot_after.deployments, snapshot_before.deployments,
        "key hashes kept"
    );
    assert_eq!(snapshot_after.failovers, snapshot_before.failovers);
    assert_eq!(svc.incidents().await.unwrap(), [incident]);

    // The idempotency key still replays, without the secret.
    let replay = svc.create(ACME, Some("idem-1"), req).await.unwrap();
    assert!(replay.replayed && replay.api_key.is_none());
    assert_eq!(replay.resource, after);

    // The lifecycle still works on loaded state: the term ends and capacity is freed.
    svc.update(
        ACME,
        &id,
        None,
        UpdateRequest {
            auto_renew: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    clock.set(after.term_end + SignedDuration::from_secs(1));
    assert_eq!(svc.run_lifecycle().await.ended, 1);
    assert_eq!(avail(&svc, "eu-west"), 200);
    assert!(svc.list(ACME, None, false).await.unwrap().is_empty());
    assert_eq!(svc.list(ACME, None, true).await.unwrap().len(), 1);
}

#[tokio::test]
async fn store_contract() {
    let Some(db) = store("contract").await else {
        return;
    };
    let clock = ManualClock::new(t0());
    let svc = service(db.clone(), &clock).await;
    let pt = svc
        .create(ACME, None, request("a", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource;

    // Tenants can't see each other's resources.
    assert!(db.get(GLOBEX, &pt.id).await.unwrap().is_none());
    assert!(db.list(GLOBEX).await.unwrap().is_empty());

    // Optimistic concurrency.
    let mut stale = pt.clone();
    stale.name = "b".into();
    stale.version += 1;
    db.update(stale.clone(), pt.version).await.unwrap();
    assert_eq!(
        db.update(stale.clone(), pt.version).await,
        Err(StoreError::VersionConflict(pt.id.clone()))
    );
    let mut ghost = stale.clone();
    ghost.id = "pt-missing".into();
    assert_eq!(
        db.update(ghost, 1).await,
        Err(StoreError::NotFound("pt-missing".into()))
    );

    // Live names are unique per tenant, enforced by the database too.
    let mut twin = pt.clone();
    twin.id = "pt-twin".into();
    twin.name = "b".into();
    for d in &mut twin.deployments {
        d.id = format!("{}-twin", d.id);
        for k in &mut d.api_keys {
            k.id = format!("{}-twin", k.id);
            k.sha256 = format!("{}0", &k.sha256[1..]);
        }
    }
    assert!(matches!(
        db.insert(twin.clone()).await,
        Err(StoreError::AlreadyExists(_))
    ));
    assert!(
        db.get(ACME, "pt-twin").await.unwrap().is_none(),
        "rolled back"
    );
    twin.name = "c".into();
    db.insert(twin).await.unwrap();
    assert_eq!(db.list_live().await.unwrap().len(), 2);

    // Idempotency keys: first write wins.
    let rec = IdempotencyRecord {
        resource_id: pt.id.clone(),
        fingerprint: "f".into(),
    };
    db.idempotency_put(ACME, "k", rec.clone()).await.unwrap();
    assert!(matches!(
        db.idempotency_put(ACME, "k", rec.clone()).await,
        Err(StoreError::AlreadyExists(_))
    ));
    assert_eq!(db.idempotency_get(ACME, "k").await.unwrap(), Some(rec));
    assert_eq!(db.idempotency_get(GLOBEX, "k").await.unwrap(), None);

    // One open incident per region, also under a race the service can't see.
    svc.declare_incident(DeclareIncident {
        region: "eu-west".into(),
        started_at: None,
        description: "one".into(),
    })
    .await
    .unwrap();
    let mut dup = svc.incidents().await.unwrap()[0].clone();
    dup.id = "inc-dup".into();
    assert!(matches!(
        db.insert_incident(dup).await,
        Err(StoreError::AlreadyExists(_))
    ));
}

#[tokio::test]
async fn concurrent_updates_have_one_winner() {
    let Some(db) = store("race").await else {
        return;
    };
    let clock = ManualClock::new(t0());
    let svc = service(db, &clock).await;
    let pt = svc
        .create(ACME, None, request("a", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource;
    let rename = |n: &str| UpdateRequest {
        name: Some(n.into()),
        ..Default::default()
    };
    let (a, b) = tokio::join!(
        svc.update(ACME, &pt.id, Some(pt.version), rename("x")),
        svc.update(ACME, &pt.id, Some(pt.version), rename("y")),
    );
    let ok = [&a, &b].iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok, 1, "{a:?} {b:?}");
    let loser = if a.is_err() { a } else { b };
    assert!(matches!(
        loser,
        Err(ServiceError::Conflict { .. } | ServiceError::PreconditionFailed { .. })
    ));
    assert_eq!(svc.get(ACME, &pt.id).await.unwrap().version, pt.version + 1);
}

#[tokio::test]
async fn an_unreachable_store_never_publishes_empty_entitlements() {
    let Some(db) = store("outage").await else {
        return;
    };
    let clock = ManualClock::new(t0());
    let svc = service(db.clone(), &clock).await;
    svc.create(ACME, None, request("a", &[("eu-west", 1)]))
        .await
        .unwrap();
    let (routes, _) = app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, routes).await });
    let http = reqwest::Client::new();
    let snapshot = || {
        http.get(format!("{base}/internal/v1/entitlements/eu-west"))
            .bearer_auth("region-token-eu-west-dev")
            .send()
    };
    assert_eq!(snapshot().await.unwrap().status(), 200);

    db.pool().close().await;
    let r = snapshot().await.unwrap();
    assert_eq!(r.status(), 503, "gateways keep their last snapshot");
    assert!(matches!(
        svc.snapshot("eu-west").await,
        Err(ServiceError::Unavailable(_))
    ));
    let r = http
        .get(format!("{base}/v1/provisioned-throughput"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["error"]["code"], "store_unavailable");
    // Background loops skip the run instead of acting on "nothing".
    assert_eq!(svc.run_lifecycle().await, Default::default());
    assert!(svc.run_failover().await.declared.is_empty());
}

#[tokio::test]
async fn final_invoices_are_stored_once_and_survive_a_restart() {
    let Some(db) = store("invoices").await else {
        return;
    };
    let clock = ManualClock::new("2026-10-11T00:00:00Z".parse().unwrap());
    let svc = service(db.clone(), &clock).await;
    svc.create(ACME, None, request("agents", &[("eu-west", 2)]))
        .await
        .unwrap();
    clock.set("2026-11-05T00:00:00Z".parse().unwrap());
    let (_, tel) = app(svc.clone());
    let stored = pt_control_plane::billing::finalize_due(&svc, &tel)
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    let first = stored[0].clone();
    assert!(matches!(
        db.insert_invoice(first.clone()).await,
        Err(StoreError::AlreadyExists(_))
    ));

    drop(svc);
    let svc = service(db.clone(), &clock).await;
    let (_, tel) = app(svc.clone());
    assert_eq!(
        db.get_invoice(ACME, "2026-10").await.unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        db.list_invoices(ACME).await.unwrap(),
        std::slice::from_ref(&first)
    );
    assert!(db.list_invoices(GLOBEX).await.unwrap().is_empty());
    // The stored invoice is served, and nothing is finalised twice.
    let served = pt_control_plane::billing::invoice(&svc, &tel, ACME, "2026-10")
        .await
        .unwrap();
    assert_eq!(served, first);
    assert!(pt_control_plane::billing::finalize_due(&svc, &tel)
        .await
        .unwrap()
        .is_empty());
}
