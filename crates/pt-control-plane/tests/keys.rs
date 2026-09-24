//! Inference key rotation and revocation.

mod common;

use std::sync::Arc;

use common::*;
use jiff::SignedDuration;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::model::{EventKind, RotateKeyRequest};
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::service::{sha256_hex, ServiceError};
use pt_control_plane::store::MemoryStore;
use pt_control_plane::{app, in_memory, Service};
use serde_json::{json, Value};

type Svc = Arc<Service<MemoryStore, MemoryPlanner, ManualClock>>;

fn setup() -> (Svc, ManualClock) {
    let clock = ManualClock::new(t0());
    (in_memory(config(), clock.clone()), clock)
}

fn grace(m: u64) -> RotateKeyRequest {
    RotateKeyRequest {
        grace_minutes: Some(m),
    }
}

async fn eu_west_deployment(svc: &Svc, id: &str) -> pt_entitlement::DeploymentEntitlement {
    let snap = svc.snapshot("eu-west").await.unwrap();
    snap.deployments
        .into_iter()
        .find(|d| d.reservation == id)
        .unwrap()
}

#[tokio::test]
async fn rotation_keeps_the_old_key_for_the_grace_period() {
    let (svc, clock) = setup();
    let created = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap();
    let old = created.api_key.unwrap();
    let id = created.resource.id;

    let (pt, new) = svc
        .rotate_key(ACME, &id, None, Some(1), grace(30))
        .await
        .unwrap();
    assert_ne!(new, old);
    assert_eq!(pt.version, 2);
    assert_eq!(pt.primary().api_keys.len(), 2);
    let current: Vec<_> = pt
        .primary()
        .api_keys
        .iter()
        .filter(|k| k.is_current())
        .collect();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].sha256, sha256_hex(new.as_bytes()));
    let previous = pt
        .primary()
        .api_keys
        .iter()
        .find(|k| !k.is_current())
        .unwrap();
    assert_eq!(
        previous.expires_at,
        Some(t0() + SignedDuration::from_mins(30))
    );
    assert!(matches!(
        pt.events.last().unwrap().kind,
        EventKind::KeyRotated { .. }
    ));

    // Gateways get both, with the old key's expiry.
    let d = eu_west_deployment(&svc, &id).await;
    assert_eq!(d.api_key_sha256, sha256_hex(new.as_bytes()));
    assert_eq!(d.previous_keys.len(), 1);
    assert_eq!(
        d.previous_keys[0].api_key_sha256,
        sha256_hex(old.as_bytes())
    );
    assert_eq!(
        d.previous_keys[0].expires_at_ms,
        (t0() + SignedDuration::from_mins(30)).as_millisecond() as u64
    );

    // After the grace period the lifecycle loop drops the key.
    clock.advance(SignedDuration::from_mins(31));
    svc.run_lifecycle().await;
    let pt = svc.get(ACME, &id).await.unwrap();
    assert_eq!(pt.primary().api_keys.len(), 1);
    assert!(pt
        .events
        .iter()
        .any(|e| matches!(e.kind, EventKind::KeyExpired { .. })));
    assert!(eu_west_deployment(&svc, &id).await.previous_keys.is_empty());
}

#[tokio::test]
async fn zero_grace_replaces_the_key_immediately() {
    let (svc, _) = setup();
    let id = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource
        .id;
    let (pt, new) = svc
        .rotate_key(ACME, &id, None, None, grace(0))
        .await
        .unwrap();
    assert_eq!(pt.primary().api_keys.len(), 1);
    assert_eq!(pt.primary().api_keys[0].sha256, sha256_hex(new.as_bytes()));
    assert!(eu_west_deployment(&svc, &id).await.previous_keys.is_empty());
}

#[tokio::test]
async fn at_most_two_previous_keys_and_revocation() {
    let (svc, clock) = setup();
    let id = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource
        .id;
    for _ in 0..3 {
        clock.advance(SignedDuration::from_mins(1));
        svc.rotate_key(ACME, &id, None, None, grace(60))
            .await
            .unwrap();
    }
    let pt = svc.get(ACME, &id).await.unwrap();
    assert_eq!(pt.primary().api_keys.len(), 3, "current + two previous");
    assert!(pt
        .events
        .iter()
        .any(|e| matches!(e.kind, EventKind::KeyRevoked { .. })));

    let current = pt
        .primary()
        .api_keys
        .iter()
        .find(|k| k.is_current())
        .unwrap()
        .id
        .clone();
    let err = svc
        .revoke_key(ACME, &id, None, &current, None)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ServiceError::Conflict {
            code: "current_key",
            ..
        }
    ));

    let previous = pt
        .primary()
        .api_keys
        .iter()
        .find(|k| !k.is_current())
        .unwrap()
        .id
        .clone();
    let pt = svc
        .revoke_key(ACME, &id, None, &previous, None)
        .await
        .unwrap();
    assert_eq!(pt.primary().api_keys.len(), 2);
    let err = svc
        .revoke_key(ACME, &id, None, &previous, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ServiceError::Validation { .. }),
        "already gone"
    );
}

#[tokio::test]
async fn rotation_rules() {
    let (svc, clock) = setup();
    let id = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource
        .id;
    let err = svc
        .rotate_key(ACME, &id, None, None, grace(10_081))
        .await
        .unwrap_err();
    assert!(matches!(err, ServiceError::Validation { .. }));
    let err = svc
        .rotate_key(ACME, &id, None, Some(9), grace(1))
        .await
        .unwrap_err();
    assert!(matches!(err, ServiceError::PreconditionFailed { .. }));
    assert_eq!(
        svc.rotate_key(GLOBEX, &id, None, None, grace(1))
            .await
            .unwrap_err(),
        ServiceError::NotFound
    );

    // Ended reservations can't rotate.
    svc.delete(ACME, &id, None).await.unwrap();
    clock.set(svc.get(ACME, &id).await.unwrap().term_end);
    svc.run_lifecycle().await;
    let err = svc
        .rotate_key(ACME, &id, None, None, grace(1))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ServiceError::Conflict {
            code: "inactive",
            ..
        }
    ));
}

#[tokio::test]
async fn key_endpoints() {
    let (svc, _) = setup();
    let (routes, _) = app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, routes).await });
    let id = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource
        .id;
    let pts = format!("{base}/v1/provisioned-throughput/{id}");
    let client = reqwest::Client::new();

    let r = client
        .post(format!("{pts}/keys/rotate"))
        .bearer_auth(ACME_KEY)
        .header("if-match", "\"1\"")
        .json(&json!({ "grace_minutes": 15 }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["etag"], "\"2\"");
    let v: Value = r.json().await.unwrap();
    assert!(v["api_key"].as_str().unwrap().starts_with("ptk_"));

    // An empty body uses the default grace period.
    let r = client
        .post(format!("{pts}/keys/rotate"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let keys: Value = client
        .get(format!("{pts}/keys"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let keys = keys["data"].as_array().unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.iter().all(|k| k.get("sha256").is_none()));
    assert_eq!(
        keys.iter()
            .filter(|k| k.get("expires_at").is_none())
            .count(),
        1
    );

    let old = keys.iter().find(|k| k.get("expires_at").is_some()).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let r = client
        .delete(format!("{pts}/keys/{old}"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let current = keys.iter().find(|k| k.get("expires_at").is_none()).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let r = client
        .delete(format!("{pts}/keys/{current}"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    let r = client
        .post(format!("{pts}/keys/rotate"))
        .bearer_auth("sk-admin-globex-dev")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}
