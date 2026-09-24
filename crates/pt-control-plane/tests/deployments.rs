//! Several deployments per reservation: their own keys and an optional cap, sharing one
//! entitlement and boundary policy.

mod common;

use std::sync::Arc;

use common::*;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::model::{CreateDeploymentRequest, RotateKeyRequest, UpdateDeploymentRequest};
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::service::{sha256_hex, ServiceError};
use pt_control_plane::store::MemoryStore;
use pt_control_plane::{app, in_memory, Service};
use serde_json::{json, Value};

type Svc = Arc<Service<MemoryStore, MemoryPlanner, ManualClock>>;

fn svc() -> Svc {
    in_memory(config(), ManualClock::new(t0()))
}

fn staging(max_share: Option<f64>) -> CreateDeploymentRequest {
    CreateDeploymentRequest {
        name: "staging".into(),
        max_share,
    }
}

async fn reservation(svc: &Svc) -> String {
    svc.create(ACME, None, request("agents", &[("eu-west", 2)]))
        .await
        .unwrap()
        .resource
        .id
}

#[tokio::test]
async fn deployments_get_their_own_keys_and_reach_snapshots() {
    let svc = svc();
    let id = reservation(&svc).await;

    let (pt, dep, key) = svc
        .create_deployment(ACME, &id, Some(1), staging(Some(0.25)))
        .await
        .unwrap();
    assert_eq!(pt.version, 2);
    assert_eq!(pt.deployments.len(), 2);
    assert_eq!(pt.primary().name, "default");
    assert_eq!(dep.name, "staging");
    assert_eq!(dep.max_share, Some(0.25));
    assert_eq!(dep.api_keys[0].sha256, sha256_hex(key.as_bytes()));

    let snap = svc.snapshot("eu-west").await.unwrap().unwrap();
    assert_eq!(snap.reservations.len(), 1, "one shared entitlement");
    assert_eq!(snap.deployments.len(), 2);
    let s = snap.deployments.iter().find(|d| d.id == dep.id).unwrap();
    assert_eq!(s.reservation, id);
    assert_eq!(s.max_share, Some(0.25));
    assert_eq!(s.api_key_sha256, sha256_hex(key.as_bytes()));
    let p = snap.deployments.iter().find(|d| d.id != dep.id).unwrap();
    assert_eq!(p.max_share, None);
    assert_eq!(
        p.boundary_policy, s.boundary_policy,
        "the policy is the reservation's"
    );

    // Rotating one deployment's key leaves the other's alone.
    let primary_key = pt.primary().current_key().unwrap().sha256.clone();
    let (pt, new) = svc
        .rotate_key(
            ACME,
            &id,
            Some(&dep.id),
            None,
            RotateKeyRequest {
                grace_minutes: Some(0),
            },
        )
        .await
        .unwrap();
    let d = pt.deployments.iter().find(|d| d.id == dep.id).unwrap();
    assert_eq!(d.current_key().unwrap().sha256, sha256_hex(new.as_bytes()));
    assert_eq!(pt.primary().current_key().unwrap().sha256, primary_key);
}

#[tokio::test]
async fn deployment_rules() {
    let svc = svc();
    let id = reservation(&svc).await;
    let conflict_code = |e: ServiceError| match e {
        ServiceError::Conflict { code, .. } => code,
        other => panic!("expected conflict, got {other:?}"),
    };

    let (_, dep, _) = svc
        .create_deployment(ACME, &id, None, staging(None))
        .await
        .unwrap();
    assert_eq!(
        conflict_code(
            svc.create_deployment(ACME, &id, None, staging(None))
                .await
                .unwrap_err()
        ),
        "name_taken"
    );
    for bad in [0.0, -0.1, 1.5, f64::NAN] {
        let req = CreateDeploymentRequest {
            name: "x".into(),
            max_share: Some(bad),
        };
        assert!(matches!(
            svc.create_deployment(ACME, &id, None, req)
                .await
                .unwrap_err(),
            ServiceError::Validation { .. }
        ));
    }
    let bad_name = CreateDeploymentRequest {
        name: "Bad Name".into(),
        max_share: None,
    };
    assert!(matches!(
        svc.create_deployment(ACME, &id, None, bad_name)
            .await
            .unwrap_err(),
        ServiceError::Validation { .. }
    ));

    // Up to 10.
    for i in 2..10 {
        let req = CreateDeploymentRequest {
            name: format!("d{i}"),
            max_share: None,
        };
        svc.create_deployment(ACME, &id, None, req).await.unwrap();
    }
    let req = CreateDeploymentRequest {
        name: "d10".into(),
        max_share: None,
    };
    assert_eq!(
        conflict_code(
            svc.create_deployment(ACME, &id, None, req)
                .await
                .unwrap_err()
        ),
        "too_many_deployments"
    );

    // Update: set, then clear, the cap; renames must stay unique.
    let set = UpdateDeploymentRequest {
        name: None,
        max_share: Some(Some(0.5)),
    };
    let pt = svc
        .update_deployment(ACME, &id, &dep.id, None, set)
        .await
        .unwrap();
    assert_eq!(pt.deployments[1].max_share, Some(0.5));
    let clear = UpdateDeploymentRequest {
        name: None,
        max_share: Some(None),
    };
    let pt = svc
        .update_deployment(ACME, &id, &dep.id, None, clear)
        .await
        .unwrap();
    assert_eq!(pt.deployments[1].max_share, None);
    let clash = UpdateDeploymentRequest {
        name: Some("default".into()),
        max_share: None,
    };
    assert_eq!(
        conflict_code(
            svc.update_deployment(ACME, &id, &dep.id, None, clash)
                .await
                .unwrap_err()
        ),
        "name_taken"
    );
    let v = pt.version;
    let same = UpdateDeploymentRequest {
        name: Some("staging".into()),
        max_share: None,
    };
    assert_eq!(
        svc.update_deployment(ACME, &id, &dep.id, None, same)
            .await
            .unwrap()
            .version,
        v
    );

    // Delete down to one; the last can't go.
    let pt = svc.get(ACME, &id).await.unwrap();
    let ids: Vec<String> = pt.deployments.iter().map(|d| d.id.clone()).collect();
    for d in &ids[1..] {
        svc.delete_deployment(ACME, &id, d, None).await.unwrap();
    }
    assert_eq!(
        conflict_code(
            svc.delete_deployment(ACME, &id, &ids[0], None)
                .await
                .unwrap_err()
        ),
        "last_deployment"
    );
    assert_eq!(
        svc.delete_deployment(ACME, &id, "dep-missing", None)
            .await
            .unwrap_err(),
        ServiceError::NotFound
    );
    assert_eq!(
        svc.create_deployment(GLOBEX, &id, None, staging(None))
            .await
            .unwrap_err(),
        ServiceError::NotFound
    );
}

#[tokio::test]
async fn deployment_endpoints() {
    let svc = svc();
    let id = reservation(&svc).await;
    let (routes, _) = app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!(
        "http://{}/v1/provisioned-throughput/{id}",
        listener.local_addr().unwrap()
    );
    tokio::spawn(async move { axum::serve(listener, routes).await });
    let c = reqwest::Client::new();

    let r = c
        .post(format!("{base}/deployments"))
        .bearer_auth(ACME_KEY)
        .header("if-match", "\"1\"")
        .json(&json!({ "name": "staging", "max_share": 0.2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let dep: Value = r.json().await.unwrap();
    let dep_id = dep["id"].as_str().unwrap().to_string();
    assert!(dep["api_key"].as_str().unwrap().starts_with("ptk_"));
    assert_eq!(dep["max_share"], 0.2);

    let list: Value = c
        .get(format!("{base}/deployments"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["data"].as_array().unwrap().len(), 2);
    assert!(list["data"][1]["api_keys"][0].get("sha256").is_none());

    let r = c
        .patch(format!("{base}/deployments/{dep_id}"))
        .bearer_auth(ACME_KEY)
        .json(&json!({ "max_share": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.json::<Value>().await.unwrap().get("max_share").is_none(),
        "cap cleared"
    );

    let r = c
        .post(format!("{base}/deployments/{dep_id}/keys/rotate"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let keys: Value = c
        .get(format!("{base}/deployments/{dep_id}/keys"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(keys["deployment"], dep_id);
    assert_eq!(keys["data"].as_array().unwrap().len(), 2);

    // The primary deployment's keys are still at the old paths.
    let primary: Value = c
        .get(format!("{base}/keys"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(primary["deployment"], dep_id);
    assert_eq!(primary["data"].as_array().unwrap().len(), 1);

    let r = c
        .get(format!("{base}/deployments/dep-nope"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let r = c
        .delete(format!("{base}/deployments/{dep_id}"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.json::<Value>().await.unwrap()["deployments"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let r = c
        .post(format!("{base}/deployments"))
        .bearer_auth("sk-admin-globex-dev")
        .json(&json!({ "name": "x" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}
