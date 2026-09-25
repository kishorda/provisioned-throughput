//! Automatic region-failure handling: headroom, detection, failover entitlements, steering.

mod common;

use std::sync::Arc;

use common::*;
use jiff::SignedDuration;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::failover::Health;
use pt_control_plane::model::{
    DeclareIncident, Heartbeat, IncidentSource, ResolveIncident, Sku, UpdateRequest,
};
use pt_control_plane::planner::{MemoryPlanner, PlanError};
use pt_control_plane::service::ServiceError;
use pt_control_plane::store::MemoryStore;
use pt_control_plane::{app, in_memory, Service};
use serde_json::{json, Value};

const OPERATOR: &str = "sk-operator-dev";

type Svc = Arc<Service<MemoryStore, MemoryPlanner, ManualClock>>;

fn setup() -> (Svc, ManualClock) {
    let clock = ManualClock::new(t0());
    (in_memory(config(), clock.clone()), clock)
}

fn available(svc: &Svc, region: &str) -> u32 {
    svc.planner.available(region, MAVERICK).unwrap()
}

async fn beat(svc: &Svc, region: &str, gateway: &str, serving: bool) {
    svc.heartbeat(
        region,
        &Heartbeat {
            gateway_id: gateway.into(),
            serving,
            snapshot_version: 1,
            key_id: None,
        },
    )
    .await
    .unwrap();
}

async fn multi_region(svc: &Svc, name: &str, regions: &[(&str, u32)]) -> String {
    let mut req = request(name, regions);
    req.sku = Sku::MultiRegion;
    svc.create(ACME, None, req).await.unwrap().resource.id
}

#[tokio::test]
async fn multi_region_holds_failover_headroom_in_the_paired_region() {
    let (svc, _) = setup();
    let id = multi_region(&svc, "agents", &[("eu-west", 10), ("eu-central", 4)]).await;
    let pt = svc.get(ACME, &id).await.unwrap();
    assert_eq!(
        pt.failover_headroom,
        shares(&[("eu-central", 10), ("eu-west", 4)])
    );
    // Each region holds its own share plus the other's.
    assert_eq!(available(&svc, "eu-west"), 200 - 10 - 4);
    assert_eq!(available(&svc, "eu-central"), 100 - 4 - 10);
    // The price covers the shares only, with the Multi-region surcharge of 0.2 × base:
    // Agentic is 1.5 + 0.2 = 1.7 × 150,000 cents per CU.
    assert_eq!(pt.cus, 14);
    assert_eq!(pt.price.per_cu_monthly, 255_000);
    assert_eq!(pt.price.monthly, 14 * 255_000);

    // Growing a share grows the headroom it needs in its pair.
    let up = UpdateRequest {
        regions: Some(shares(&[("eu-west", 12), ("eu-central", 4)])),
        ..Default::default()
    };
    let pt = svc.update(ACME, &id, None, up).await.unwrap();
    assert_eq!(
        pt.failover_headroom,
        shares(&[("eu-central", 12), ("eu-west", 4)])
    );
    assert_eq!(available(&svc, "eu-west"), 200 - 12 - 4);
    assert_eq!(available(&svc, "eu-central"), 100 - 4 - 12);

    // Regional reservations hold no headroom.
    let regional = svc
        .create(ACME, None, request("regional", &[("eu-west", 5)]))
        .await
        .unwrap()
        .resource;
    assert!(regional.failover_headroom.is_empty());
    assert_eq!(regional.price.per_cu_monthly, 225_000, "no surcharge");
    assert_eq!(available(&svc, "eu-west"), 200 - 12 - 4 - 5);
}

#[tokio::test]
async fn headroom_must_fit_or_nothing_is_reserved() {
    let (svc, _) = setup();
    // eu-central has 100 CUs: 1 of its own plus 150 of headroom for eu-west won't fit.
    let mut req = request("big", &[("eu-west", 150), ("eu-central", 1)]);
    req.sku = Sku::MultiRegion;
    let err = svc.create(ACME, None, req).await.unwrap_err();
    assert!(
        matches!(
            err,
            ServiceError::Capacity(PlanError::CapacityUnavailable { ref region, .. }) if region == "eu-central"
        ),
        "{err:?}"
    );
    assert_eq!(available(&svc, "eu-west"), 200);
    assert_eq!(available(&svc, "eu-central"), 100);

    // Increases that would overflow the pair are refused too.
    let id = multi_region(&svc, "ok", &[("eu-west", 50), ("eu-central", 1)]).await;
    let up = UpdateRequest {
        regions: Some(shares(&[("eu-west", 120), ("eu-central", 1)])),
        ..Default::default()
    };
    assert!(svc.update(ACME, &id, None, up).await.is_err());
    assert_eq!(available(&svc, "eu-central"), 100 - 1 - 50);

    // Releasing gives back shares and headroom.
    let mut req = request("later", &[("eu-west", 3), ("eu-central", 2)]);
    req.sku = Sku::MultiRegion;
    req.start_at = Some(t0() + SignedDuration::from_hours(24));
    let later = svc.create(ACME, None, req).await.unwrap().resource.id;
    assert_eq!(available(&svc, "eu-west"), 200 - 50 - 1 - 3 - 2);
    svc.delete(ACME, &later, None).await.unwrap();
    assert_eq!(available(&svc, "eu-west"), 200 - 50 - 1);
    assert_eq!(available(&svc, "eu-central"), 100 - 1 - 50);
}

#[tokio::test]
async fn multi_region_must_stay_in_one_residency_zone() {
    let (svc, _) = setup();
    let mut req = request("split", &[("eu-west", 2), ("us-east", 2)]);
    req.sku = Sku::MultiRegion;
    match svc.create(ACME, None, req).await.unwrap_err() {
        ServiceError::Validation { field, .. } => assert_eq!(field, "regions"),
        other => panic!("{other:?}"),
    }
    // A Regional reservation may span zones: it never fails over.
    svc.create(
        ACME,
        None,
        request("split", &[("eu-west", 2), ("us-east", 2)]),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn silent_region_is_declared_down_then_recovers() {
    let (svc, clock) = setup();
    let id = multi_region(&svc, "agents", &[("eu-west", 6), ("eu-central", 4)]).await;
    let secs = |s| SignedDuration::from_secs(s);

    beat(&svc, "eu-west", "gw-a", true).await;
    beat(&svc, "eu-central", "gw-b", true).await;
    let last_west = clock_now(&clock);
    assert!(svc.run_failover().await.declared.is_empty());

    // eu-west goes quiet. Within the timeout nothing happens.
    for _ in 0..6 {
        clock.advance(secs(5));
        beat(&svc, "eu-central", "gw-b", true).await;
    }
    assert!(
        svc.run_failover().await.declared.is_empty(),
        "30 s is not over 30 s"
    );
    clock.advance(secs(1));
    beat(&svc, "eu-central", "gw-b", true).await;
    let before = svc.entitlement_version();
    let r = svc.run_failover().await;
    assert_eq!(r.declared.len(), 1, "{r:?}");
    assert_eq!(r.declared[0].1, "eu-west");
    assert!(svc.entitlement_version() > before, "snapshots change");
    let incident = svc.incidents().await.unwrap().pop().unwrap();
    assert_eq!(incident.source, IncidentSource::Automatic);
    assert_eq!(
        incident.started_at, last_west,
        "starts at the last serving heartbeat"
    );
    // Declared once only.
    assert!(svc.run_failover().await.declared.is_empty());

    // eu-central's snapshot carries the dormant entitlement and the active failure.
    let snap = svc.snapshot("eu-central").await.unwrap().unwrap();
    let r = snap.reservations.iter().find(|r| r.id == id).unwrap();
    assert_eq!(r.cus, 4);
    assert_eq!(r.failover.len(), 1);
    assert_eq!(r.failover[0].from_region, "eu-west");
    assert_eq!(r.failover[0].cus, 6);
    assert_eq!(snap.failovers.len(), 1);
    let now_ms = clock_now(&clock).as_millisecond() as u64;
    assert_eq!(snap.effective_cus(r, now_ms), 10.0);

    // A gateway that heartbeats but isn't serving doesn't count.
    beat(&svc, "eu-west", "gw-a", false).await;
    assert_eq!(status_of(&svc, "eu-west").await, Health::Down);

    // eu-west serves again, but must stay up for 60 s before the incident resolves.
    for _ in 0..12 {
        beat(&svc, "eu-west", "gw-a", true).await;
        beat(&svc, "eu-central", "gw-b", true).await;
        assert!(svc.run_failover().await.resolved.is_empty());
        clock.advance(secs(5));
    }
    beat(&svc, "eu-west", "gw-a", true).await;
    beat(&svc, "eu-central", "gw-b", true).await;
    let r = svc.run_failover().await;
    assert_eq!(r.resolved.len(), 1, "{r:?}");
    let resolved = svc.incidents().await.unwrap().pop().unwrap();
    assert_eq!(resolved.ended_at, Some(clock_now(&clock)));

    // The failover entitlement ramps down over 10 minutes, then leaves the snapshot.
    clock.advance(SignedDuration::from_mins(5));
    let snap = svc.snapshot("eu-central").await.unwrap().unwrap();
    let r = snap.reservations.iter().find(|r| r.id == id).unwrap();
    let now_ms = clock_now(&clock).as_millisecond() as u64;
    assert_eq!(snap.effective_cus(r, now_ms), 7.0, "half of 6 CUs left");
    clock.advance(SignedDuration::from_mins(5));
    assert!(svc
        .snapshot("eu-central")
        .await
        .unwrap()
        .unwrap()
        .failovers
        .is_empty());
}

#[tokio::test]
async fn detection_guards() {
    let (svc, clock) = setup();
    // us-east never reported: unknown, never declared.
    beat(&svc, "eu-west", "gw-a", true).await;
    beat(&svc, "eu-central", "gw-b", true).await;
    clock.advance(SignedDuration::from_mins(2));
    assert_eq!(status_of(&svc, "us-east").await, Health::Unknown);
    // Every region that ever reported is silent: the control plane is the one cut off.
    assert!(svc.run_failover().await.declared.is_empty());

    // The first region to report after a silence (for example, after a control-plane
    // outage) doesn't fail the others over at once: heartbeats must be flowing for a full
    // timeout first.
    beat(&svc, "eu-central", "gw-b", true).await;
    assert!(svc.run_failover().await.declared.is_empty());
    for _ in 0..6 {
        clock.advance(SignedDuration::from_secs(5));
        beat(&svc, "eu-central", "gw-b", true).await;
    }
    // eu-central has now served continuously for 30 s, and eu-west is still silent.
    let r = svc.run_failover().await;
    assert_eq!(r.declared.len(), 1);
    assert_eq!(r.declared[0].1, "eu-west");

    // Operator incidents are never resolved automatically.
    svc.declare_incident(DeclareIncident {
        region: "eu-central".into(),
        started_at: None,
        description: "Planned drain".into(),
    })
    .await
    .unwrap();
    for _ in 0..7 {
        beat(&svc, "eu-west", "gw-a", true).await;
        beat(&svc, "eu-central", "gw-b", true).await;
        clock.advance(SignedDuration::from_secs(10));
    }
    beat(&svc, "eu-west", "gw-a", true).await;
    beat(&svc, "eu-central", "gw-b", true).await;
    let r = svc.run_failover().await;
    assert_eq!(r.resolved.len(), 1);
    assert_eq!(r.resolved[0].1, "eu-west");
    let open: Vec<_> = svc
        .incidents()
        .await
        .unwrap()
        .into_iter()
        .filter(|i| i.ended_at.is_none())
        .collect();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].region, "eu-central");
    assert_eq!(open[0].source, IncidentSource::Operator);

    // Turned off, nothing is declared.
    let mut cfg = config();
    cfg.failover.auto_declare = false;
    let off = in_memory(cfg, clock.clone());
    beat(&off, "eu-west", "gw-a", true).await;
    beat(&off, "eu-central", "gw-b", true).await;
    clock.advance(SignedDuration::from_mins(2));
    beat(&off, "eu-central", "gw-b", true).await;
    assert!(off.run_failover().await.declared.is_empty());
}

#[tokio::test]
async fn heartbeat_and_steering_api() {
    let clock = ManualClock::new(t0());
    let svc = in_memory(config(), clock.clone());
    let (routes, _) = app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, routes).await });
    let http = reqwest::Client::new();

    let multi = multi_region(&svc, "multi", &[("eu-west", 6), ("eu-central", 2)]).await;
    let regional = svc
        .create(
            ACME,
            None,
            request("regional", &[("eu-west", 3), ("eu-central", 1)]),
        )
        .await
        .unwrap()
        .resource
        .id;

    let hb = |token: &str, body: Value| {
        http.post(format!("{base}/internal/v1/heartbeats"))
            .bearer_auth(token)
            .json(&body)
            .send()
    };
    let ok = json!({ "gateway_id": "gw-1", "serving": true });
    assert_eq!(hb("nope", ok.clone()).await.unwrap().status(), 401);
    assert_eq!(
        hb(
            "region-token-eu-west-dev",
            json!({ "gateway_id": "", "serving": true })
        )
        .await
        .unwrap()
        .status(),
        400
    );
    assert_eq!(
        hb("region-token-eu-west-dev", ok.clone())
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        hb("region-token-eu-central-dev", ok.clone())
            .await
            .unwrap()
            .status(),
        204
    );

    let get = |path: &str, key: &str| {
        let req = http.get(format!("{base}{path}")).bearer_auth(key);
        async move {
            let r = req.send().await.unwrap();
            (
                r.status().as_u16(),
                r.json::<Value>().await.unwrap_or(Value::Null),
            )
        }
    };
    assert_eq!(
        get("/internal/v1/steering", "sk-admin-acme-dev").await.0,
        401
    );
    let (code, regions) = get("/internal/v1/regions", OPERATOR).await;
    assert_eq!(code, 200);
    let health: Vec<_> = regions["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["region"].as_str().unwrap(), r["health"].as_str().unwrap()))
        .collect();
    assert_eq!(
        health,
        [
            ("eu-west", "serving"),
            ("eu-central", "serving"),
            ("us-east", "unknown")
        ]
    );

    let targets = |s: &Value, id: &str| -> Vec<(String, f64)> {
        s["reservations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == id)
            .unwrap()["targets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| {
                (
                    t["region"].as_str().unwrap().into(),
                    t["weight"].as_f64().unwrap(),
                )
            })
            .collect()
    };
    let (_, s) = get("/internal/v1/steering", OPERATOR).await;
    assert_eq!(
        targets(&s, &multi),
        [("eu-west".into(), 0.75), ("eu-central".into(), 0.25)]
    );

    // eu-west fails: the multi-region reservation moves wholly to eu-central. The regional
    // one keeps only its eu-central share.
    let inc = svc
        .declare_incident(DeclareIncident {
            region: "eu-west".into(),
            started_at: None,
            description: "Region down".into(),
        })
        .await
        .unwrap();
    let (_, s) = get("/internal/v1/steering", OPERATOR).await;
    assert_eq!(s["regions"][0]["weight"], 0.0);
    assert_eq!(s["regions"][0]["incident"], inc.id.as_str());
    assert_eq!(targets(&s, &multi), [("eu-central".into(), 1.0)]);
    assert_eq!(targets(&s, &regional), [("eu-central".into(), 1.0)]);
    assert!(s["reservations"][0]["targets"][0]["endpoint"]
        .as_str()
        .unwrap()
        .contains("eu-central"));

    // Recovery: eu-west's weight returns at 10% a minute.
    svc.resolve_incident(&inc.id, ResolveIncident::default())
        .await
        .unwrap();
    clock.advance(SignedDuration::from_mins(5));
    let (_, s) = get("/internal/v1/steering", OPERATOR).await;
    assert_eq!(s["regions"][0]["weight"], 0.5);
    // Half of eu-west's 6 CUs returned: 3 in eu-west, 2 + 3 in eu-central.
    assert_eq!(
        targets(&s, &multi),
        [("eu-west".into(), 0.375), ("eu-central".into(), 0.625)]
    );
    clock.advance(SignedDuration::from_mins(5));
    let (_, s) = get("/internal/v1/steering", OPERATOR).await;
    assert_eq!(s["regions"][0]["weight"], 1.0);
    assert!(s["regions"][0].get("incident").is_none());
}

fn clock_now(clock: &ManualClock) -> jiff::Timestamp {
    use pt_control_plane::clock::Clock;
    clock.now()
}

async fn status_of(svc: &Svc, region: &str) -> Health {
    svc.region_statuses()
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.region == region)
        .unwrap()
        .health
}
