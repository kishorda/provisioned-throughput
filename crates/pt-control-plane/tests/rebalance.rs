//! Moving multi-region splits toward demand (docs/07 §3, ADR-024).

mod common;

use std::sync::Arc;

use common::*;
use jiff::{SignedDuration, Timestamp};
use pt_control_plane::clock::{Clock, ManualClock};
use pt_control_plane::model::{DeclareIncident, EventKind, RegionShare, Sku, UpdateRequest};
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::store::MemoryStore;
use pt_control_plane::telemetry::CpTelemetry;
use pt_control_plane::{app, in_memory, Service};
use pt_core::{Outcome, RejectReason, Timings, TokenBreakdown, TrafficClass, UsageRecord};
use pt_telemetry::UsageStore;

type Svc = Arc<Service<MemoryStore, MemoryPlanner, ManualClock>>;
type Tel = Arc<CpTelemetry<MemoryStore, MemoryPlanner, ManualClock>>;

fn setup() -> (Svc, Tel) {
    let svc = in_memory(config(), ManualClock::new(t0()));
    let (_, tel) = app(svc.clone());
    (svc, tel)
}

fn ms(t: Timestamp) -> u64 {
    t.as_millisecond() as u64
}

/// `n` requests of 100 WU for `id` in the last 10 minutes; every fifth one throttled.
fn requests(id: &str, now: Timestamp, n: u64) -> Vec<UsageRecord> {
    (0..n)
        .map(|i| UsageRecord {
            request_id: uuid::Uuid::new_v4(),
            received_at_ms: ms(now - SignedDuration::from_mins(10)) + i,
            tenant: ACME.into(),
            reservation: id.into(),
            deployment: "dep".into(),
            class: Some(TrafficClass::Provisioned),
            session_id: None,
            tokens: TokenBreakdown::default(),
            kv_token_seconds: 0.0,
            wu_estimated: 100.0,
            wu_actual: if i % 5 == 0 { 0.0 } else { 100.0 },
            timings: Timings {
                queue_ms: 0.0,
                ttft_ms: Some(100.0),
                total_ms: 500.0,
                tpot_ms: None,
            },
            in_shape: true,
            outcome: if i % 5 == 0 {
                Outcome::Rejected(RejectReason::EntitlementExhausted)
            } else {
                Outcome::Ok
            },
            profile: "p".into(),
        })
        .collect()
}

/// Push traffic split `west`/`central` requests for `id`.
async fn traffic(svc: &Svc, tel: &Tel, id: &str, west: u64, central: u64) {
    let now = svc.clock.now();
    tel.store
        .append("eu-west", requests(id, now, west), ms(now))
        .await
        .unwrap();
    tel.store
        .append("eu-central", requests(id, now, central), ms(now))
        .await
        .unwrap();
}

fn split(v: &[(&str, u32)]) -> Vec<RegionShare> {
    shares(v)
}

async fn cus_in(svc: &Svc, region: &str, id: &str) -> u32 {
    svc.snapshot(region)
        .await
        .unwrap()
        .unwrap()
        .reservations
        .into_iter()
        .find(|r| r.id == id)
        .map_or(0, |r| r.cus)
}

fn available(svc: &Svc, region: &str) -> u32 {
    svc.planner.available(region, MAVERICK).unwrap()
}

#[tokio::test]
async fn split_follows_demand_and_returns() {
    let (svc, tel) = setup();
    let pt = svc
        .create(
            ACME,
            None,
            request("agents", &[("eu-west", 6), ("eu-central", 4)]),
        )
        .await
        .unwrap()
        .resource;
    let price = pt.price.clone();

    // 80% of demand arrives in eu-central (throttled requests included).
    traffic(&svc, &tel, &pt.id, 40, 160).await;
    let r = svc.run_rebalance(&tel.store).await.unwrap();
    // Ideally 2/8, but at most 20% of 10 CUs moves.
    let moved = split(&[("eu-west", 4), ("eu-central", 6)]);
    assert_eq!(r.moved, [(pt.id.clone(), moved.clone())]);

    let now = svc.get(ACME, &pt.id).await.unwrap();
    assert_eq!(now.effective_regions, moved);
    assert_eq!(now.regions, pt.regions, "the contract doesn't change");
    assert_eq!(now.price, price, "nor does the price");
    assert!(matches!(
        now.events.last().unwrap().kind,
        EventKind::SplitRebalanced { .. }
    ));
    // Gateways enforce the new split; the planner holds it.
    assert_eq!(cus_in(&svc, "eu-central", &pt.id).await, 6);
    assert_eq!(cus_in(&svc, "eu-west", &pt.id).await, 4);
    assert_eq!(available(&svc, "eu-west"), 200 - 4);
    assert_eq!(available(&svc, "eu-central"), 100 - 6);

    // Within the cooldown, nothing moves.
    svc.clock.advance(SignedDuration::from_mins(5));
    traffic(&svc, &tel, &pt.id, 120, 80).await;
    assert!(svc
        .run_rebalance(&tel.store)
        .await
        .unwrap()
        .moved
        .is_empty());

    // After it, balanced demand returns the split to the contract.
    svc.clock.advance(SignedDuration::from_mins(16));
    traffic(&svc, &tel, &pt.id, 120, 80).await;
    let r = svc.run_rebalance(&tel.store).await.unwrap();
    assert_eq!(r.moved, [(pt.id.clone(), pt.regions.clone())]);
    let now = svc.get(ACME, &pt.id).await.unwrap();
    assert!(now.effective_regions.is_empty());
    assert_eq!(available(&svc, "eu-west"), 200 - 6);
    assert_eq!(available(&svc, "eu-central"), 100 - 4);
}

#[tokio::test]
async fn guards() {
    let (svc, tel) = setup();
    let pt = svc
        .create(
            ACME,
            None,
            request("agents", &[("eu-west", 6), ("eu-central", 4)]),
        )
        .await
        .unwrap()
        .resource;

    // Too little traffic: no move.
    traffic(&svc, &tel, &pt.id, 5, 40).await;
    assert!(svc
        .run_rebalance(&tel.store)
        .await
        .unwrap()
        .moved
        .is_empty());

    // An incident in one of its regions freezes the split.
    traffic(&svc, &tel, &pt.id, 40, 160).await;
    let inc = svc
        .declare_incident(DeclareIncident {
            region: "eu-west".into(),
            started_at: None,
            description: "Region down".into(),
        })
        .await
        .unwrap();
    assert!(svc
        .run_rebalance(&tel.store)
        .await
        .unwrap()
        .moved
        .is_empty());
    svc.resolve_incident(&inc.id, Default::default())
        .await
        .unwrap();
    // ... through the return ramp too.
    svc.clock.advance(SignedDuration::from_mins(5));
    traffic(&svc, &tel, &pt.id, 40, 160).await;
    assert!(svc
        .run_rebalance(&tel.store)
        .await
        .unwrap()
        .moved
        .is_empty());
    svc.clock.advance(SignedDuration::from_mins(6));
    traffic(&svc, &tel, &pt.id, 40, 160).await;
    assert_eq!(svc.run_rebalance(&tel.store).await.unwrap().moved.len(), 1);

    // Opting out returns the contracted split at once, and keeps it.
    let off = svc
        .update(
            ACME,
            &pt.id,
            None,
            UpdateRequest {
                rebalance: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!off.rebalance);
    assert!(off.effective_regions.is_empty());
    assert_eq!(available(&svc, "eu-central"), 100 - 4);
    svc.clock.advance(SignedDuration::from_mins(20));
    traffic(&svc, &tel, &pt.id, 40, 160).await;
    assert!(svc
        .run_rebalance(&tel.store)
        .await
        .unwrap()
        .moved
        .is_empty());
}

#[tokio::test]
async fn a_cu_increase_resets_the_split() {
    let (svc, tel) = setup();
    let pt = svc
        .create(
            ACME,
            None,
            request("agents", &[("eu-west", 6), ("eu-central", 4)]),
        )
        .await
        .unwrap()
        .resource;
    traffic(&svc, &tel, &pt.id, 40, 160).await;
    svc.run_rebalance(&tel.store).await.unwrap();
    assert_eq!(available(&svc, "eu-west"), 196);
    // The new contract replaces the rebalanced split; the planner follows exactly.
    let up = svc
        .update(
            ACME,
            &pt.id,
            None,
            UpdateRequest {
                regions: Some(shares(&[("eu-west", 7), ("eu-central", 4)])),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(up.effective_regions.is_empty());
    assert_eq!(available(&svc, "eu-west"), 200 - 7);
    assert_eq!(available(&svc, "eu-central"), 100 - 4);
}

#[tokio::test]
async fn capacity_and_headroom_are_respected() {
    let (svc, tel) = setup();
    // Multi-region: headroom follows the effective split.
    let mut req = request("multi", &[("eu-west", 6), ("eu-central", 4)]);
    req.sku = Sku::MultiRegion;
    let multi = svc.create(ACME, None, req).await.unwrap().resource;
    let (west, central) = (available(&svc, "eu-west"), available(&svc, "eu-central"));
    traffic(&svc, &tel, &multi.id, 40, 160).await;
    svc.run_rebalance(&tel.store).await.unwrap();
    let now = svc.get(ACME, &multi.id).await.unwrap();
    assert_eq!(
        now.effective_regions,
        split(&[("eu-west", 4), ("eu-central", 6)])
    );
    assert_eq!(
        now.failover_headroom,
        split(&[("eu-central", 4), ("eu-west", 6)])
    );
    // 4 + 6 in each region, as before: the move needed no extra capacity.
    assert_eq!(
        (available(&svc, "eu-west"), available(&svc, "eu-central")),
        (west, central)
    );

    // A move that doesn't fit is skipped.
    let full = svc
        .create(
            GLOBEX,
            None,
            request("fill", &[("eu-central", central - 10)]),
        )
        .await
        .unwrap();
    let _ = full;
    let pt = svc
        .create(
            ACME,
            None,
            request("regional", &[("eu-west", 10), ("eu-central", 10)]),
        )
        .await
        .unwrap()
        .resource;
    // eu-central has 0 spare CUs now; moving 2 CUs there can't be reserved.
    assert_eq!(available(&svc, "eu-central"), 0);
    traffic(&svc, &tel, &pt.id, 20, 180).await;
    let r = svc.run_rebalance(&tel.store).await.unwrap();
    assert_eq!(r.no_capacity, std::slice::from_ref(&pt.id));
    assert!(svc
        .get(ACME, &pt.id)
        .await
        .unwrap()
        .effective_regions
        .is_empty());
    assert_eq!(available(&svc, "eu-west"), west - 10, "nothing leaked");
}
