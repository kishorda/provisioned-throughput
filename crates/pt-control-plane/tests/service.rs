//! Business rules, driven through `Service` with a manual clock.

mod common;

use std::sync::Arc;

use common::*;
use jiff::SignedDuration;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::model::{EventKind, State, UpdateRequest};
use pt_control_plane::planner::{MemoryPlanner, PlanError};
use pt_control_plane::service::{sha256_hex, DeleteEffect, ServiceError};
use pt_control_plane::store::MemoryStore;
use pt_control_plane::{in_memory, Service};
use pt_core::{TermMonths, Tier};

type Svc = Arc<Service<MemoryStore, MemoryPlanner, ManualClock>>;

fn setup() -> (Svc, ManualClock) {
    let clock = ManualClock::new(t0());
    (in_memory(config(), clock.clone()), clock)
}

fn available(svc: &Svc, region: &str) -> u32 {
    svc.planner.available(region, MAVERICK).unwrap()
}

fn days(n: i64) -> SignedDuration {
    SignedDuration::from_hours(24 * n)
}

#[tokio::test]
async fn create_reserves_capacity_and_starts_the_term() {
    let (svc, _) = setup();
    let out = svc
        .create(
            ACME,
            None,
            request("agents", &[("eu-west", 10), ("us-east", 5)]),
        )
        .await
        .unwrap();
    let pt = &out.resource;

    assert_eq!(pt.state, State::Active);
    assert_eq!(pt.cus, 15);
    assert_eq!(pt.term_start, t0());
    assert_eq!(pt.term_end.to_string(), "2026-11-01T00:00:00Z");
    // Agentic is 1.5× the base.
    assert_eq!(pt.price.per_cu_monthly, BASE * 3 / 2);
    assert_eq!(pt.price.monthly, 15 * BASE * 3 / 2);
    assert_eq!(pt.endpoints[0].url, "https://eu-west.pt.example.com/v1");
    assert_eq!(available(&svc, "eu-west"), 190);
    assert_eq!(available(&svc, "us-east"), 295);

    let key = out.api_key.unwrap();
    assert!(key.starts_with("ptk_"));
    assert_eq!(pt.api_key_sha256, sha256_hex(key.as_bytes()));
}

#[tokio::test]
async fn failed_create_reserves_nothing() {
    let (svc, _) = setup();
    let err = svc
        .create(
            ACME,
            None,
            request("big", &[("eu-west", 150), ("us-east", 400)]),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ServiceError::Capacity(PlanError::CapacityUnavailable { requested: 400, .. })
    ));
    assert_eq!(available(&svc, "eu-west"), 200);
    assert!(svc.list(ACME, None, true).await.is_empty());
}

#[tokio::test]
async fn long_context_needs_a_region_that_serves_it() {
    let (svc, _) = setup();
    let mut req = request("long", &[("eu-central", 2)]);
    req.shape = shape(100_000);
    let err = svc.create(ACME, None, req).await.unwrap_err();
    assert!(matches!(
        err,
        ServiceError::Capacity(PlanError::ShapeUnsupported {
            max_context: 32_768,
            ..
        })
    ));
}

#[tokio::test]
async fn validation_rules() {
    let (svc, _) = setup();
    let field = |e: ServiceError| match e {
        ServiceError::Validation { field, .. } => field,
        other => panic!("expected validation error, got {other:?}"),
    };

    let mut r = request("q", &[("us-east", 1)]);
    r.model = "qwen3-32b".into(); // offered at interactive and standard only
    assert_eq!(field(svc.create(ACME, None, r).await.unwrap_err()), "tier");

    let mut r = request("m", &[("eu-west", 1)]);
    r.sku = pt_control_plane::model::Sku::MultiRegion;
    assert_eq!(field(svc.create(ACME, None, r).await.unwrap_err()), "sku");

    let r = request("zero", &[("eu-west", 0)]);
    assert_eq!(
        field(svc.create(ACME, None, r).await.unwrap_err()),
        "regions"
    );

    let r = request("nowhere", &[("ap-south", 1)]);
    assert_eq!(
        field(svc.create(ACME, None, r).await.unwrap_err()),
        "regions"
    );

    let mut r = request("unknown", &[("eu-west", 1)]);
    r.model = "gpt-9".into();
    assert_eq!(field(svc.create(ACME, None, r).await.unwrap_err()), "model");

    let mut r = request("past", &[("eu-west", 1)]);
    r.start_at = Some(t0() - days(1));
    assert_eq!(
        field(svc.create(ACME, None, r).await.unwrap_err()),
        "start_at"
    );

    assert_eq!(
        field(
            svc.create(ACME, None, request("Bad_Name", &[("eu-west", 1)]))
                .await
                .unwrap_err()
        ),
        "name"
    );
}

#[tokio::test]
async fn names_are_unique_per_tenant_among_live_reservations() {
    let (svc, _) = setup();
    svc.create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap();
    let err = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ServiceError::Conflict {
            code: "name_taken",
            ..
        }
    ));
    // Another tenant may use the same name.
    svc.create(GLOBEX, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap();
}

#[tokio::test]
async fn idempotent_create() {
    let (svc, _) = setup();
    let a = svc
        .create(ACME, Some("k1"), request("agents", &[("eu-west", 10)]))
        .await
        .unwrap();
    let b = svc
        .create(ACME, Some("k1"), request("agents", &[("eu-west", 10)]))
        .await
        .unwrap();
    assert!(b.replayed);
    assert!(b.api_key.is_none());
    assert_eq!(a.resource.id, b.resource.id);
    assert_eq!(available(&svc, "eu-west"), 190);

    let err = svc
        .create(ACME, Some("k1"), request("agents", &[("eu-west", 11)]))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ServiceError::Conflict {
            code: "idempotency_key_reused",
            ..
        }
    ));
}

#[tokio::test]
async fn increase_applies_now_with_a_prorated_charge() {
    let (svc, clock) = setup();
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 10)]))
        .await
        .unwrap()
        .resource;
    // The October term is 31 days; half of it remains after 15.5 days.
    clock.advance(SignedDuration::from_hours(24 * 15 + 12));

    let req = UpdateRequest {
        regions: Some(shares(&[("eu-west", 14)])),
        ..Default::default()
    };
    let pt = svc
        .update(ACME, &pt.id, Some(pt.version), req)
        .await
        .unwrap();
    assert_eq!(pt.cus, 14);
    assert_eq!(pt.version, 2);
    assert_eq!(available(&svc, "eu-west"), 186);
    let charge = pt
        .events
        .iter()
        .find_map(|e| match &e.kind {
            EventKind::CapacityIncreased {
                from: 10,
                to: 14,
                prorated_charge,
                ..
            } => Some(*prorated_charge),
            _ => None,
        })
        .unwrap();
    // 4 CUs × 225,000/month × 1 month × ½
    assert_eq!(charge, 450_000);
}

#[tokio::test]
async fn increase_without_capacity_changes_nothing() {
    let (svc, _) = setup();
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 150)]))
        .await
        .unwrap()
        .resource;
    let req = UpdateRequest {
        regions: Some(shares(&[("eu-west", 250)])),
        ..Default::default()
    };
    let err = svc.update(ACME, &pt.id, None, req).await.unwrap_err();
    assert!(matches!(
        err,
        ServiceError::Capacity(PlanError::CapacityUnavailable { .. })
    ));
    assert_eq!(svc.get(ACME, &pt.id).await.unwrap(), pt);
    assert_eq!(available(&svc, "eu-west"), 50);
}

#[tokio::test]
async fn decrease_and_region_change_wait_for_renewal() {
    let (svc, clock) = setup();
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 10)]))
        .await
        .unwrap()
        .resource;

    let req = UpdateRequest {
        regions: Some(shares(&[("eu-west", 4), ("us-east", 2)])),
        tier: Some(Tier::Standard),
        ..Default::default()
    };
    let pt = svc.update(ACME, &pt.id, None, req).await.unwrap();
    assert_eq!(pt.cus, 10, "current term is unchanged");
    assert_eq!(pt.tier, Tier::Agentic);
    let pending = pt.pending_changes.clone().unwrap();
    assert_eq!(pending.effective_at, pt.term_end);
    assert_eq!(pending.tier, Some(Tier::Standard));
    assert_eq!(available(&svc, "eu-west"), 190);

    clock.set(pt.term_end);
    let report = svc.run_lifecycle().await;
    assert_eq!(report.renewed, 1);
    let pt = svc.get(ACME, &pt.id).await.unwrap();
    assert_eq!(pt.state, State::Active);
    assert_eq!(pt.regions, shares(&[("eu-west", 4), ("us-east", 2)]));
    assert_eq!(pt.tier, Tier::Standard);
    assert_eq!(pt.price.monthly, 6 * BASE);
    assert_eq!(pt.term_end.to_string(), "2026-12-01T00:00:00Z");
    assert!(pt.pending_changes.is_none());
    assert_eq!(available(&svc, "eu-west"), 196);
    assert_eq!(available(&svc, "us-east"), 298);
}

#[tokio::test]
async fn scheduled_change_that_no_longer_fits_renews_unchanged() {
    let (svc, clock) = setup();
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 10)]))
        .await
        .unwrap()
        .resource;
    let req = UpdateRequest {
        regions: Some(shares(&[("us-east", 50)])),
        ..Default::default()
    };
    let pt = svc.update(ACME, &pt.id, None, req).await.unwrap();
    // Someone else takes all of us-east first.
    svc.create(GLOBEX, None, request("hog", &[("us-east", 300)]))
        .await
        .unwrap();

    clock.set(pt.term_end);
    svc.run_lifecycle().await;
    let pt = svc.get(ACME, &pt.id).await.unwrap();
    assert_eq!(pt.regions, shares(&[("eu-west", 10)]));
    assert!(pt
        .events
        .iter()
        .any(|e| matches!(e.kind, EventKind::ScheduledChangeFailed { .. })));
    assert_eq!(available(&svc, "eu-west"), 190);
}

#[tokio::test]
async fn delete_mid_term_ends_at_term_end() {
    let (svc, clock) = setup();
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 10)]))
        .await
        .unwrap()
        .resource;

    let (pt, effect) = svc.delete(ACME, &pt.id, None).await.unwrap();
    assert_eq!(effect, DeleteEffect::EndsAtTermEnd);
    assert_eq!(pt.state, State::PendingCancellation);
    assert!(!pt.auto_renew);
    assert_eq!(
        available(&svc, "eu-west"),
        190,
        "still serving until term end"
    );

    // No next term, so no scheduled changes. Increases still work.
    let sched = UpdateRequest {
        tier: Some(Tier::Standard),
        ..Default::default()
    };
    let err = svc.update(ACME, &pt.id, None, sched).await.unwrap_err();
    assert!(matches!(
        err,
        ServiceError::Conflict {
            code: "no_next_term",
            ..
        }
    ));

    // Deleting again is idempotent.
    assert_eq!(
        svc.delete(ACME, &pt.id, None).await.unwrap().1,
        DeleteEffect::EndsAtTermEnd
    );

    clock.set(pt.term_end);
    assert_eq!(svc.run_lifecycle().await.ended, 1);
    let pt = svc.get(ACME, &pt.id).await.unwrap();
    assert_eq!(pt.state, State::Ended);
    assert_eq!(available(&svc, "eu-west"), 200);
    assert!(svc.list(ACME, None, false).await.is_empty());
    assert_eq!(svc.list(ACME, None, true).await.len(), 1);

    assert_eq!(
        svc.delete(ACME, &pt.id, None).await.unwrap().1,
        DeleteEffect::AlreadyInactive
    );
    let err = svc
        .update(
            ACME,
            &pt.id,
            None,
            UpdateRequest {
                auto_renew: Some(true),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(
        err,
        Err(ServiceError::Conflict {
            code: "inactive",
            ..
        })
    ));
}

#[tokio::test]
async fn cancellation_can_be_withdrawn() {
    let (svc, clock) = setup();
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 10)]))
        .await
        .unwrap()
        .resource;
    svc.delete(ACME, &pt.id, None).await.unwrap();
    let pt = svc
        .update(
            ACME,
            &pt.id,
            None,
            UpdateRequest {
                auto_renew: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(pt.state, State::Active);

    clock.set(pt.term_end);
    assert_eq!(svc.run_lifecycle().await.renewed, 1);
    assert_eq!(svc.get(ACME, &pt.id).await.unwrap().state, State::Active);
}

#[tokio::test]
async fn before_the_term_starts_changes_apply_now_and_delete_cancels() {
    let (svc, clock) = setup();
    let mut req = request("later", &[("eu-west", 10)]);
    req.start_at = Some(t0() + days(7));
    req.term_months = TermMonths::Three;
    let pt = svc.create(ACME, None, req).await.unwrap().resource;
    assert_eq!(pt.state, State::Scheduled);
    assert_eq!(pt.term_end.to_string(), "2027-01-08T00:00:00Z");

    // A decrease applies immediately because no term has started.
    let upd = UpdateRequest {
        regions: Some(shares(&[("eu-west", 3)])),
        ..Default::default()
    };
    let pt = svc.update(ACME, &pt.id, None, upd).await.unwrap();
    assert_eq!(pt.cus, 3);
    assert!(pt.pending_changes.is_none());
    assert_eq!(available(&svc, "eu-west"), 197);

    let (pt, effect) = svc.delete(ACME, &pt.id, None).await.unwrap();
    assert_eq!(effect, DeleteEffect::CancelledNow);
    assert_eq!(pt.state, State::Cancelled);
    assert_eq!(available(&svc, "eu-west"), 200);

    // A second scheduled reservation activates when its start arrives.
    let mut req = request("later2", &[("eu-west", 1)]);
    req.start_at = Some(t0() + days(1));
    let pt = svc.create(ACME, None, req).await.unwrap().resource;
    clock.advance(days(1));
    assert_eq!(svc.run_lifecycle().await.activated, 1);
    assert_eq!(svc.get(ACME, &pt.id).await.unwrap().state, State::Active);
}

#[tokio::test]
async fn optimistic_concurrency_and_tenant_isolation() {
    let (svc, _) = setup();
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource;
    let err = svc
        .update(
            ACME,
            &pt.id,
            Some(pt.version + 1),
            UpdateRequest {
                auto_renew: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ServiceError::PreconditionFailed { .. }));
    assert_eq!(
        svc.get(GLOBEX, &pt.id).await.unwrap_err(),
        ServiceError::NotFound
    );
    assert_eq!(
        svc.delete(GLOBEX, &pt.id, None).await.unwrap_err(),
        ServiceError::NotFound
    );
}

#[tokio::test]
async fn no_op_update_keeps_the_version() {
    let (svc, _) = setup();
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource;
    let same = UpdateRequest {
        name: Some("agents".into()),
        regions: Some(shares(&[("eu-west", 1)])),
        auto_renew: Some(true),
        ..Default::default()
    };
    assert_eq!(
        svc.update(ACME, &pt.id, None, same).await.unwrap().version,
        1
    );
}

#[tokio::test]
async fn missed_renewals_catch_up() {
    let (svc, clock) = setup();
    let pt = svc
        .create(ACME, None, request("agents", &[("eu-west", 1)]))
        .await
        .unwrap()
        .resource;
    // The lifecycle loop was down for three months.
    clock.set("2027-01-15T00:00:00Z".parse().unwrap());
    assert_eq!(svc.run_lifecycle().await.renewed, 3);
    let pt = svc.get(ACME, &pt.id).await.unwrap();
    assert_eq!(pt.term_start.to_string(), "2027-01-01T00:00:00Z");
    assert_eq!(pt.term_end.to_string(), "2027-02-01T00:00:00Z");
}
