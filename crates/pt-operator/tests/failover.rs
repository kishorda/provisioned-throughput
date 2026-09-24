//! The controller follows the control plane's snapshot and turns failovers into demand.

use std::path::PathBuf;

use jiff::{SignedDuration, Timestamp};
use pt_control_plane::clock::ManualClock;
use pt_control_plane::model::{CreateRequest, DeclareIncident, RegionShare, Sku};
use pt_control_plane::{api, in_memory, ControlPlaneConfig};
use pt_core::{PoolIsolation, Shape, TermMonths, Tier};
use pt_crds::PoolAllocationSpec;
use pt_operator::failover::{failover_extra, SnapshotFollower, SnapshotSource};

fn share(region: &str, cus: u32) -> RegionShare {
    RegionShare {
        region: region.into(),
        cus,
    }
}

#[tokio::test]
async fn follower_turns_a_region_failure_into_pool_demand() {
    let config = ControlPlaneConfig::load(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/control-plane.toml"),
    )
    .unwrap();
    // Activation is judged by wall-clock time, so run the control plane's clock at now.
    let clock = ManualClock::new(Timestamp::now());
    let svc = in_memory(config, clock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cp = format!("http://{}", listener.local_addr().unwrap());
    let app = api::router(svc.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let id = svc
        .create(
            "acme",
            None,
            CreateRequest {
                name: "agents".into(),
                model: "llama-4-maverick".into(),
                tier: Tier::Agentic,
                regions: vec![share("eu-west", 6), share("eu-central", 2)],
                sku: Sku::MultiRegion,
                isolation: PoolIsolation::Shared,
                term_months: TermMonths::One,
                start_at: None,
                auto_renew: true,
                shape: Shape {
                    input_p95: 1_000,
                    input_max: 8_000,
                    output_p95: 100,
                    context_ceiling: 16_000,
                    cache_hit_ratio: 0.0,
                    burst_factor: 1.0,
                },
                boundary_policy: Default::default(),
            },
        )
        .await
        .unwrap()
        .resource
        .id;

    let cache = std::env::temp_dir().join(format!("pt-operator-{}.json", uuid_like()));
    let source = SnapshotSource {
        control_plane_url: cp.clone(),
        region: "eu-central".into(),
        token: "region-token-eu-central-dev".into(),
        public_key: svc.signer().public_key_hex(),
        cache_path: Some(cache.clone()),
        wait_secs: 1,
    };
    let (follower, rx) = SnapshotFollower::new(source.clone()).unwrap();
    assert!(follower.poll_once(false).await.unwrap());
    // eu-central's pool holds the reservation's 2 CUs (2,000 WU/s).
    let allocs = [PoolAllocationSpec {
        reservation: id.clone(),
        tenant: "acme".into(),
        pool: "maverick-h200".into(),
        wu_per_sec: 2_000.0,
        tier: Tier::Agentic,
        kv_share: 0.0,
        burst_factor: 1.0,
        dedicated_workers: vec![],
    }];
    let now_ms = || Timestamp::now().as_millisecond() as u64;
    let snap = rx.borrow().clone();
    assert_eq!(failover_extra(snap.as_deref(), &allocs, now_ms()), [0.0]);

    // eu-west fails: eu-central's pool must absorb its 6 CUs, three times its own share.
    svc.declare_incident(DeclareIncident {
        region: "eu-west".into(),
        started_at: Some(Timestamp::now() - SignedDuration::from_secs(5)),
        description: "Region down".into(),
    })
    .await
    .unwrap();
    assert!(follower.poll_once(true).await.unwrap());
    let snap = rx.borrow().clone();
    assert_eq!(
        failover_extra(snap.as_deref(), &allocs, now_ms()),
        [6_000.0]
    );
    assert!(!follower.poll_once(true).await.unwrap(), "nothing newer");

    // A restarted controller knows about the failover from its cache, with the control
    // plane unreachable.
    let offline = SnapshotSource {
        control_plane_url: "http://127.0.0.1:1".into(),
        ..source.clone()
    };
    let (restarted, rx2) = SnapshotFollower::new(offline).unwrap();
    assert!(restarted.load_cache());
    assert_eq!(
        failover_extra(rx2.borrow().clone().as_deref(), &allocs, now_ms()),
        [6_000.0]
    );
    assert!(restarted.poll_once(false).await.is_err());
    assert_eq!(rx2.borrow().as_ref().unwrap().failovers.len(), 1, "kept");

    // Snapshots signed by another key, or for another region, are refused.
    let (_, other_key) = pt_entitlement::SnapshotSigner::generate();
    let (wrong_key, _) = SnapshotFollower::new(SnapshotSource {
        public_key: other_key.clone(),
        cache_path: None,
        ..source.clone()
    })
    .unwrap();
    assert!(wrong_key.poll_once(false).await.is_err());
    // During a rotation, several comma-separated keys are trusted.
    let (both, _) = SnapshotFollower::new(SnapshotSource {
        public_key: format!("{other_key}, {}", source.public_key),
        cache_path: None,
        ..source.clone()
    })
    .unwrap();
    assert!(both.poll_once(false).await.unwrap());
    let (wrong_region, _) = SnapshotFollower::new(SnapshotSource {
        region: "eu-west".into(),
        cache_path: None,
        ..source
    })
    .unwrap();
    assert!(
        wrong_region.poll_once(false).await.is_err(),
        "token is for eu-central"
    );
    let _ = std::fs::remove_file(cache);
}

fn uuid_like() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
