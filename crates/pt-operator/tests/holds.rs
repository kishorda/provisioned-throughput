//! The controller pauses and resumes sales through the control plane (ADR-041), and
//! reports its pools for the system dashboard (ADR-043).

#[path = "../../pt-control-plane/tests/common/mod.rs"]
mod common;

use common::*;
use pt_control_plane::api;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::in_memory;
use pt_control_plane::service::ServiceError;
use pt_operator::failover::SnapshotSource;
use pt_operator::holds::HoldClient;

#[tokio::test]
async fn an_expedited_drain_pauses_sales_until_it_ends() {
    let svc = in_memory(config(), ManualClock::new(t0()));
    let app = api::router(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let source = SnapshotSource {
        control_plane_url: url,
        region: "eu-west".into(),
        token: "region-token-eu-west-dev".into(),
        public_key: String::new(),
        cache_path: None,
        wait_secs: 1,
        tls: Default::default(),
    };
    let holds = HoldClient::new(&source).unwrap();
    holds
        .place(
            MAVERICK,
            "pt-serving/maverick-b200-a1",
            "expedited node drain",
        )
        .await
        .unwrap();
    let h = svc.sales_holds().await.unwrap();
    assert_eq!(h.len(), 1);
    assert_eq!(
        (h[0].region.as_str(), h[0].model.as_str()),
        ("eu-west", MAVERICK)
    );
    assert!(matches!(
        svc.create(ACME, None, request("paused", &[("eu-west", 1)]))
            .await,
        Err(ServiceError::Conflict {
            code: "sales_paused",
            ..
        })
    ));

    holds
        .lift(MAVERICK, "pt-serving/maverick-b200-a1")
        .await
        .unwrap();
    assert!(svc.sales_holds().await.unwrap().is_empty());
    svc.create(ACME, None, request("open", &[("eu-west", 1)]))
        .await
        .unwrap();

    // A model the region doesn't sell is refused.
    assert!(holds.place("qwen3-32b", "x", "r").await.is_err());
}

#[tokio::test]
async fn a_planned_pool_is_reported_to_the_dashboard() {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
    use pt_crds::{ModelPool, PerformanceProfile, PoolAllocation};
    use pt_entitlement::report::Roles;
    use pt_operator::plan::{plan, PoolInput};
    use pt_operator::report::pool_report;

    fn load<T: serde::de::DeserializeOwned>(file: &str) -> T {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/examples")
            .join(file);
        serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    let svc = in_memory(config(), ManualClock::new(t0()));
    let (app, telemetry) = pt_control_plane::app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let profile: PerformanceProfile = load("performanceprofile.yaml");
    let pool: ModelPool = load("modelpool.yaml");
    let alloc: PoolAllocation = load("poolallocation.yaml");
    let input = PoolInput {
        name: "maverick-b200-a1",
        namespace: "pt-serving",
        generation: Some(1),
        spec: &pool.spec,
        previous: None,
        owner: OwnerReference::default(),
        drain: Default::default(),
    };
    let p = plan(
        &input,
        Some(&profile.spec),
        &[alloc.spec],
        &[],
        "2026-09-23T00:00:00Z",
    );
    let ready = Roles {
        prefill: 6,
        decode: 20,
        total: 26,
        ..Default::default()
    };
    let report = pool_report(
        "pt-serving",
        "maverick-b200-a1",
        &pool.spec,
        &p.status,
        ready,
    );

    let source = SnapshotSource {
        control_plane_url: url,
        region: "eu-west".into(),
        token: "region-token-eu-west-dev".into(),
        public_key: String::new(),
        cache_path: None,
        wait_secs: 1,
        tls: Default::default(),
    };
    HoldClient::new(&source)
        .unwrap()
        .report_pool(&report)
        .await
        .unwrap();

    let view = pt_control_plane::dashboard::system(&svc, &telemetry)
        .await
        .unwrap();
    assert_eq!(view.pools.len(), 1);
    let got = &view.pools[0];
    assert_eq!(
        (got.region.as_str(), got.id.as_str()),
        ("eu-west", "pt-serving/maverick-b200-a1")
    );
    assert_eq!(got.report.desired.total, 29);
    assert_eq!(got.report.ready.total, 26);
    assert_eq!(
        got.report.engine,
        format!(
            "{} {}",
            pool.spec.engine.backend.as_str(),
            pool.spec.engine.version
        )
    );
    // 26 ready of 29 is above the 25 that must stay available: informational only.
    assert!(view
        .alerts
        .iter()
        .any(|a| a.scope.contains("maverick-b200-a1") && a.message == "26 of 29 replicas ready."));
}
