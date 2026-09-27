//! The controller pauses and resumes sales through the control plane (ADR-041).

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
