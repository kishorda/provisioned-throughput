//! The router reports its status for the system dashboard (ADR-043).

#[path = "../../pt-control-plane/tests/common/mod.rs"]
mod common;

use common::*;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::in_memory;
use pt_router::config::{ReportConfig, RouterConfig, WorkerConfig};
use pt_router::http::Shared;
use pt_router::report::Reporter;

fn router_config() -> RouterConfig {
    let c: RouterConfig = toml::from_str(
        r#"
        listen = "127.0.0.1:0"
        [[workers]]
        id = "w0"
        url = "http://127.0.0.1:1"
        slots = 4
        kv_blocks = 100
        "#,
    )
    .unwrap();
    assert!(matches!(c.workers[..], [WorkerConfig { .. }]));
    c
}

#[tokio::test]
async fn router_status_reaches_the_system_view() {
    let svc = in_memory(config(), ManualClock::new(t0()));
    let (app, telemetry) = pt_control_plane::app(svc.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let shared = Shared::new(&router_config());
    let report = |token: &str| ReportConfig {
        control_plane_url: url.clone(),
        token: token.into(),
        router_id: "router-0".into(),
        interval_secs: 15,
        tls: Default::default(),
    };
    let wrong = Reporter::new(&report("nope")).unwrap();
    assert!(wrong.send(&shared).await.unwrap_err().starts_with("401"));
    Reporter::new(&report("region-token-eu-west-dev"))
        .unwrap()
        .send(&shared)
        .await
        .unwrap();

    let view = pt_control_plane::dashboard::system(&svc, &telemetry)
        .await
        .unwrap();
    assert_eq!(view.routers.len(), 1);
    let r = &view.routers[0];
    assert_eq!((r.region.as_str(), r.id.as_str()), ("eu-west", "router-0"));
    assert!(!r.stale);
    assert_eq!(r.report["workers"][0]["id"], "w0", "{}", r.report);
}
