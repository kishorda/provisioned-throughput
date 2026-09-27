//! The customer API and region traffic on separate listeners (ADR-034).

mod common;

use common::*;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::{app, in_memory, restrict, Surface};

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

async fn status(url: &str, path: &str) -> u16 {
    reqwest::get(format!("{url}{path}"))
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[tokio::test]
async fn each_listener_serves_only_its_surface() {
    let svc = in_memory(config(), ManualClock::new(t0()));
    let (routes, _) = app(svc);
    let customer = serve(restrict(routes.clone(), Surface::Customer)).await;
    let internal = serve(restrict(routes.clone(), Surface::Internal)).await;
    let both = serve(restrict(routes, Surface::All)).await;

    // Without credentials, a served path answers 401 and one that isn't served answers 404.
    assert_eq!(status(&customer, "/v1/models").await, 401);
    assert_eq!(status(&internal, "/v1/models").await, 404);
    assert_eq!(status(&customer, "/internal/v1/regions").await, 404);
    assert_eq!(status(&internal, "/internal/v1/regions").await, 401);
    // One listener serves both, as before.
    assert_eq!(status(&both, "/v1/models").await, 401);
    assert_eq!(status(&both, "/internal/v1/regions").await, 401);
    for url in [&customer, &internal, &both] {
        assert_eq!(status(url, "/healthz").await, 200);
    }
}

#[test]
fn surfaces_split_on_the_internal_prefix() {
    assert!(Surface::Customer.serves("/v1/provisioned-throughput/pt-1/usage"));
    assert!(!Surface::Customer.serves("/internal/v1/entitlements/eu-west"));
    assert!(Surface::Internal.serves("/internal/v1/usage"));
    assert!(!Surface::Internal.serves("/v1/quotes"));
    assert!(!Surface::Internal.serves("/internalish"));
}

#[test]
fn the_internal_listener_needs_its_own_address() {
    let mut c = config();
    c.server.internal = Some(pt_control_plane::config::InternalListener {
        listen: c.server.listen.clone(),
        tls: None,
    });
    assert!(c
        .validate()
        .unwrap_err()
        .to_string()
        .contains("must differ"));
}
