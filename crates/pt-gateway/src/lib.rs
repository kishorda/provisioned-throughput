//! Provisioned Throughput gateway (docs/03 §2.2, docs/04).

pub mod chat;
pub mod config;
pub mod quota;
pub mod sse;
pub mod state;
pub mod sync;
pub mod usage;

use std::time::Instant;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

pub use config::GatewayConfig;
pub use state::AppState;

pub fn router(app: AppState) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat::chat_completions))
        .route("/v1/pt/status", get(status))
        .route("/internal/v1/entitlements", get(entitlements))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

/// `GET /internal/v1/entitlements`: which entitlements this gateway is serving, and how
/// old they are. For operators and health checks; exposes no tenant data.
async fn entitlements(State(app): State<AppState>) -> Json<serde_json::Value> {
    let e = app.entitlements();
    let (source, region) = match &e.source {
        state::EntitlementSource::Static => ("static", None),
        state::EntitlementSource::Snapshot { region } => ("snapshot", Some(region.clone())),
        state::EntitlementSource::Pending { region } => ("pending", Some(region.clone())),
    };
    Json(json!({
        "source": source,
        "region": region,
        "version": e.version,
        "generated_at": e.generated_at,
        "applied_secs_ago": e.applied_at.elapsed().as_secs(),
        "reservations": e.reservation_count(),
        "deployments": e.deployment_count(),
    }))
}

/// `GET /v1/pt/status`: the caller's deployment and its current entitlement state.
async fn status(State(app): State<AppState>, headers: HeaderMap) -> Response {
    let Some(dep) = chat::bearer_token(&headers).and_then(|k| app.deployment_for_key(k)) else {
        return chat::api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "Invalid or missing API key.",
        );
    };
    let res = &dep.reservation;
    let now = Instant::now();
    let s = res.limiter.status(now);
    let (_, quota) = app.local_rate(&res.id, res.entitlement_wu_s, now);
    Json(json!({
        "deployment": dep.id,
        "reservation": res.id,
        "model": res.model,
        "tier": res.tier,
        "cus": res.cus,
        // The region's entitlement, and the share this gateway replica enforces.
        "entitlement_wu_per_s": res.entitlement_wu_s,
        "local_share_wu_per_s": s.entitlement_wu_s,
        "quota": quota.as_str(),
        "deployment_max_share": dep.max_share,
        "deployment_cap_wu_per_s": dep.cap.as_ref().map(|c| c.config().entitlement_wu_s),
        "bucket_wu": s.level_wu,
        "burst_credit_wu": s.burst_credit_wu,
        "queued_wu": s.queued_wu,
        "boundary_policy": res.limiter.policy(),
    }))
    .into_response()
}
