//! HTTP API.
//!
//! ```text
//! POST /v1/leases/renew   gateway token; RenewRequest → RenewResponse (503 not_leader on a standby)
//! GET  /v1/leases         gateway token; leadership and per-reservation grants, for operators
//! GET  /healthz           liveness
//! GET  /leader            200 while this replica leads, 503 on a standby. For monitoring:
//!                         don't use it as a readiness probe, or rollouts wait forever for
//!                         a standby to become ready
//! ```

use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::coordinator::Coordinator;
use crate::election::Leadership;
use crate::wire::RenewRequest;

#[derive(Clone)]
struct Ctx {
    coordinator: Arc<Coordinator>,
    leadership: Arc<Leadership>,
    token: Arc<str>,
}

/// Pass [`Leadership::always`] for a single coordinator without election.
pub fn router(coordinator: Arc<Coordinator>, leadership: Arc<Leadership>, token: &str) -> Router {
    Router::new()
        .route("/v1/leases/renew", post(renew))
        .route("/v1/leases", get(leases))
        .route("/healthz", get(|| async { "ok" }))
        .route("/leader", get(leader))
        .with_state(Ctx {
            coordinator,
            leadership,
            token: token.into(),
        })
}

async fn leader(State(ctx): State<Ctx>) -> Response {
    if ctx.leadership.serving(Instant::now()) {
        "leader".into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "standby").into_response()
    }
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({ "error": { "code": code, "message": message } })),
    )
        .into_response()
}

fn authorized(ctx: &Ctx, headers: &HeaderMap) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|t| t.trim() == &*ctx.token)
}

async fn renew(State(ctx): State<Ctx>, headers: HeaderMap, body: Bytes) -> Response {
    if !authorized(&ctx, &headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_token",
            "Invalid or missing gateway token.",
        );
    }
    let req: RenewRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_json",
                &e.to_string(),
            )
        }
    };
    if req.gateway_id.is_empty() {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
            "gateway_id is required.",
        );
    }
    // Checked after parsing, as close to granting as possible.
    let now = Instant::now();
    if !ctx.leadership.serving(now) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_leader",
            "This coordinator is a standby. Try another replica.",
        );
    }
    Json(ctx.coordinator.renew(&req, now)).into_response()
}

async fn leases(State(ctx): State<Ctx>, headers: HeaderMap) -> Response {
    if !authorized(&ctx, &headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_token",
            "Invalid or missing gateway token.",
        );
    }
    let now = Instant::now();
    Json(json!({
        "leader": ctx.leadership.serving(now),
        "warming_up": ctx.coordinator.warming_up(now),
        "data": ctx.coordinator.view(now),
    }))
    .into_response()
}
