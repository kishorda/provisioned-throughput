//! HTTP API.
//!
//! ```text
//! POST /v1/leases/renew   gateway token; RenewRequest → RenewResponse
//! GET  /v1/leases         gateway token; per-reservation grants, for operators
//! GET  /healthz
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
use crate::wire::RenewRequest;

#[derive(Clone)]
struct Ctx {
    coordinator: Arc<Coordinator>,
    token: Arc<str>,
}

pub fn router(coordinator: Arc<Coordinator>, token: &str) -> Router {
    Router::new()
        .route("/v1/leases/renew", post(renew))
        .route("/v1/leases", get(leases))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(Ctx {
            coordinator,
            token: token.into(),
        })
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
    Json(ctx.coordinator.renew(&req, Instant::now())).into_response()
}

async fn leases(State(ctx): State<Ctx>, headers: HeaderMap) -> Response {
    if !authorized(&ctx, &headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_token",
            "Invalid or missing gateway token.",
        );
    }
    Json(json!({ "data": ctx.coordinator.view(Instant::now()) })).into_response()
}
