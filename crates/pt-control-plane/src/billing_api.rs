//! Invoices (docs/12 §7, ADR-018). Uses the tenant's management key.
//!
//! ```text
//! GET /v1/invoices              final invoices, newest first, plus current drafts
//! GET /v1/invoices/{period}     one month, for example 2026-10: final, or a draft
//! ```

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::api::{self, ApiError};
use crate::billing::{self, Invoice};
use crate::clock::Clock;
use crate::planner::CapacityPlanner;
use crate::service::Service;
use crate::store::Store;
use crate::telemetry::CpTelemetry;

struct Ctx<S, P, C> {
    svc: Arc<Service<S, P, C>>,
    telemetry: Arc<CpTelemetry<S, P, C>>,
}

pub fn router<S: Store, P: CapacityPlanner, C: Clock>(
    svc: Arc<Service<S, P, C>>,
    telemetry: Arc<CpTelemetry<S, P, C>>,
) -> Router {
    Router::new()
        .route("/v1/invoices", get(list::<S, P, C>))
        .route("/v1/invoices/{period}", get(one::<S, P, C>))
        .with_state(Arc::new(Ctx { svc, telemetry }))
}

async fn list<S: Store, P: CapacityPlanner, C: Clock>(
    State(ctx): State<Arc<Ctx<S, P, C>>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let tenant = api::tenant(&ctx.svc, &headers)?;
    let invoices = billing::list(&ctx.svc, &ctx.telemetry, &tenant).await?;
    Ok(Json(json!({ "data": invoices })))
}

async fn one<S: Store, P: CapacityPlanner, C: Clock>(
    State(ctx): State<Arc<Ctx<S, P, C>>>,
    headers: HeaderMap,
    Path(period): Path<String>,
) -> Result<Json<Invoice>, ApiError> {
    let tenant = api::tenant(&ctx.svc, &headers)?;
    Ok(Json(
        billing::invoice(&ctx.svc, &ctx.telemetry, &tenant, &period).await?,
    ))
}
