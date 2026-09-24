//! `POST /v1/quotes` (docs/02 §5). Uses the tenant's management key.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};

use crate::api::{self, ApiError};
use crate::clock::Clock;
use crate::planner::CapacityPlanner;
use crate::quote::{self, QuoteRequest, QuoteResponse};
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
        .route("/v1/quotes", post(create_quote::<S, P, C>))
        .with_state(Arc::new(Ctx { svc, telemetry }))
}

async fn create_quote<S: Store, P: CapacityPlanner, C: Clock>(
    State(ctx): State<Arc<Ctx<S, P, C>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<QuoteResponse>, ApiError> {
    let tenant = api::tenant(&ctx.svc, &headers)?;
    let req: QuoteRequest = api::parse(&body)?;
    let resp = quote::quote(&ctx.svc, &ctx.telemetry.store, &tenant, req).await?;
    Ok(Json(resp))
}
