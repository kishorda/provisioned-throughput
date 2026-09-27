//! Dashboards and the reports that feed them (ADR-043).
//!
//! ```text
//! POST /internal/v1/reports/pools              region token; a capacity controller's ModelPool
//! POST /internal/v1/reports/routers            region token; a router's status
//! GET  /internal/v1/dashboard/system           operator key; the system view
//! GET  /internal/v1/dashboard/customers        operator key; every customer
//! GET  /internal/v1/dashboard/usage/{tenant}   operator key; one customer's usage (?hours=)
//! GET  /v1/dashboard/usage                     tenant key; the caller's usage (?hours=)
//! GET  /internal/dashboard                     the system page (operators)
//! GET  /dashboard                              the usage page (customers)
//! ```

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use pt_entitlement::report::{PoolReport, RouterReport};
use serde::Deserialize;
use serde_json::json;

use crate::api::{self, ApiError};
use crate::clock::Clock;
use crate::dashboard;
use crate::planner::CapacityPlanner;
use crate::service::Service;
use crate::store::Store;
use crate::telemetry::CpTelemetry;

struct Ctx<S, P, C> {
    svc: Arc<Service<S, P, C>>,
    telemetry: Arc<CpTelemetry<S, P, C>>,
}

type St<S, P, C> = State<Arc<Ctx<S, P, C>>>;

const SYSTEM_HTML: &str = include_str!("../assets/dashboard/system.html");
const USAGE_HTML: &str = include_str!("../assets/dashboard/usage.html");
const APP_JS: &str = include_str!("../assets/dashboard/app.js");
const APP_CSS: &str = include_str!("../assets/dashboard/app.css");

pub fn router<S: Store, P: CapacityPlanner, C: Clock>(
    svc: Arc<Service<S, P, C>>,
    telemetry: Arc<CpTelemetry<S, P, C>>,
) -> Router {
    Router::new()
        .route("/internal/v1/reports/pools", post(pool_report::<S, P, C>))
        .route(
            "/internal/v1/reports/routers",
            post(router_report::<S, P, C>),
        )
        .route("/internal/v1/dashboard/system", get(system::<S, P, C>))
        .route(
            "/internal/v1/dashboard/customers",
            get(customers::<S, P, C>),
        )
        .route(
            "/internal/v1/dashboard/usage/{tenant}",
            get(operator_usage::<S, P, C>),
        )
        .route("/v1/dashboard/usage", get(own_usage::<S, P, C>))
        .route("/internal/dashboard", get(|| page(SYSTEM_HTML)))
        .route("/internal/dashboard/app.js", get(script))
        .route("/internal/dashboard/app.css", get(style))
        .route("/dashboard", get(|| page(USAGE_HTML)))
        .route("/dashboard/app.js", get(script))
        .route("/dashboard/app.css", get(style))
        .with_state(Arc::new(Ctx { svc, telemetry }))
}

async fn page(html: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        html,
    )
        .into_response()
}

async fn script() -> Response {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        APP_JS,
    )
        .into_response()
}

async fn style() -> Response {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], APP_CSS).into_response()
}

async fn pool_report<S: Store, P: CapacityPlanner, C: Clock>(
    State(ctx): St<S, P, C>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let region = api::region_of(&ctx.svc, &headers)?.to_string();
    let report: PoolReport = api::parse(&body)?;
    dashboard::record_pool(&ctx.svc, &region, &report).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn router_report<S: Store, P: CapacityPlanner, C: Clock>(
    State(ctx): St<S, P, C>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let region = api::region_of(&ctx.svc, &headers)?.to_string();
    let report: RouterReport = api::parse(&body)?;
    dashboard::record_router(&ctx.svc, &region, &report).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn system<S: Store, P: CapacityPlanner, C: Clock>(
    State(ctx): St<S, P, C>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    api::operator(&ctx.svc, &headers)?;
    Ok(Json(dashboard::system(&ctx.svc, &ctx.telemetry).await?).into_response())
}

async fn customers<S: Store, P: CapacityPlanner, C: Clock>(
    State(ctx): St<S, P, C>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    api::operator(&ctx.svc, &headers)?;
    Ok(Json(json!({ "data": dashboard::customers(&ctx.svc).await? })).into_response())
}

#[derive(Deserialize)]
struct Window {
    hours: Option<u64>,
}

async fn operator_usage<S: Store, P: CapacityPlanner, C: Clock>(
    State(ctx): St<S, P, C>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Query(w): Query<Window>,
) -> Result<Response, ApiError> {
    api::operator(&ctx.svc, &headers)?;
    if !ctx.svc.config.tenants.iter().any(|t| t.id == tenant) {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("No customer {tenant}."),
        ));
    }
    let view = dashboard::usage(&ctx.svc, &ctx.telemetry, &tenant, w.hours.unwrap_or(24)).await?;
    Ok(Json(view).into_response())
}

async fn own_usage<S: Store, P: CapacityPlanner, C: Clock>(
    State(ctx): St<S, P, C>,
    headers: HeaderMap,
    Query(w): Query<Window>,
) -> Result<Response, ApiError> {
    let tenant = api::tenant(&ctx.svc, &headers)?;
    let view = dashboard::usage(&ctx.svc, &ctx.telemetry, &tenant, w.hours.unwrap_or(24)).await?;
    Ok(Json(view).into_response())
}
