//! REST API (docs/12 §2).
//!
//! ```text
//! GET    /v1/models
//! POST   /v1/provisioned-throughput            Idempotency-Key supported
//! GET    /v1/provisioned-throughput            ?model=&include_inactive=
//! GET    /v1/provisioned-throughput/{id}
//! PATCH  /v1/provisioned-throughput/{id}       If-Match supported
//! DELETE /v1/provisioned-throughput/{id}       If-Match supported
//! ```

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::clock::Clock;
use crate::model::ProvisionedThroughput;
use crate::planner::{CapacityPlanner, PlanError};
use crate::service::{DeleteEffect, Service, ServiceError};
use crate::store::Store;

type Svc<S, P, C> = Arc<Service<S, P, C>>;

pub fn router<S: Store, P: CapacityPlanner, C: Clock>(svc: Svc<S, P, C>) -> Router {
    Router::new()
        .route("/v1/models", get(list_models::<S, P, C>))
        .route(
            "/v1/provisioned-throughput",
            get(list::<S, P, C>).post(create::<S, P, C>),
        )
        .route(
            "/v1/provisioned-throughput/{id}",
            get(get_one::<S, P, C>)
                .patch(update::<S, P, C>)
                .delete(delete::<S, P, C>),
        )
        .route("/healthz", get(|| async { "ok" }))
        .with_state(svc)
}

/// An API error in the same shape the gateway uses.
pub struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
    field: Option<String>,
}

impl ApiError {
    fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            field: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let kind = match self.status {
            StatusCode::UNAUTHORIZED => "authentication_error",
            StatusCode::NOT_FOUND => "not_found_error",
            StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED => "conflict_error",
            s if s.is_server_error() => "server_error",
            _ => "invalid_request_error",
        };
        let mut err = json!({ "type": kind, "code": self.code, "message": self.message });
        if let Some(f) = self.field {
            err["field"] = json!(f);
        }
        (self.status, Json(json!({ "error": err }))).into_response()
    }
}

impl From<ServiceError> for ApiError {
    fn from(e: ServiceError) -> Self {
        match e {
            ServiceError::NotFound => ApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "No provisioned throughput with that id.",
            ),
            ServiceError::Validation { field, message } => ApiError {
                field: Some(field),
                ..ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "invalid_request", message)
            },
            ServiceError::Capacity(p @ PlanError::CapacityUnavailable { .. }) => {
                ApiError::new(StatusCode::CONFLICT, "capacity_unavailable", p.to_string())
            }
            ServiceError::Capacity(p @ PlanError::NotOffered { .. }) => ApiError {
                field: Some("regions".into()),
                ..ApiError::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "not_offered",
                    p.to_string(),
                )
            },
            ServiceError::Capacity(p @ PlanError::ShapeUnsupported { .. }) => ApiError {
                field: Some("shape".into()),
                ..ApiError::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "shape_unsupported",
                    p.to_string(),
                )
            },
            ServiceError::Conflict { code, message } => {
                ApiError::new(StatusCode::CONFLICT, code, message)
            }
            e @ ServiceError::PreconditionFailed { .. } => ApiError::new(
                StatusCode::PRECONDITION_FAILED,
                "version_mismatch",
                e.to_string(),
            ),
        }
    }
}

fn tenant<S, P, C>(svc: &Service<S, P, C>, headers: &HeaderMap) -> Result<String, ApiError> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .and_then(|k| svc.config.tenant_for_key(k.trim()))
        .map(str::to_owned)
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid or missing API key.",
            )
        })
}

/// Parse a JSON body with an error in the API's format rather than axum's plain text.
fn parse<T: DeserializeOwned>(body: &Bytes) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_json",
            format!("Invalid request body: {e}"),
        )
    })
}

/// `If-Match: "3"`, `W/"3"`, `3`, or `*`.
fn if_match(headers: &HeaderMap) -> Result<Option<u64>, ApiError> {
    let Some(raw) = headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) else {
        return Ok(None);
    };
    let v = raw.trim().trim_start_matches("W/").trim_matches('"');
    if v == "*" {
        return Ok(None);
    }
    v.parse().map(Some).map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_if_match",
            "If-Match must be an ETag returned by this API.",
        )
    })
}

fn etag(pt: &ProvisionedThroughput) -> (header::HeaderName, HeaderValue) {
    (
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", pt.version)).expect("valid etag"),
    )
}

async fn list_models<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    tenant(&svc, &headers)?;
    let data: Vec<Value> = svc
        .config
        .models
        .iter()
        .map(|m| {
            let regions: Vec<Value> = svc
                .config
                .capacity
                .iter()
                .filter(|c| c.model == m.id)
                .map(|c| json!({ "region": c.region, "max_context": c.max_context }))
                .collect();
            json!({
                "id": m.id,
                "display_name": m.display_name,
                "max_context": m.max_context,
                "tiers": m.tiers,
                "regions": regions,
            })
        })
        .collect();
    Ok(Json(json!({ "data": data })))
}

async fn create<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let req = parse(&body)?;
    let key = headers.get("idempotency-key").and_then(|v| v.to_str().ok());
    let out = svc.create(&tenant, key, req).await?;
    let mut body = serde_json::to_value(&out.resource).expect("resource serialises");
    if let Some(k) = out.api_key {
        body["api_key"] = json!(k);
    }
    let status = if out.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    let location = format!("/v1/provisioned-throughput/{}", out.resource.id);
    Ok((
        status,
        [
            etag(&out.resource),
            (
                header::LOCATION,
                HeaderValue::from_str(&location).expect("valid location"),
            ),
        ],
        Json(body),
    )
        .into_response())
}

#[derive(Deserialize)]
struct ListQuery {
    model: Option<String>,
    #[serde(default)]
    include_inactive: bool,
}

async fn list<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let data = svc
        .list(&tenant, q.model.as_deref(), q.include_inactive)
        .await;
    Ok(Json(json!({ "data": data })))
}

async fn get_one<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let pt = svc.get(&tenant, &id).await?;
    Ok(([etag(&pt)], Json(pt)).into_response())
}

async fn update<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let req = parse(&body)?;
    let pt = svc.update(&tenant, &id, if_match(&headers)?, req).await?;
    Ok(([etag(&pt)], Json(pt)).into_response())
}

async fn delete<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let (pt, effect) = svc.delete(&tenant, &id, if_match(&headers)?).await?;
    // 202: the deletion takes effect at term_end. 200: it's already done.
    let status = match effect {
        DeleteEffect::EndsAtTermEnd => StatusCode::ACCEPTED,
        DeleteEffect::CancelledNow | DeleteEffect::AlreadyInactive => StatusCode::OK,
    };
    Ok((status, [etag(&pt)], Json(pt)).into_response())
}
