//! REST API (docs/12 §2).
//!
//! ```text
//! GET    /v1/models
//! POST   /v1/provisioned-throughput            Idempotency-Key supported
//! GET    /v1/provisioned-throughput            ?model=&include_inactive=
//! GET    /v1/provisioned-throughput/{id}
//! PATCH  /v1/provisioned-throughput/{id}       If-Match supported
//! DELETE /v1/provisioned-throughput/{id}       If-Match supported
//!
//! GET    /internal/v1/entitlements/{region}    region token; If-None-Match + ?wait= long-poll
//! POST   /internal/v1/heartbeats               region token; gateway liveness (docs/07 §4)
//! GET    /internal/v1/regions                  operator key; region health
//! GET    /internal/v1/steering                 operator key; DNS weights per region and reservation
//! ```

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::clock::Clock;
use crate::model::{Deployment, ProvisionedThroughput};
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
        .route(
            "/internal/v1/entitlements/{region}",
            get(entitlements::<S, P, C>),
        )
        // Keys of the primary deployment (kept for clients of the single-deployment API).
        .route(
            "/v1/provisioned-throughput/{id}/keys",
            get(list_keys::<S, P, C>),
        )
        .route(
            "/v1/provisioned-throughput/{id}/keys/rotate",
            post(rotate_key::<S, P, C>),
        )
        .route(
            "/v1/provisioned-throughput/{id}/keys/{key_id}",
            axum::routing::delete(revoke_key::<S, P, C>),
        )
        // Deployments.
        .route(
            "/v1/provisioned-throughput/{id}/deployments",
            get(list_deployments::<S, P, C>).post(create_deployment::<S, P, C>),
        )
        .route(
            "/v1/provisioned-throughput/{id}/deployments/{deployment}",
            get(get_deployment::<S, P, C>)
                .patch(update_deployment::<S, P, C>)
                .delete(delete_deployment::<S, P, C>),
        )
        .route(
            "/v1/provisioned-throughput/{id}/deployments/{deployment}/keys",
            get(list_deployment_keys::<S, P, C>),
        )
        .route(
            "/v1/provisioned-throughput/{id}/deployments/{deployment}/keys/rotate",
            post(rotate_deployment_key::<S, P, C>),
        )
        .route(
            "/v1/provisioned-throughput/{id}/deployments/{deployment}/keys/{key_id}",
            axum::routing::delete(revoke_deployment_key::<S, P, C>),
        )
        .route(
            "/internal/v1/incidents",
            get(list_incidents::<S, P, C>).post(declare_incident::<S, P, C>),
        )
        .route(
            "/internal/v1/incidents/{id}/resolve",
            post(resolve_incident::<S, P, C>),
        )
        .route("/internal/v1/heartbeats", post(heartbeat::<S, P, C>))
        .route("/internal/v1/regions", get(regions::<S, P, C>))
        .route("/internal/v1/steering", get(steering::<S, P, C>))
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

pub(crate) fn tenant<S, P, C>(
    svc: &Service<S, P, C>,
    headers: &HeaderMap,
) -> Result<String, ApiError> {
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
pub(crate) fn parse<T: DeserializeOwned>(body: &Bytes) -> Result<T, ApiError> {
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

fn find_deployment<'a>(
    pt: &'a ProvisionedThroughput,
    id: Option<&str>,
) -> Result<&'a Deployment, ApiError> {
    match id {
        None => Ok(pt.primary()),
        Some(d) => pt.deployments.iter().find(|x| x.id == d).ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "No deployment with that id.",
            )
        }),
    }
}

async fn keys_of<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    headers: &HeaderMap,
    id: &str,
    deployment: Option<&str>,
) -> Result<Response, ApiError> {
    let tenant = tenant(svc, headers)?;
    let pt = svc.get(&tenant, id).await?;
    let d = find_deployment(&pt, deployment)?;
    Ok((
        [etag(&pt)],
        Json(json!({ "deployment": d.id, "data": d.api_keys })),
    )
        .into_response())
}

async fn rotate<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    headers: &HeaderMap,
    id: &str,
    deployment: Option<&str>,
    body: &Bytes,
) -> Result<Response, ApiError> {
    let tenant = tenant(svc, headers)?;
    let req = if body.is_empty() {
        Default::default()
    } else {
        parse(body)?
    };
    let (pt, secret) = svc
        .rotate_key(&tenant, id, deployment, if_match(headers)?, req)
        .await?;
    let mut out = serde_json::to_value(&pt).expect("resource serialises");
    out["api_key"] = json!(secret);
    Ok(([etag(&pt)], Json(out)).into_response())
}

async fn revoke<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    headers: &HeaderMap,
    id: &str,
    deployment: Option<&str>,
    key_id: &str,
) -> Result<Response, ApiError> {
    let tenant = tenant(svc, headers)?;
    let pt = svc
        .revoke_key(&tenant, id, deployment, key_id, if_match(headers)?)
        .await?;
    Ok(([etag(&pt)], Json(pt)).into_response())
}

/// `GET /v1/provisioned-throughput/{id}/keys`: the primary deployment's key metadata.
async fn list_keys<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    keys_of(&svc, &headers, &id, None).await
}

/// `POST /v1/provisioned-throughput/{id}/keys/rotate`: the primary deployment.
async fn rotate_key<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    rotate(&svc, &headers, &id, None, &body).await
}

/// `DELETE /v1/provisioned-throughput/{id}/keys/{key_id}`: the primary deployment.
async fn revoke_key<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path((id, key_id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    revoke(&svc, &headers, &id, None, &key_id).await
}

async fn list_deployment_keys<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path((id, deployment)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    keys_of(&svc, &headers, &id, Some(&deployment)).await
}

async fn rotate_deployment_key<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path((id, deployment)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    rotate(&svc, &headers, &id, Some(&deployment), &body).await
}

async fn revoke_deployment_key<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path((id, deployment, key_id)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    revoke(&svc, &headers, &id, Some(&deployment), &key_id).await
}

/// `GET /v1/provisioned-throughput/{id}/deployments`
async fn list_deployments<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let pt = svc.get(&tenant, &id).await?;
    Ok(([etag(&pt)], Json(json!({ "data": pt.deployments }))).into_response())
}

/// `POST /v1/provisioned-throughput/{id}/deployments`: returns the deployment and its key,
/// once.
async fn create_deployment<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let req = parse(&body)?;
    let (pt, deployment, secret) = svc
        .create_deployment(&tenant, &id, if_match(&headers)?, req)
        .await?;
    let mut out = serde_json::to_value(&deployment).expect("deployment serialises");
    out["api_key"] = json!(secret);
    let location = format!(
        "/v1/provisioned-throughput/{id}/deployments/{}",
        deployment.id
    );
    Ok((
        StatusCode::CREATED,
        [
            etag(&pt),
            (
                header::LOCATION,
                HeaderValue::from_str(&location).expect("valid location"),
            ),
        ],
        Json(out),
    )
        .into_response())
}

/// `GET /v1/provisioned-throughput/{id}/deployments/{deployment}`
async fn get_deployment<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path((id, deployment)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let pt = svc.get(&tenant, &id).await?;
    let d = find_deployment(&pt, Some(&deployment))?;
    Ok(([etag(&pt)], Json(d)).into_response())
}

/// `PATCH /v1/provisioned-throughput/{id}/deployments/{deployment}`
async fn update_deployment<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path((id, deployment)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let req = parse(&body)?;
    let pt = svc
        .update_deployment(&tenant, &id, &deployment, if_match(&headers)?, req)
        .await?;
    let d = find_deployment(&pt, Some(&deployment))?;
    Ok(([etag(&pt)], Json(d)).into_response())
}

/// `DELETE /v1/provisioned-throughput/{id}/deployments/{deployment}`
async fn delete_deployment<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path((id, deployment)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let tenant = tenant(&svc, &headers)?;
    let pt = svc
        .delete_deployment(&tenant, &id, &deployment, if_match(&headers)?)
        .await?;
    Ok(([etag(&pt)], Json(pt)).into_response())
}

fn operator<S, P, C>(svc: &Service<S, P, C>, headers: &HeaderMap) -> Result<(), ApiError> {
    let expected = svc.config.operators.as_ref().map(|o| o.api_key.as_str());
    let given = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    match (expected, given) {
        (Some(e), Some(g)) if e == g => Ok(()),
        _ => Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid_operator_key",
            "Invalid or missing operator key.",
        )),
    }
}

/// `POST /internal/v1/incidents`: declare a region incident (operators only).
async fn declare_incident<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    operator(&svc, &headers)?;
    let incident = svc.declare_incident(parse(&body)?).await?;
    Ok((StatusCode::CREATED, Json(incident)).into_response())
}

/// `POST /internal/v1/incidents/{id}/resolve`
async fn resolve_incident<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    operator(&svc, &headers)?;
    let req = if body.is_empty() {
        Default::default()
    } else {
        parse(&body)?
    };
    Ok(Json(svc.resolve_incident(&id, req).await?).into_response())
}

/// `GET /internal/v1/incidents`
async fn list_incidents<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    operator(&svc, &headers)?;
    Ok(Json(json!({ "data": svc.incidents().await })).into_response())
}

/// `POST /internal/v1/heartbeats`: a gateway reports that it's alive and whether it serves.
async fn heartbeat<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let region = region_of(&svc, &headers)?.to_string();
    let hb: crate::model::Heartbeat = parse(&body)?;
    if hb.gateway_id.is_empty() || hb.gateway_id.len() > 128 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "gateway_id must be 1–128 characters.",
        ));
    }
    svc.heartbeat(&region, &hb);
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `GET /internal/v1/regions`: health of every region, from heartbeats.
async fn regions<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    operator(&svc, &headers)?;
    Ok(Json(json!({ "data": svc.region_statuses() })).into_response())
}

/// `GET /internal/v1/steering`: DNS weights for a GeoDNS or global load-balancer controller.
async fn steering<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    operator(&svc, &headers)?;
    Ok(Json(svc.steering().await).into_response())
}

/// The region a bearer region token belongs to.
fn region_of<'a, S, P, C>(
    svc: &'a Service<S, P, C>,
    headers: &HeaderMap,
) -> Result<&'a str, ApiError> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .and_then(|t| svc.config.region_for_token(t.trim()))
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "invalid_region_token",
                "Invalid or missing region token.",
            )
        })
}

/// Longest a snapshot long-poll may wait.
const MAX_WAIT_SECS: u64 = 60;

#[derive(Deserialize)]
struct WaitQuery {
    #[serde(default)]
    wait: u64,
}

/// `GET /internal/v1/entitlements/{region}`: the signed snapshot for a region (ADR-007).
///
/// With `If-None-Match: "<version>"` and `?wait=<secs>`, the request waits until the
/// entitlements change or the wait ends, then returns 304 if nothing changed.
async fn entitlements<S: Store, P: CapacityPlanner, C: Clock>(
    State(svc): State<Svc<S, P, C>>,
    headers: HeaderMap,
    Path(region): Path<String>,
    Query(q): Query<WaitQuery>,
) -> Result<Response, ApiError> {
    let token_region = region_of(&svc, &headers)?;
    if token_region != region {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "wrong_region",
            format!("This token is for {token_region}, not {region}."),
        ));
    }

    let known = if_none_match(&headers);
    let mut rx = svc.subscribe();
    let current = *rx.borrow_and_update();
    if known == Some(current) {
        let wait = Duration::from_secs(q.wait.min(MAX_WAIT_SECS));
        if !wait.is_zero() {
            let _ = tokio::time::timeout(wait, rx.changed()).await;
        }
        let now = *rx.borrow();
        if known == Some(now) {
            let tag = HeaderValue::from_str(&format!("\"{now}\"")).expect("valid etag");
            return Ok((StatusCode::NOT_MODIFIED, [(header::ETAG, tag)]).into_response());
        }
    }

    let snapshot = svc.snapshot(&region).await.ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "unknown_region",
            format!("Unknown region {region}."),
        )
    })?;
    let (body, signature) = svc.signer().sign(&snapshot);
    let tag = HeaderValue::from_str(&format!("\"{}\"", snapshot.version)).expect("valid etag");
    Ok((
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (header::ETAG, tag),
            (
                header::HeaderName::from_static(pt_entitlement::SIGNATURE_HEADER),
                HeaderValue::from_str(&signature).expect("hex signature"),
            ),
        ],
        body,
    )
        .into_response())
}

fn if_none_match(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.trim()
                .trim_start_matches("W/")
                .trim_matches('"')
                .parse()
                .ok()
        })
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
