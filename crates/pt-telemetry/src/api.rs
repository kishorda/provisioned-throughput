//! HTTP API (docs/09 §3–4).
//!
//! ```text
//! POST /internal/v1/usage                                    region token; {"records": [...]}
//! GET  /v1/provisioned-throughput/{id}/usage                 ?from=&to=&granularity=1m|5m|1h|1d
//! GET  /v1/provisioned-throughput/{id}/sla                   ?month=YYYY-MM
//! GET  /v1/provisioned-throughput/{id}/sessions/{session}    ?from=&to=
//! ```
//!
//! Customer routes use the tenant's management key. A reservation that belongs to another
//! tenant is reported as not found.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use pt_core::UsageRecord;
use serde::Deserialize;
use serde_json::json;

use crate::directory::{Directory, ReservationInfo};
use crate::store::UsageStore;
use crate::usage::{Granularity, MAX_BUCKETS};
use crate::{sessions, sla, usage, Telemetry};

/// Largest ingest batch.
pub const MAX_BATCH: usize = 5_000;
/// Longest query range.
const MAX_RANGE_MS: u64 = 35 * 86_400_000;
const DAY_MS: u64 = 86_400_000;

type Tel<U, D> = Arc<Telemetry<U, D>>;

pub fn router<U: UsageStore, D: Directory>(tel: Tel<U, D>) -> Router {
    Router::new()
        .route("/internal/v1/usage", post(ingest::<U, D>))
        .route(
            "/v1/provisioned-throughput/{id}/usage",
            get(usage_report::<U, D>),
        )
        .route(
            "/v1/provisioned-throughput/{id}/sla",
            get(sla_report::<U, D>),
        )
        .route(
            "/v1/provisioned-throughput/{id}/sessions/{session}",
            get(session_report::<U, D>),
        )
        .with_state(tel)
}

/// An API error, in the same shape the rest of the platform uses.
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

fn error(status: StatusCode, code: &'static str, message: impl Into<String>) -> ApiError {
    ApiError {
        status,
        code,
        message: message.into(),
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let kind = match self.status {
            StatusCode::UNAUTHORIZED => "authentication_error",
            StatusCode::NOT_FOUND => "not_found_error",
            StatusCode::SERVICE_UNAVAILABLE => "api_error",
            _ => "invalid_request_error",
        };
        let body = json!({ "error": { "type": kind, "code": self.code, "message": self.message } });
        (self.status, Json(body)).into_response()
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

#[derive(Deserialize)]
struct IngestBody {
    records: Vec<UsageRecord>,
}

async fn ingest<U: UsageStore, D: Directory>(
    State(tel): State<Tel<U, D>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let region = bearer(&headers)
        .and_then(|t| tel.directory.region_for_token(t))
        .ok_or_else(|| {
            error(
                StatusCode::UNAUTHORIZED,
                "invalid_region_token",
                "Invalid or missing region token.",
            )
        })?;
    let batch: IngestBody = serde_json::from_slice(&body).map_err(|e| {
        error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_json",
            e.to_string(),
        )
    })?;
    if batch.records.len() > MAX_BATCH {
        return Err(error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "batch_too_large",
            format!("Send at most {MAX_BATCH} records per request."),
        ));
    }
    let result = tel
        .store
        .append(&region, batch.records, tel.directory.now_ms())
        .await;
    Ok((StatusCode::ACCEPTED, Json(result)).into_response())
}

/// Authenticate the tenant and load the reservation.
async fn reservation<U: UsageStore, D: Directory>(
    tel: &Telemetry<U, D>,
    headers: &HeaderMap,
    id: &str,
) -> Result<ReservationInfo, ApiError> {
    let tenant = bearer(headers)
        .and_then(|k| tel.directory.tenant_for_key(k))
        .ok_or_else(|| {
            error(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid or missing API key.",
            )
        })?;
    tel.directory
        .reservation(&tenant, id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "reservation lookup failed");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Reservation data is unavailable. Retry shortly.",
            )
        })?
        .ok_or_else(|| {
            error(
                StatusCode::NOT_FOUND,
                "not_found",
                "No provisioned throughput with that id.",
            )
        })
}

#[derive(Deserialize)]
struct RangeQuery {
    from: Option<String>,
    to: Option<String>,
    granularity: Option<String>,
    /// Only this deployment's requests (usage report only).
    deployment: Option<String>,
}

fn parse_ms(field: &str, value: &str) -> Result<u64, ApiError> {
    value
        .parse::<jiff::Timestamp>()
        .map(|t| t.as_millisecond().max(0) as u64)
        .map_err(|_| {
            error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_time",
                format!("{field} must be an RFC 3339 timestamp, such as 2026-10-01T00:00:00Z."),
            )
        })
}

/// `from`/`to` with defaults of the last 24 hours.
fn range(q: &RangeQuery, now_ms: u64) -> Result<(u64, u64), ApiError> {
    let to =
        q.to.as_deref()
            .map(|v| parse_ms("to", v))
            .transpose()?
            .unwrap_or(now_ms);
    let from = q
        .from
        .as_deref()
        .map(|v| parse_ms("from", v))
        .transpose()?
        .unwrap_or(to.saturating_sub(DAY_MS));
    if from >= to {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_range",
            "from must be before to.",
        ));
    }
    if to - from > MAX_RANGE_MS {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_range",
            "The range can be at most 35 days.",
        ));
    }
    Ok((from, to))
}

async fn usage_report<U: UsageStore, D: Directory>(
    State(tel): State<Tel<U, D>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<RangeQuery>,
) -> Result<Response, ApiError> {
    let info = reservation(&tel, &headers, &id).await?;
    let (from, to) = range(&q, tel.directory.now_ms())?;
    let granularity = match q.granularity.as_deref() {
        None => {
            // Aim for at most a few hundred points.
            let span = to - from;
            [
                Granularity::Minute,
                Granularity::FiveMinutes,
                Granularity::Hour,
                Granularity::Day,
            ]
            .into_iter()
            .find(|g| span / g.millis() <= 300)
            .unwrap_or(Granularity::Day)
        }
        Some(g) => match Granularity::parse(g) {
            Some(g) => g,
            None => {
                return Err(error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid_granularity",
                    "granularity must be 1m, 5m, 1h, or 1d.",
                ))
            }
        },
    };
    if (to - from).div_ceil(granularity.millis()) > MAX_BUCKETS {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "too_many_points",
            format!("That range and granularity give more than {MAX_BUCKETS} points. Use a coarser granularity."),
        ));
    }
    let mut records = tel.store.range(&info.tenant, &info.id, from, to).await;
    if let Some(d) = &q.deployment {
        records.retain(|r| r.record.deployment == *d);
    }
    let mut report = usage::report(&info, &records, from, to, granularity);
    report.deployment = q.deployment.clone();
    Ok(Json(report).into_response())
}

#[derive(Deserialize)]
struct SlaQuery {
    month: Option<String>,
}

async fn sla_report<U: UsageStore, D: Directory>(
    State(tel): State<Tel<U, D>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<SlaQuery>,
) -> Result<Response, ApiError> {
    let info = reservation(&tel, &headers, &id).await?;
    let now = tel.directory.now_ms();
    let month = q.month.unwrap_or_else(|| sla::month_of(now));
    let Some((start, end)) = sla::month_bounds(&month) else {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_month",
            "month must look like 2026-10.",
        ));
    };
    let period_end = end.min(now.max(start));
    let records = tel.store.range(&info.tenant, &info.id, start, end).await;
    let report = sla::report(&info, &records, &month, (start, period_end), now >= end);
    Ok(Json(report).into_response())
}

async fn session_report<U: UsageStore, D: Directory>(
    State(tel): State<Tel<U, D>>,
    headers: HeaderMap,
    Path((id, session)): Path<(String, String)>,
    Query(q): Query<RangeQuery>,
) -> Result<Response, ApiError> {
    let info = reservation(&tel, &headers, &id).await?;
    let (from, to) = range(&q, tel.directory.now_ms())?;
    let records = tel.store.range(&info.tenant, &info.id, from, to).await;
    sessions::report(&session, &records)
        .map(|r| Json(r).into_response())
        .ok_or_else(|| {
            error(
                StatusCode::NOT_FOUND,
                "session_not_found",
                "No calls for that session in the range.",
            )
        })
}
