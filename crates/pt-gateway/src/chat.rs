//! `POST /v1/chat/completions`: the admission path (docs/04 §1).
//!
//! authenticate → count input tokens → estimate WU → admit (with boundary policy) →
//! forward → stream back → settle actual WU → emit a usage record.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes;
use futures::StreamExt;
use pt_admission::{AdmitRequest, AdmitTicket, Decision};
use pt_core::cost::{estimate_kv_token_seconds, measured_kv_token_seconds};
use pt_core::{
    count_message, Outcome, RejectReason, Timings, TokenBreakdown, TrafficClass, UsageRecord,
    WorkBreakdown,
};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::sse::{EngineUsage, Observations, SseInspector};
use crate::state::{AppState, Deployment};

pub async fn chat_completions(
    State(app): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // The body extractor has read the whole request, so this is "fully received" (ADR-010).
    let received_at = Instant::now();
    let request_id = Uuid::new_v4();

    let Some(dep) = bearer_token(&headers).and_then(|k| app.deployment_for_key(k)) else {
        return api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "Invalid or missing API key.",
        );
    };
    let res = Arc::clone(&dep.reservation);

    let mut req: Value = match serde_json::from_slice(&body) {
        Ok(v @ Value::Object(_)) => v,
        _ => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "Request body must be a JSON object.",
            )
        }
    };
    if let Some(model) = req.get("model").and_then(Value::as_str) {
        if model != res.model {
            return api_error(
                StatusCode::BAD_REQUEST,
                "model_not_found",
                &format!(
                    "Deployment {} serves model {}, not {model}.",
                    dep.id, res.model
                ),
            );
        }
    }
    let messages = match parse_messages(&req) {
        Ok(m) => m,
        Err(msg) => return api_error(StatusCode::BAD_REQUEST, "invalid_request_error", msg),
    };

    // Estimate WU. Prefill is exact after counting; decode is predicted (docs/04 §3).
    let input_tokens: u64 = messages
        .iter()
        .map(|(role, content)| count_message(&*app.tokens, role, content))
        .sum();
    let max_tokens = req
        .get("max_completion_tokens")
        .or_else(|| req.get("max_tokens"))
        .and_then(Value::as_u64);
    let decode_est = dep.estimator().estimate(max_tokens);
    let in_shape = res
        .shape
        .is_in_shape(input_tokens, max_tokens.unwrap_or(decode_est));
    let kv_est = estimate_kv_token_seconds(input_tokens, decode_est, res.tier.tpot_target_s());
    let wu_est = res
        .profile
        .work_units(&WorkBreakdown::new(input_tokens, 0, decode_est, kv_est));

    let session_id = header_str(&headers, "x-pt-session-id").map(str::to_owned);
    let continuation = header_str(&headers, "x-pt-priority")
        .is_some_and(|p| p.eq_ignore_ascii_case("continuation"));

    let mut settlement = Settlement {
        app: app.clone(),
        dep: Arc::clone(&dep),
        ticket: None,
        request_id,
        session_id,
        received_at,
        input_tokens,
        wu_est,
        in_shape,
        class: None,
        queued_for: Duration::ZERO,
        obs: Observations::default(),
        outcome: Outcome::ClientCancelled,
        engine_accepted: false,
        streamed: false,
    };

    // Admission, with the boundary-policy chain (docs/04 §4).
    let admit = AdmitRequest {
        estimated_wu: wu_est,
        continuation,
        received_at,
    };
    let mut slot = None;
    let ticket = loop {
        match res.limiter.admit(&admit, slot.take(), Instant::now()) {
            Decision::Admit(t) => break t,
            Decision::Queue(s) => {
                tokio::time::sleep(s.wait).await;
                slot = Some(s);
            }
            Decision::Reject {
                reason,
                retry_after,
            } => {
                settlement.outcome = Outcome::Rejected(reason);
                settlement.emit(0.0, 0.0, TokenBreakdown::default(), Instant::now());
                return reject_response(&settlement, reason, retry_after);
            }
        }
    };
    settlement.class = Some(ticket.class);
    settlement.queued_for = ticket.queued_for;
    let class = ticket.class;
    settlement.ticket = Some(ticket);

    // Forward. Always ask for usage so settlement uses the engine's counts.
    let stream = req.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let client_wants_usage = req
        .pointer("/stream_options/include_usage")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if stream {
        req["stream_options"] = json!({ "include_usage": true });
    }
    let base = match class {
        TrafficClass::Spillover | TrafficClass::Payg => &app.payg_engine_url,
        TrafficClass::Provisioned | TrafficClass::Burst => &app.engine_url,
    };
    let mut upstream = app
        .http
        .post(format!("{base}/v1/chat/completions"))
        .header("x-request-id", request_id.to_string())
        .header("x-pt-tenant", &res.tenant)
        .header("x-pt-reservation", &res.id)
        .header("x-pt-class", class.as_str())
        .header("x-pt-wu-estimate", format!("{wu_est:.1}"))
        .json(&req);
    if let Some(s) = &settlement.session_id {
        upstream = upstream.header("x-pt-session-id", s);
    }

    let resp = match upstream.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, %request_id, "engine request failed");
            settlement.outcome = Outcome::Error(format!("engine unreachable: {e}"));
            drop(settlement);
            return api_error(
                StatusCode::BAD_GATEWAY,
                "engine_unavailable",
                "The inference engine is unavailable.",
            );
        }
    };

    let status = resp.status();
    let pt_headers = pt_response_headers(&settlement, wu_est);

    if !status.is_success() {
        let body = resp.bytes().await.unwrap_or_default();
        settlement.outcome = Outcome::Error(format!("engine returned {status}"));
        drop(settlement);
        let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        return (status, pt_headers, body).into_response();
    }
    settlement.engine_accepted = true;
    let content_type = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or(HeaderValue::from_static("application/json"));

    if !stream {
        return match resp.bytes().await {
            Ok(body) => {
                let now = Instant::now();
                settlement.obs.usage = serde_json::from_slice::<Value>(&body)
                    .ok()
                    .as_ref()
                    .and_then(EngineUsage::from_json);
                settlement.obs.first_content_at = Some(now);
                settlement.obs.last_content_at = Some(now);
                settlement.outcome = Outcome::Ok;
                drop(settlement);
                (
                    StatusCode::OK,
                    pt_headers,
                    [(header::CONTENT_TYPE, content_type)],
                    body,
                )
                    .into_response()
            }
            Err(e) => {
                settlement.outcome = Outcome::Error(format!("reading engine response: {e}"));
                drop(settlement);
                api_error(
                    StatusCode::BAD_GATEWAY,
                    "engine_error",
                    "The inference engine response was interrupted.",
                )
            }
        };
    }

    settlement.streamed = true;
    let state = StreamState {
        upstream: Box::pin(resp.bytes_stream()),
        inspector: SseInspector::new(!client_wants_usage),
        settlement,
        done: false,
    };
    let body = futures::stream::unfold(state, |mut st| async move {
        if st.done {
            return None; // dropping `st` settles the request
        }
        loop {
            match st.upstream.next().await {
                Some(Ok(bytes)) => {
                    let out = st
                        .inspector
                        .push(&bytes, &mut st.settlement.obs, Instant::now());
                    if !out.is_empty() {
                        return Some((Ok::<_, std::io::Error>(out), st));
                    }
                }
                Some(Err(e)) => {
                    st.settlement.outcome = Outcome::Error(format!("engine stream failed: {e}"));
                    st.settlement.finish();
                    return None;
                }
                None => {
                    st.settlement.outcome = Outcome::Ok;
                    st.done = true;
                    let rest = st.inspector.finish();
                    if rest.is_empty() {
                        return None;
                    }
                    return Some((Ok(rest), st));
                }
            }
        }
    });
    (
        StatusCode::OK,
        pt_headers,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
        ],
        Body::from_stream(body),
    )
        .into_response()
}

struct StreamState {
    upstream: std::pin::Pin<Box<dyn futures::Stream<Item = reqwest::Result<Bytes>> + Send>>,
    inspector: SseInspector,
    settlement: Settlement,
    done: bool,
}

/// Settles the reservation and emits the usage record exactly once, when dropped. Dropping
/// covers every exit: normal completion, engine errors, and client disconnects mid-stream.
struct Settlement {
    app: AppState,
    dep: Arc<Deployment>,
    ticket: Option<AdmitTicket>,
    request_id: Uuid,
    session_id: Option<String>,
    received_at: Instant,
    input_tokens: u64,
    wu_est: f64,
    in_shape: bool,
    class: Option<TrafficClass>,
    queued_for: Duration,
    obs: Observations,
    outcome: Outcome,
    /// The engine returned 2xx, so work was done even if usage never arrived.
    engine_accepted: bool,
    streamed: bool,
}

impl Settlement {
    fn finish(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        let now = Instant::now();
        let res = &self.dep.reservation;

        let (uncached, cached, decode) = match self.obs.usage {
            Some(u) => (
                u.prompt_tokens.saturating_sub(u.cached_tokens),
                u.cached_tokens,
                u.completion_tokens,
            ),
            // Cancelled or truncated: charge the prompt and the content we forwarded.
            None if self.engine_accepted => (self.input_tokens, 0, self.obs.content_chunks),
            None => (0, 0, 0),
        };
        let prompt = uncached + cached;
        let kv = match (
            self.streamed,
            self.obs.first_content_at,
            self.obs.last_content_at,
        ) {
            (true, Some(first), Some(last)) => {
                measured_kv_token_seconds(prompt, decode, (last - first).as_secs_f64())
            }
            // No per-token timing for non-streamed responses: use the tier's TPOT target.
            _ => estimate_kv_token_seconds(prompt, decode, res.tier.tpot_target_s()),
        };
        let wu_actual = res
            .profile
            .work_units(&WorkBreakdown::new(uncached, cached, decode, kv));
        res.limiter.settle(ticket, wu_actual, now);
        if self.outcome == Outcome::Ok {
            self.dep.estimator().record(decode);
        }
        self.emit(
            wu_actual,
            kv,
            TokenBreakdown {
                uncached_prefill: uncached,
                cached_prefill: cached,
                decode,
            },
            now,
        );
    }

    fn emit(&self, wu_actual: f64, kv: f64, tokens: TokenBreakdown, now: Instant) {
        let ms = |d: Duration| d.as_secs_f64() * 1_000.0;
        let ttft = self.obs.first_content_at.map(|f| f - self.received_at);
        let tpot = match (
            self.streamed,
            self.obs.first_content_at,
            self.obs.last_content_at,
        ) {
            (true, Some(f), Some(l)) if tokens.decode > 1 => {
                Some(ms(l - f) / (tokens.decode - 1) as f64)
            }
            _ => None,
        };
        let res = &self.dep.reservation;
        self.app.sink.emit(UsageRecord {
            request_id: self.request_id,
            tenant: res.tenant.clone(),
            reservation: res.id.clone(),
            deployment: self.dep.id.clone(),
            class: self.class,
            session_id: self.session_id.clone(),
            tokens,
            kv_token_seconds: kv,
            wu_estimated: self.wu_est,
            wu_actual,
            timings: Timings {
                queue_ms: ms(self.queued_for),
                ttft_ms: ttft.map(ms),
                total_ms: ms(now - self.received_at),
                tpot_ms: tpot,
            },
            in_shape: self.in_shape,
            outcome: self.outcome.clone(),
            profile: res.profile.name.clone(),
        });
    }
}

impl Drop for Settlement {
    fn drop(&mut self) {
        self.finish();
    }
}

fn reject_response(s: &Settlement, reason: RejectReason, retry_after: Duration) -> Response {
    let remaining = s
        .dep
        .reservation
        .limiter
        .status(Instant::now())
        .level_wu
        .max(0.0);
    let retry_secs = retry_after.as_secs_f64().ceil().max(1.0) as u64;
    let mut resp = api_error(
        StatusCode::TOO_MANY_REQUESTS,
        reason.as_str(),
        "The deployment's provisioned throughput is exhausted. Retry after the Retry-After interval.",
    );
    let h = resp.headers_mut();
    h.insert(header::RETRY_AFTER, HeaderValue::from(retry_secs));
    h.insert(
        HeaderName::from_static("x-pt-reason"),
        HeaderValue::from_static(reason.as_str()),
    );
    h.insert(
        HeaderName::from_static("x-pt-entitlement-remaining"),
        HeaderValue::from(remaining.floor() as u64),
    );
    h.insert(
        HeaderName::from_static("x-request-id"),
        hv(&s.request_id.to_string()),
    );
    resp
}

fn pt_response_headers(s: &Settlement, wu_est: f64) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        HeaderName::from_static("x-request-id"),
        hv(&s.request_id.to_string()),
    );
    h.insert(HeaderName::from_static("x-pt-deployment"), hv(&s.dep.id));
    if let Some(class) = s.class {
        h.insert(
            HeaderName::from_static("x-pt-class"),
            HeaderValue::from_static(class.as_str()),
        );
    }
    h.insert(
        HeaderName::from_static("x-pt-wu-estimate"),
        hv(&format!("{wu_est:.1}")),
    );
    h.insert(
        HeaderName::from_static("x-pt-queue-ms"),
        HeaderValue::from(s.queued_for.as_millis() as u64),
    );
    h
}

fn hv(s: &str) -> HeaderValue {
    HeaderValue::from_str(s).unwrap_or(HeaderValue::from_static("invalid"))
}

pub(crate) fn api_error(status: StatusCode, code: &str, message: &str) -> Response {
    let kind = match status {
        StatusCode::UNAUTHORIZED => "authentication_error",
        StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
        s if s.is_server_error() => "server_error",
        _ => "invalid_request_error",
    };
    (
        status,
        Json(json!({ "error": { "message": message, "type": kind, "code": code } })),
    )
        .into_response()
}

pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    header_str(headers, "authorization")?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Extract `(role, text)` pairs. Content may be a string, an array of parts, or null
/// (for example an assistant tool call).
fn parse_messages(req: &Value) -> Result<Vec<(String, String)>, &'static str> {
    let messages = req
        .get("messages")
        .and_then(Value::as_array)
        .filter(|m| !m.is_empty())
        .ok_or("`messages` must be a non-empty array.")?;
    messages
        .iter()
        .map(|m| {
            let role = m
                .get("role")
                .and_then(Value::as_str)
                .ok_or("Each message needs a `role`.")?;
            let text = match m.get("content") {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(s)) => s.clone(),
                Some(Value::Array(parts)) => parts
                    .iter()
                    .filter_map(|p| p.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n"),
                Some(_) => return Err("Message `content` must be a string or an array of parts."),
            };
            Ok((role.to_owned(), text))
        })
        .collect()
}
