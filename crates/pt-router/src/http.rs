//! The router's HTTP tier: an OpenAI-compatible proxy in front of the workers.
//!
//! ```text
//! POST /v1/chat/completions   queue → dispatch to a worker → stream back → free capacity
//! GET  /v1/router/status      queues, dispatch counts, preemptions, per-worker load and KV
//! ```
//!
//! It reads the gateway's `x-pt-reservation`, `x-pt-class`, `x-pt-wu-estimate`,
//! `x-pt-weight`, `x-pt-session-id`, and `x-pt-failover` headers. Requests without them are
//! treated as PAYG.
//!
//! **Failover.** A request marked `x-pt-failover` (it uses a failover entitlement) raises
//! the fence for `failover_hold_ms`: new PAYG stays off hot spares, and provisioned work
//! waiting longer than `preempt_grace_ms` aborts running PAYG. A preempted PAYG request
//! gets 503 `preempted` if its response hasn't started, or a final SSE error event if it
//! is streaming. Dropping the upstream response cancels the work on the worker.
//!
//! Capacity accounting must survive every exit: normal completion, worker errors, client
//! disconnects while queued or streaming, and timeouts that race a dispatch. The dispatch
//! message ([`Go`]) owns a [`Release`] guard, so capacity is freed whenever the assignment
//! is dropped, used or not.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use pt_core::{count_message, ApproxTokenCounter, TrafficClass};
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::config::RouterConfig;
use crate::dispatch::Dispatcher;
use crate::workers::{NoWorker, Placement};

/// Sent to a waiting request when it's dispatched.
struct Go {
    url: String,
    worker_id: String,
    release: Release,
    /// Fires if the request is preempted.
    abort: oneshot::Receiver<()>,
}

/// Frees a worker's slot and KV blocks when dropped, unless disarmed.
struct Release {
    shared: Arc<Shared>,
    id: u64,
    armed: bool,
}

impl Drop for Release {
    fn drop(&mut self) {
        if self.armed {
            let mut s = self.shared.lock();
            s.aborts.remove(&self.id);
            s.dispatcher.release(self.id);
            drop(s);
            self.shared.pump();
        }
    }
}

/// Cancels a queued request if the handler stops waiting (client gone).
struct QueueGuard {
    shared: Arc<Shared>,
    id: u64,
    armed: bool,
}

impl Drop for QueueGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shared.lock().dispatcher.cancel(self.id);
        }
    }
}

pub struct Shared {
    state: Mutex<Inner>,
    http: reqwest::Client,
    block_size: u64,
    default_max_tokens: u64,
    queue_timeout: Duration,
    failover_hold: Duration,
    preempt_grace: Duration,
}

struct Inner {
    dispatcher: Dispatcher<oneshot::Sender<Go>>,
    /// Abort handles for dispatched requests, by id.
    aborts: HashMap<u64, oneshot::Sender<()>>,
}

impl Shared {
    pub fn new(config: &RouterConfig) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Inner {
                dispatcher: Dispatcher::new(
                    config.workers(),
                    config.allocations(),
                    config.weights(),
                    config.payg_guard_every,
                ),
                aborts: HashMap::new(),
            }),
            http: reqwest::Client::new(),
            block_size: config.block_size,
            default_max_tokens: config.default_max_tokens,
            queue_timeout: Duration::from_millis(config.queue_timeout_ms),
            failover_hold: Duration::from_millis(config.failover_hold_ms),
            preempt_grace: Duration::from_millis(config.preempt_grace_ms),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Hand out every request that can go now, then preempt PAYG for provisioned work that
    /// has waited too long during a failover.
    fn pump(self: &Arc<Self>) {
        let now = Instant::now();
        let mut s = self.lock();
        loop {
            let sent = s.dispatcher.dispatch(now);
            if sent.is_empty() {
                break;
            }
            for a in sent {
                let w = s.dispatcher.worker(a.worker);
                let (abort_tx, abort) = oneshot::channel();
                let go = Go {
                    url: w.url.clone(),
                    worker_id: w.id.clone(),
                    release: Release {
                        shared: Arc::clone(self),
                        id: a.id,
                        armed: true,
                    },
                    abort,
                };
                match a.payload.send(go) {
                    Ok(()) => {
                        s.aborts.insert(a.id, abort_tx);
                    }
                    Err(mut go) => {
                        // The client already left. We hold the lock, so release directly.
                        go.release.armed = false;
                        s.dispatcher.release(a.id);
                    }
                }
            }
        }
        for id in s.dispatcher.preempt(now, self.preempt_grace) {
            if let Some(abort) = s.aborts.remove(&id) {
                tracing::info!(id, "preempting PAYG for provisioned work");
                let _ = abort.send(());
            }
        }
    }
}

/// Re-run dispatch periodically, so preemption happens once the grace period passes even
/// if no request arrives or finishes. Stops when the router is dropped.
fn spawn_ticker(shared: &Arc<Shared>) {
    let weak = Arc::downgrade(shared);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            tick.tick().await;
            match weak.upgrade() {
                Some(s) => s.pump(),
                None => return,
            }
        }
    });
}

/// The HTTP app. Must be called inside a Tokio runtime (it starts the dispatch ticker).
pub fn router(shared: Arc<Shared>) -> Router {
    spawn_ticker(&shared);
    Router::new()
        .route("/v1/chat/completions", post(chat))
        .route("/v1/router/status", get(status))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(shared)
}

fn error(status: StatusCode, code: &'static str, message: &str) -> Response {
    let body = json!({ "error": { "type": "router_error", "code": code, "message": message } });
    let mut r = (status, Json(body)).into_response();
    r.headers_mut()
        .insert("x-pt-reason", HeaderValue::from_static(code));
    r
}

fn header_str<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}

fn class_of(h: &HeaderMap) -> TrafficClass {
    match header_str(h, "x-pt-class") {
        Some("provisioned") => TrafficClass::Provisioned,
        Some("burst") => TrafficClass::Burst,
        Some("spillover") => TrafficClass::Spillover,
        _ => TrafficClass::Payg,
    }
}

/// Cumulative message-prefix hashes with token counts, plus the prompt's token count.
fn prefixes(req: &Value) -> (Vec<(u64, u64)>, u64) {
    let counter = ApproxTokenCounter;
    let mut hasher = DefaultHasher::new();
    let mut tokens = 0;
    let mut out = Vec::new();
    for m in req
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let role = m.get("role").and_then(Value::as_str).unwrap_or("");
        let content = match m.get("content") {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => String::new(),
        };
        role.hash(&mut hasher);
        content.hash(&mut hasher);
        tokens += count_message(&counter, role, &content);
        out.push((hasher.finish(), tokens));
    }
    (out, tokens)
}

async fn chat(State(shared): State<Arc<Shared>>, headers: HeaderMap, body: Bytes) -> Response {
    let received = Instant::now();
    let req: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "Request body must be JSON.",
            )
        }
    };
    let (prefix_hashes, prompt_tokens) = prefixes(&req);
    let max_tokens = req
        .get("max_completion_tokens")
        .or_else(|| req.get("max_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(shared.default_max_tokens);
    let kv_blocks = (prompt_tokens + max_tokens)
        .div_ceil(shared.block_size)
        .max(1) as u32;
    let reservation = header_str(&headers, "x-pt-reservation")
        .or_else(|| header_str(&headers, "x-pt-tenant"))
        .unwrap_or("anonymous")
        .to_string();
    let placement = Placement {
        reservation,
        class: class_of(&headers),
        kv_blocks,
        prefixes: prefix_hashes,
        prompt_tokens,
        session: header_str(&headers, "x-pt-session-id").map(str::to_owned),
    };
    let wu = header_str(&headers, "x-pt-wu-estimate")
        .and_then(|v| v.parse().ok())
        .unwrap_or((prompt_tokens + max_tokens) as f64);
    let weight = header_str(&headers, "x-pt-weight").and_then(|v| v.parse().ok());

    let (tx, mut rx) = oneshot::channel();
    let deadline = received + shared.queue_timeout;
    let enqueued = {
        let mut s = shared.lock();
        if headers.contains_key("x-pt-failover") {
            s.dispatcher.note_failover(received + shared.failover_hold);
        }
        s.dispatcher
            .enqueue(placement, wu, weight, received, deadline, tx)
    };
    let id = match enqueued {
        Ok(id) => id,
        Err(NoWorker::TooLarge) => {
            return error(
                StatusCode::BAD_REQUEST,
                "router_request_too_large",
                "The request needs more KV cache than any worker has.",
            )
        }
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "router_no_worker",
                "No worker can serve this reservation.",
            )
        }
    };
    let mut guard = QueueGuard {
        shared: Arc::clone(&shared),
        id,
        armed: true,
    };
    shared.pump();

    let go = match tokio::time::timeout(shared.queue_timeout, &mut rx).await {
        Ok(Ok(go)) => go,
        _ => {
            // Cancel under the lock so it can't be dispatched from now on, then drop any
            // dispatch that raced the timeout; its guard frees the capacity.
            guard.armed = false;
            shared.lock().dispatcher.cancel(id);
            drop(rx.try_recv());
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "router_queue_timeout",
                "The request waited too long for a worker.",
            );
        }
    };
    guard.armed = false;
    let queued_ms = received.elapsed().as_millis() as u64;
    let Go {
        url,
        worker_id,
        release,
        mut abort,
    } = go;

    let mut upstream = shared
        .http
        .post(format!("{url}/v1/chat/completions"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(body);
    for (name, value) in &headers {
        let n = name.as_str();
        if n.starts_with("x-pt-") || n == "x-request-id" {
            upstream = upstream.header(name, value);
        }
    }
    let sent = tokio::select! {
        r = upstream.send() => r,
        _ = &mut abort => return preempted(),
    };
    let resp = match sent {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, worker = %worker_id, "worker request failed");
            drop(release);
            return error(
                StatusCode::BAD_GATEWAY,
                "worker_unavailable",
                "The worker is unavailable.",
            );
        }
    };

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut out_headers = HeaderMap::new();
    for (name, value) in resp.headers() {
        if name != header::CONTENT_LENGTH
            && name != header::TRANSFER_ENCODING
            && name != header::CONNECTION
        {
            out_headers.insert(name.clone(), value.clone());
        }
    }
    if let Ok(v) = HeaderValue::from_str(&worker_id) {
        out_headers.insert("x-pt-router-worker", v);
    }
    out_headers.insert("x-pt-router-queue-ms", HeaderValue::from(queued_ms));

    let sse = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/event-stream"));

    // The release guard lives with the body stream, so capacity frees when the response
    // finishes, the client disconnects, or the request is preempted.
    let stream = futures::stream::unfold(
        Some((resp.bytes_stream(), release, abort)),
        move |state| async move {
            let (mut s, g, mut abort) = state?;
            tokio::select! {
                chunk = s.next() => {
                    let chunk = chunk?;
                    Some((chunk.map_err(std::io::Error::other), Some((s, g, abort))))
                }
                _ = &mut abort => {
                    // Dropping the upstream stream and guard cancels the work and frees it.
                    drop((s, g));
                    let end = if sse {
                        Ok(Bytes::from_static(PREEMPTED_EVENT.as_bytes()))
                    } else {
                        Err(std::io::Error::other("preempted"))
                    };
                    Some((end, None))
                }
            }
        },
    );
    (status, out_headers, Body::from_stream(stream)).into_response()
}

/// Final event for a preempted streaming response.
const PREEMPTED_EVENT: &str = "data: {\"error\":{\"type\":\"router_error\",\"code\":\"preempted\",\"message\":\"Preempted to serve provisioned traffic. Retry.\"}}\n\n";

fn preempted() -> Response {
    let mut r = error(
        StatusCode::SERVICE_UNAVAILABLE,
        "preempted",
        "Preempted to serve provisioned traffic. Retry.",
    );
    r.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    r
}

async fn status(State(shared): State<Arc<Shared>>) -> Json<Value> {
    let status = shared.lock().dispatcher.status(Instant::now());
    Json(serde_json::to_value(status).unwrap_or(Value::Null))
}
