//! A stand-in for a Dynamo frontend: serves `POST /v1/chat/completions` in the OpenAI
//! format, streams tokens at a configurable TTFT and TPOT, and reports usage including
//! simulated prefix-cache hits.
//!
//! With [`MockConfig::contention`] set, requests share one simulated engine instead, so
//! tenants really interfere (see [`contention`]): decode steps slow down as the batch
//! grows, unchunked prefill stalls everyone's decode, and KV overflow forces recompute.
//! The interference test suite (docs/05 §7) relies on it.

pub mod contention;

use std::collections::HashSet;
use std::convert::Infallible;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use pt_tokenize::Tokenizers;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// Token text emitted by the mock. Four bytes, so it counts as one token.
const TOKEN: &str = "tok ";

#[derive(Debug, Clone)]
pub struct MockConfig {
    /// Returned in the `x-mock-engine` header so tests can tell engines apart.
    pub name: String,
    pub ttft: Duration,
    pub tpot: Duration,
    /// Output length when the request doesn't set `max_tokens`.
    pub default_output_tokens: u64,
    /// Simulate a continuous-batching engine instead of fixed TTFT/TPOT.
    pub contention: Option<contention::Contention>,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            name: "mock".into(),
            ttft: Duration::from_millis(50),
            tpot: Duration::from_millis(10),
            default_output_tokens: 64,
            contention: None,
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    requests: AtomicU64,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
}

/// Counts a request as in flight until dropped.
struct InFlight(Arc<Stats>);

impl InFlight {
    fn start(stats: &Arc<Stats>) -> Self {
        let now = stats.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        stats.max_in_flight.fetch_max(now, Ordering::SeqCst);
        Self(Arc::clone(stats))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct MockEngine {
    config: Arc<MockConfig>,
    stats: Arc<Stats>,
    /// The simulated engine, when contention is on.
    batcher: Option<contention::Batcher>,
    /// Hashes of message-level prefixes already seen, standing in for a KV prefix cache.
    prefixes: Arc<Mutex<HashSet<u64>>>,
    /// Counts prompt tokens: 4 bytes per token unless a tokenizer is set.
    tokens: Arc<Tokenizers>,
}

impl MockEngine {
    pub fn new(config: MockConfig) -> Self {
        let batcher = config.contention.clone().map(contention::Batcher::start);
        Self {
            config: Arc::new(config),
            stats: Arc::default(),
            batcher,
            prefixes: Arc::default(),
            tokens: Arc::new(Tokenizers::ratio_only()),
        }
    }

    /// Count prompt tokens with real tokenizers, as an engine would (ADR-028).
    pub fn with_tokenizers(mut self, tokens: Arc<Tokenizers>) -> Self {
        self.tokens = tokens;
        self
    }

    /// Requests the simulated engine evicted for lack of KV and recomputed.
    pub fn preemptions(&self) -> u64 {
        self.batcher.as_ref().map_or(0, |b| b.preemptions())
    }

    pub fn requests_served(&self) -> u64 {
        self.stats.requests.load(Ordering::Relaxed)
    }

    /// Most requests this engine has had in progress at once.
    pub fn max_in_flight(&self) -> u64 {
        self.stats.max_in_flight.load(Ordering::SeqCst)
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/v1/chat/completions", post(chat_completions))
            .route("/healthz", get(|| async { "ok" }))
            .with_state(self.clone())
    }

    pub async fn serve(self, listener: TcpListener) -> anyhow::Result<()> {
        axum::serve(listener, self.router()).await?;
        Ok(())
    }

    /// Returns (prompt_tokens, cached_tokens) and records the prompt's prefixes.
    fn prefill(&self, model: &str, messages: &[ChatMessage]) -> (u64, u64) {
        let mut hasher = DefaultHasher::new();
        let mut prompt = 0;
        let mut cached = 0;
        let mut still_cached = true;
        let mut seen = self.prefixes.lock().unwrap_or_else(|e| e.into_inner());
        for m in messages {
            m.role.hash(&mut hasher);
            m.content.hash(&mut hasher);
            let prefix = hasher.finish();
            let tokens = self
                .tokens
                .count(model, &[(m.role.clone(), m.content.clone())]);
            prompt += tokens;
            if still_cached && seen.contains(&prefix) {
                cached += tokens;
            } else {
                still_cached = false;
                seen.insert(prefix);
            }
        }
        (prompt, cached)
    }
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    role: String,
    #[serde(default)]
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChatRequest {
    #[serde(default)]
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(default)]
    max_tokens: Option<u64>,
    #[serde(default)]
    max_completion_tokens: Option<u64>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    stream_options: Option<StreamOptions>,
}

#[derive(Debug, Deserialize)]
struct StreamOptions {
    #[serde(default)]
    include_usage: bool,
}

async fn chat_completions(
    State(engine): State<MockEngine>,
    Json(req): Json<ChatRequest>,
) -> Response {
    engine.stats.requests.fetch_add(1, Ordering::Relaxed);
    let in_flight = InFlight::start(&engine.stats);
    if req.messages.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": {"message": "messages is empty"}})),
        )
            .into_response();
    }
    let cfg = Arc::clone(&engine.config);
    let (prompt_tokens, cached_tokens) = engine.prefill(&req.model, &req.messages);
    let limit = req.max_completion_tokens.or(req.max_tokens);
    let output_tokens = limit.map_or(cfg.default_output_tokens, |m| {
        m.min(cfg.default_output_tokens)
    });
    let finish_reason = if limit.is_some_and(|m| m <= cfg.default_output_tokens) {
        "length"
    } else {
        "stop"
    };
    let usage = json!({
        "prompt_tokens": prompt_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": prompt_tokens + output_tokens,
        "prompt_tokens_details": { "cached_tokens": cached_tokens },
    });
    let id = format!("chatcmpl-{}", uuid::Uuid::new_v4().simple());
    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-mock-engine",
        cfg.name.parse().expect("engine name is a valid header"),
    );

    // With contention, tokens come from the shared engine; otherwise from fixed timings.
    let mut tokens = engine.batcher.as_ref().map(|b| {
        b.submit(
            prompt_tokens,
            prompt_tokens.saturating_sub(cached_tokens),
            output_tokens,
        )
    });

    if !req.stream {
        match tokens.as_mut() {
            Some(rx) => while rx.recv().await.is_some() {},
            None => {
                tokio::time::sleep(cfg.ttft + cfg.tpot * output_tokens.saturating_sub(1) as u32)
                    .await
            }
        }
        let body = json!({
            "id": id,
            "object": "chat.completion",
            "created": created,
            "model": req.model,
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": TOKEN.repeat(output_tokens as usize) },
                "finish_reason": finish_reason,
            }],
            "usage": usage,
        });
        return (headers, Json(body)).into_response();
    }

    let include_usage = req.stream_options.is_some_and(|o| o.include_usage);
    let (tx, rx) = mpsc::channel::<Bytes>(16);
    let model = req.model;
    tokio::spawn(async move {
        let _in_flight = in_flight;
        let chunk = |delta: Value, finish: Option<&str>| {
            sse(&json!({
                "id": id, "object": "chat.completion.chunk", "created": created, "model": model,
                "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }],
            }))
        };
        if tokens.is_none() {
            tokio::time::sleep(cfg.ttft).await;
        }
        for i in 0..output_tokens {
            match tokens.as_mut() {
                Some(rx) => {
                    if rx.recv().await.is_none() {
                        break;
                    }
                }
                None if i > 0 => tokio::time::sleep(cfg.tpot).await,
                None => {}
            }
            let delta = if i == 0 {
                json!({ "role": "assistant", "content": TOKEN })
            } else {
                json!({ "content": TOKEN })
            };
            if tx.send(chunk(delta, None)).await.is_err() {
                return; // client went away
            }
        }
        let _ = tx.send(chunk(json!({}), Some(finish_reason))).await;
        if include_usage {
            let _ = tx
                .send(sse(&json!({
                    "id": id, "object": "chat.completion.chunk", "created": created, "model": model,
                    "choices": [], "usage": usage,
                })))
                .await;
        }
        let _ = tx.send(Bytes::from_static(b"data: [DONE]\n\n")).await;
    });

    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|b| (Ok::<_, Infallible>(b), rx))
    });
    headers.insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());
    headers.insert(header::CACHE_CONTROL, "no-cache".parse().unwrap());
    (headers, Body::from_stream(stream)).into_response()
}

fn sse(v: &Value) -> Bytes {
    Bytes::from(format!("data: {v}\n\n"))
}
