//! A stand-in for a Dynamo frontend: serves `POST /v1/chat/completions` in the OpenAI
//! format, streams tokens at a configurable TTFT and TPOT, and reports usage including
//! simulated prefix-cache hits.

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
use pt_core::{count_message, ApproxTokenCounter};
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
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            name: "mock".into(),
            ttft: Duration::from_millis(50),
            tpot: Duration::from_millis(10),
            default_output_tokens: 64,
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    requests: AtomicU64,
}

#[derive(Clone)]
pub struct MockEngine {
    config: Arc<MockConfig>,
    stats: Arc<Stats>,
    /// Hashes of message-level prefixes already seen, standing in for a KV prefix cache.
    prefixes: Arc<Mutex<HashSet<u64>>>,
}

impl MockEngine {
    pub fn new(config: MockConfig) -> Self {
        Self {
            config: Arc::new(config),
            stats: Arc::default(),
            prefixes: Arc::default(),
        }
    }

    pub fn requests_served(&self) -> u64 {
        self.stats.requests.load(Ordering::Relaxed)
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
    fn prefill(&self, messages: &[ChatMessage]) -> (u64, u64) {
        let counter = ApproxTokenCounter;
        let mut hasher = DefaultHasher::new();
        let mut prompt = 0;
        let mut cached = 0;
        let mut still_cached = true;
        let mut seen = self.prefixes.lock().unwrap_or_else(|e| e.into_inner());
        for m in messages {
            m.role.hash(&mut hasher);
            m.content.hash(&mut hasher);
            let prefix = hasher.finish();
            let tokens = count_message(&counter, &m.role, &m.content);
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
    if req.messages.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": {"message": "messages is empty"}})),
        )
            .into_response();
    }
    let cfg = Arc::clone(&engine.config);
    let (prompt_tokens, cached_tokens) = engine.prefill(&req.messages);
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

    if !req.stream {
        tokio::time::sleep(cfg.ttft + cfg.tpot * output_tokens.saturating_sub(1) as u32).await;
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
        let chunk = |delta: Value, finish: Option<&str>| {
            sse(&json!({
                "id": id, "object": "chat.completion.chunk", "created": created, "model": model,
                "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }],
            }))
        };
        tokio::time::sleep(cfg.ttft).await;
        for i in 0..output_tokens {
            if i > 0 {
                tokio::time::sleep(cfg.tpot).await;
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
