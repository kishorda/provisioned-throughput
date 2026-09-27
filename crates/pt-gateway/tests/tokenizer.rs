//! Input tokens counted with the model's tokenizer (ADR-028): exact admission estimates,
//! the per-message cache, the inline budget with background fill, and the count passed
//! downstream in `x-pt-prompt-tokens`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile, Shape, Tier};
use pt_gateway::config::{DeploymentConfig, ReservationConfig, ServerConfig, TokenizationConfig};
use pt_gateway::usage::MemorySink;
use pt_gateway::{router, AppState, GatewayConfig};
use pt_mock_engine::{MockConfig, MockEngine};
use pt_tokenize::{TokenizerSpec, Tokenizers};
use serde_json::{json, Value};

const KEY: &str = "sk-tok";
const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../pt-tokenize/tests/fixtures/wordlevel.json"
);

fn spec(model: &str) -> TokenizerSpec {
    TokenizerSpec {
        model: model.into(),
        path: FIXTURE.into(),
        message_overhead: 4,
        ..Default::default()
    }
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

/// A mock engine counting with the same tokenizer, recording `x-pt-prompt-tokens`.
async fn engine() -> (String, Arc<Mutex<Vec<u64>>>) {
    let tokens = Arc::new(Tokenizers::load(&[spec("m")], 1_000).unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    let app = MockEngine::new(MockConfig {
        ttft: Duration::from_millis(1),
        tpot: Duration::from_millis(1),
        default_output_tokens: 2,
        ..Default::default()
    })
    .with_tokenizers(tokens)
    .router()
    .layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let record = record.clone();
            async move {
                if let Some(n) = req
                    .headers()
                    .get("x-pt-prompt-tokens")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                {
                    record.lock().unwrap().push(n);
                }
                next.run(req).await
            }
        },
    ));
    (serve(app).await, seen)
}

fn config(engine: &str, inline_bytes: usize) -> GatewayConfig {
    GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: engine.into(),
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: 100_000.0,
        },
        profiles: vec![PerformanceProfile {
            name: "p".into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 1.0,
                d: 0.0,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }],
        entitlements: None,
        quota: None,
        tokenization: TokenizationConfig {
            tokenizers: vec![spec("m")],
            cache_entries: 1_000,
            inline_bytes,
        },
        prefix_cache: Default::default(),
        usage_export: None,
        reservations: vec![ReservationConfig {
            id: "res".into(),
            tenant: "acme".into(),
            model: "m".into(),
            cus: 10,
            tier: Tier::Agentic,
            profile: "p".into(),
            shape: Shape {
                input_p95: 100_000,
                input_max: 200_000,
                output_p95: 4,
                context_ceiling: 200_000,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
        }],
        deployments: vec![DeploymentConfig {
            id: "dep".into(),
            reservation: "res".into(),
            api_key: KEY.into(),
            boundary_policy: Default::default(),
            max_share: None,
        }],
    }
}

async fn chat(gw: &str, messages: &Value) {
    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .bearer_auth(KEY)
        .json(&json!({ "model": "m", "max_tokens": 2, "messages": messages }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.bytes().await.unwrap();
}

async fn model_status(gw: &str) -> Value {
    let s: Value = reqwest::get(format!("{gw}/internal/v1/tokenizers"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    s["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["model"] == "m")
        .cloned()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_counts_with_the_model_tokenizer() {
    let (engine, seen) = engine().await;
    let sink = Arc::new(MemorySink::default());
    let app = AppState::new(&config(&engine, 200), sink.clone()).unwrap();
    let gw = serve(router(app.clone())).await;

    // Short messages: counted exactly before admission. "hello, world." is 4 tokens,
    // "user" 1, plus 4 of template overhead.
    let short = json!([{ "role": "user", "content": "hello, world." }]);
    chat(&gw, &short).await;
    assert_eq!(
        *seen.lock().unwrap(),
        vec![9],
        "the router gets the exact count"
    );
    let r = &sink.records()[0];
    assert_eq!(r.tokens.uncached_prefill, 9, "the engine agrees");
    let s = model_status(&gw).await;
    assert_eq!(s["method"], "tokenizer");
    assert_eq!(s["engine_to_estimate"], 1.0);

    // A 1,200-byte message is over the 200-byte budget: estimated at 4 bytes per token,
    // then tokenized in the background.
    let long = "hello world ".repeat(100);
    let convo = json!([
        { "role": "system", "content": long },
        { "role": "user", "content": "hello" },
    ]);
    chat(&gw, &convo).await;
    let estimated = seen.lock().unwrap()[1];
    assert_eq!(estimated, (4 + 2 + 300) + (4 + 1 + 1));
    let deadline = Instant::now() + Duration::from_secs(5);
    let pairs = |v: &Value| -> Vec<(String, String)> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|m| {
                (
                    m["role"].as_str().unwrap().to_string(),
                    m["content"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    };
    while app.tokens.uncached_bytes("m", &pairs(&convo)) > 0 {
        assert!(Instant::now() < deadline, "background fill");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The agent's next turn resends the conversation: now it's exact, from the cache.
    let mut next = convo.as_array().unwrap().clone();
    next.push(json!({ "role": "assistant", "content": "world" }));
    chat(&gw, &Value::Array(next)).await;
    let exact = (4 + 1 + 200) + (4 + 1 + 1) + (4 + 1 + 1);
    assert_eq!(seen.lock().unwrap()[2], exact);
    let t = sink.records()[2].tokens;
    assert_eq!(
        t.uncached_prefill + t.cached_prefill,
        exact,
        "the engine agrees (most of it from its prefix cache)"
    );
    let s = model_status(&gw).await;
    assert_eq!(s["estimated_messages"], 1);
    assert_eq!(s["exact_messages"], 1 + 1 + 3);
    // The fill taught the ratio for the next long message: 6 bytes per token.
    assert!((s["bytes_per_token"].as_f64().unwrap() - 6.0).abs() < 1e-9);
}

#[tokio::test]
async fn a_missing_tokenizer_file_is_a_config_error() {
    let mut c = config("http://127.0.0.1:1", 100);
    c.tokenization.tokenizers[0].path = "/nonexistent/tokenizer.json".into();
    let err = AppState::new(&c, Arc::new(MemorySink::default()))
        .err()
        .expect("fails");
    assert!(err.to_string().contains("tokenizer for m"), "{err}");
}

/// Tokens the fixture's word-level tokenizer makes of `text`: runs of word characters and
/// runs of other non-space characters.
fn word_level(text: &str) -> u64 {
    let mut n = 0;
    let mut prev: Option<bool> = None; // Some(is_word) inside a run
    for c in text.chars() {
        let class = if c.is_whitespace() {
            None
        } else {
            Some(c.is_alphanumeric() || c == '_')
        };
        if class.is_some() && class != prev {
            n += 1;
        }
        prev = class;
    }
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_chat_template_counts_tools_tool_calls_and_images() {
    let (engine, seen) = engine().await;
    let mut c = config(&engine, 100_000);
    let template = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../pt-tokenize/tests/fixtures/chatml.jinja"
    );
    c.tokenization.tokenizers[0].chat_template = Some(template.into());
    c.tokenization.tokenizers[0].image_tokens = 100;
    let app = AppState::new(&c, Arc::new(MemorySink::default())).unwrap();
    let gw = serve(router(app.clone())).await;

    let tools = json!([{ "type": "function", "function": {
        "name": "search", "description": "Search the code base.",
        "parameters": { "type": "object", "properties": { "q": { "type": "string" } } }
    } }]);
    let messages = json!([
        { "role": "user", "content": [
            { "type": "text", "text": "what is in this picture ?" },
            { "type": "image_url", "image_url": { "url": "data:image/png;base64,AAAA" } }
        ] },
        { "role": "assistant", "content": "", "tool_calls": [
            { "type": "function", "function": { "name": "search", "arguments": { "q": "picture" } } }
        ] },
        { "role": "tool", "content": "no results" },
    ]);
    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .bearer_auth(KEY)
        .json(&json!({ "model": "m", "max_tokens": 2, "messages": messages, "tools": tools }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "{body}");

    // What the engine sees: the whole rendered prompt, plus the image's own tokens.
    let t = pt_tokenize::ChatTemplate::load(std::path::Path::new(template)).unwrap();
    let prompt = t.render(&messages, Some(&tools)).unwrap();
    let expected = word_level(&prompt) + 100;
    let counted = seen.lock().unwrap()[0];
    // Text and template are tokenized apart, so a token can split where they meet ("?"
    // then "<image>" here): at most one per message.
    assert!(counted.abs_diff(expected) <= 3, "{counted} vs {expected}");
    // Far more than the text alone.
    assert!(expected > 60, "{expected}: {prompt}");

    let s = model_status(&gw).await;
    assert_eq!(s["chat_template"], true);
    assert_eq!(s["template_errors"], 0);
}

fn png_data_url(w: u32, h: u32) -> String {
    use base64::Engine;
    let mut b = b"\x89PNG\r\n\x1a\n\0\0\0\x0dIHDR".to_vec();
    b.extend(w.to_be_bytes());
    b.extend(h.to_be_bytes());
    b.extend([8, 2, 0, 0, 0]);
    format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(b)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_images_cost_what_their_size_needs() {
    let (engine, seen) = engine().await;
    let mut c = config(&engine, 100_000);
    c.tokenization.tokenizers[0].image = Some(pt_tokenize::ImageTiling {
        tile_px: 336,
        tokens_per_tile: 144,
        max_tiles: 16,
        base_tokens: 144,
    });
    let app = AppState::new(&c, Arc::new(MemorySink::default())).unwrap();
    let gw = serve(router(app)).await;
    for (w, h) in [(300, 300), (672, 336)] {
        chat(
            &gw,
            &json!([{ "role": "user", "content": [
                { "type": "text", "text": "hello" },
                { "type": "image_url", "image_url": { "url": png_data_url(w, h) } }
            ] }]),
        )
        .await;
    }
    let seen = seen.lock().unwrap().clone();
    // One tile and a thumbnail, then two tiles and a thumbnail (ADR-036).
    assert_eq!(seen[1] - seen[0], 144, "{seen:?}");
}
