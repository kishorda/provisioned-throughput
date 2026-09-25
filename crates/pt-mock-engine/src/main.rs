//! Run the mock engine.
//!
//! Configured with environment variables:
//! `MOCK_ADDR` (default `127.0.0.1:9000`), `MOCK_NAME`, `MOCK_TTFT_MS`, `MOCK_TPOT_MS`,
//! `MOCK_OUTPUT_TOKENS`, `MOCK_CONTENTION=1` for the continuous-batching model, and
//! `MOCK_TOKENIZER=path/to/tokenizer.json` to count prompt tokens with a real tokenizer
//! (for any model; `MOCK_MESSAGE_OVERHEAD`, default 3, per message).

use std::time::Duration;

use anyhow::Context;
use pt_mock_engine::{MockConfig, MockEngine};

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(key) {
        Ok(v) => v.parse().with_context(|| format!("invalid {key}={v}")),
        Err(_) => Ok(default),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let defaults = MockConfig::default();
    let config = MockConfig {
        name: env_or("MOCK_NAME", defaults.name)?,
        ttft: Duration::from_millis(env_or("MOCK_TTFT_MS", defaults.ttft.as_millis() as u64)?),
        tpot: Duration::from_millis(env_or("MOCK_TPOT_MS", defaults.tpot.as_millis() as u64)?),
        default_output_tokens: env_or("MOCK_OUTPUT_TOKENS", defaults.default_output_tokens)?,
        // MOCK_CONTENTION=1 simulates a continuous-batching engine (docs/05 §7).
        contention: env_or("MOCK_CONTENTION", 0u8)?
            .eq(&1)
            .then(pt_mock_engine::contention::Contention::default),
    };
    let addr: String = env_or("MOCK_ADDR", "127.0.0.1:9000".to_string())?;
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, ?config, "mock engine listening");
    let mut engine = MockEngine::new(config);
    if let Ok(path) = std::env::var("MOCK_TOKENIZER") {
        let spec = pt_tokenize::TokenizerSpec {
            model: pt_tokenize::ANY_MODEL.into(),
            path: path.into(),
            message_overhead: env_or("MOCK_MESSAGE_OVERHEAD", pt_tokenize::MESSAGE_OVERHEAD)?,
        };
        let tokens = pt_tokenize::Tokenizers::load(&[spec], 100_000)?;
        engine = engine.with_tokenizers(std::sync::Arc::new(tokens));
    }
    engine.serve(listener).await
}
