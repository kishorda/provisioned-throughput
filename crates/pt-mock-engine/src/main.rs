//! Run the mock engine.
//!
//! Configured with environment variables:
//! `MOCK_ADDR` (default `127.0.0.1:9000`), `MOCK_NAME`, `MOCK_TTFT_MS`, `MOCK_TPOT_MS`,
//! `MOCK_OUTPUT_TOKENS`.

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
    };
    let addr: String = env_or("MOCK_ADDR", "127.0.0.1:9000".to_string())?;
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, ?config, "mock engine listening");
    MockEngine::new(config).serve(listener).await
}
