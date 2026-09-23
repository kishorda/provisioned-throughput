//! Run the gateway: `pt-gateway [config.toml]` (default `config/gateway.toml`).

use std::sync::Arc;

use anyhow::Context;
use pt_gateway::usage::{JsonlSink, UsageSink};
use pt_gateway::{router, AppState, GatewayConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/gateway.toml".into());
    let config = GatewayConfig::load(&path)?;
    let sink: Arc<dyn UsageSink> = match &config.server.usage_log {
        Some(p) => Arc::new(JsonlSink::file(p).with_context(|| format!("opening usage log {p}"))?),
        None => Arc::new(JsonlSink::stdout()),
    };
    let app = AppState::new(&config, sink)?;

    let listener = tokio::net::TcpListener::bind(&config.server.listen)
        .await
        .with_context(|| format!("binding {}", config.server.listen))?;
    tracing::info!(
        listen = %config.server.listen,
        engine = %config.server.engine_url,
        deployments = config.deployments.len(),
        "gateway listening"
    );
    axum::serve(listener, router(app))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
