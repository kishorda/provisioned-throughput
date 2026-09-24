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

    if let Some(source) = config.entitlements.clone() {
        let client = pt_gateway::sync::SnapshotClient::new(source)?;
        // Serve last-known-good entitlements immediately, then keep them fresh.
        match client.load_cache(&app) {
            Ok(Some(v)) => tracing::info!(version = v, "serving cached entitlements"),
            Ok(None) => {
                tracing::warn!("no cached entitlements; serving nothing until the first snapshot")
            }
            Err(e) => tracing::warn!(error = %e, "ignoring entitlement cache"),
        }
        tokio::spawn(client.run(app.clone()));
    }

    let listener = tokio::net::TcpListener::bind(&config.server.listen)
        .await
        .with_context(|| format!("binding {}", config.server.listen))?;
    tracing::info!(
        listen = %config.server.listen,
        engine = %config.server.engine_url,
        entitlements = if config.entitlements.is_some() { "snapshot" } else { "static" },
        "gateway listening"
    );
    axum::serve(listener, router(app))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
