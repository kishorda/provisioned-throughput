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
    let mut sinks: Vec<Arc<dyn UsageSink>> = Vec::new();
    if let Some(p) = &config.server.usage_log {
        sinks.push(Arc::new(
            JsonlSink::file(p).with_context(|| format!("opening usage log {p}"))?,
        ));
    }
    if let Some(export) = config.usage_export.clone() {
        sinks.push(pt_gateway::usage::HttpSink::start(export));
    }
    if sinks.is_empty() {
        sinks.push(Arc::new(JsonlSink::stdout()));
    }
    let sink: Arc<dyn UsageSink> = Arc::new(pt_gateway::usage::TeeSink(sinks));
    let app = AppState::new(&config, sink)?;

    if let Some(source) = config.entitlements.clone() {
        let client = pt_gateway::sync::SnapshotClient::new(source.clone())?;
        // Serve last-known-good entitlements immediately, then keep them fresh.
        match client.load_cache(&app) {
            Ok(Some(v)) => tracing::info!(version = v, "serving cached entitlements"),
            Ok(None) => {
                tracing::warn!("no cached entitlements; serving nothing until the first snapshot")
            }
            Err(e) => tracing::warn!(error = %e, "ignoring entitlement cache"),
        }
        if source.heartbeat_interval_ms > 0 {
            let id = config
                .quota
                .as_ref()
                .and_then(|q| q.gateway_id.clone())
                .unwrap_or_else(|| format!("gw-{}", uuid::Uuid::new_v4().simple()));
            let heartbeat = pt_gateway::health::HeartbeatClient::new(source.clone(), id)?;
            tokio::spawn(heartbeat.run(app.clone()));
        }
        tokio::spawn(client.run(app.clone()));
        tokio::spawn(pt_gateway::health::run_rate_refresh(
            app.clone(),
            std::time::Duration::from_secs(1),
        ));
    }
    if let Some(quota) = config.quota.clone() {
        let client = pt_gateway::quota::QuotaClient::new(quota)?;
        tracing::info!(
            gateway_id = client.gateway_id(),
            "sharing entitlements through the quota coordinator"
        );
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
