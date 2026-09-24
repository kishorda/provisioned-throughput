//! Run the regional Quota Coordinator: `pt-quota-coordinator [config.toml]`
//! (default `config/quota.toml`). One instance per region (ADR-012).

use std::sync::Arc;

use anyhow::Context;
use pt_quota::config::QuotaConfig;
use pt_quota::{api, Coordinator};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/quota.toml".into());
    let config = QuotaConfig::load(&path).with_context(|| format!("loading {path}"))?;
    let coordinator = Arc::new(Coordinator::new(config.coordinator()));
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    tracing::info!(listen = %config.listen, lease_ttl_ms = config.lease_ttl_ms, "quota coordinator listening");
    axum::serve(listener, api::router(coordinator, &config.token))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
