//! Run the tenant-aware router: `pt-router [config.toml]` (default `config/router.toml`).

use anyhow::Context;
use pt_router::config::RouterConfig;
use pt_router::http;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/router.toml".into());
    let config = RouterConfig::load(&path).with_context(|| format!("loading {path}"))?;
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    tracing::info!(listen = %config.listen, workers = config.workers.len(), "router listening");
    axum::serve(listener, http::router(http::Shared::new(&config)))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
