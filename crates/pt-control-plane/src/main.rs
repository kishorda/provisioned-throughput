//! Run the control-plane API: `pt-control-plane [config.toml]`
//! (default `config/control-plane.toml`). State is in memory and lost on restart.
//!
//! `pt-control-plane keygen` prints a new snapshot signing key and its public key.

use std::time::Duration;

use anyhow::Context;
use pt_control_plane::clock::SystemClock;
use pt_control_plane::{app, in_memory, ControlPlaneConfig};
use pt_telemetry::UsageStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let arg = std::env::args().nth(1);
    if arg.as_deref() == Some("keygen") {
        let (seed, public) = pt_entitlement::SnapshotSigner::generate();
        println!("signing_key = \"{seed}\"   # control plane [entitlements]");
        println!("public_key  = \"{public}\"   # gateway [entitlements]");
        return Ok(());
    }
    let path = arg.unwrap_or_else(|| "config/control-plane.toml".into());
    let config = ControlPlaneConfig::load(&path)?;
    let listen = config.server.listen.clone();
    let interval = Duration::from_secs(config.server.lifecycle_interval_secs);
    let retention_ms = config.telemetry.retention_days * 86_400_000;
    let svc = in_memory(config, SystemClock);

    let lifecycle = svc.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            let r = lifecycle.run_lifecycle().await;
            if r != Default::default() {
                tracing::info!(?r, "lifecycle run");
            }
        }
    });

    let failover = svc.clone();
    let check = Duration::from_millis(failover.config.failover.check_interval_ms);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(check);
        loop {
            tick.tick().await;
            let r = failover.run_failover().await;
            for (id, region) in &r.declared {
                tracing::warn!(%id, %region, "region down: failover entitlements active");
            }
            for (id, region) in &r.resolved {
                tracing::info!(%id, %region, "region recovered: returning traffic gradually");
            }
        }
    });

    let (routes, telemetry) = app(svc.clone());
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(3_600));
        loop {
            tick.tick().await;
            let now = pt_telemetry::Directory::now_ms(&telemetry.directory);
            let removed = telemetry
                .store
                .prune(now.saturating_sub(retention_ms))
                .await;
            if removed > 0 {
                tracing::info!(removed, "pruned old usage records");
            }
        }
    });

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("binding {listen}"))?;
    tracing::info!(%listen, "control plane listening (in-memory store)");
    axum::serve(listener, routes)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
