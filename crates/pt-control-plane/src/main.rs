//! Run the control-plane API: `pt-control-plane [config.toml]`
//! (default `config/control-plane.toml`). State is in memory and lost on restart.
//!
//! `pt-control-plane keygen` prints a new snapshot signing key and its public key.

use std::time::Duration;

use anyhow::Context;
use pt_control_plane::clock::SystemClock;
use pt_control_plane::{api, in_memory, ControlPlaneConfig};

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

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("binding {listen}"))?;
    tracing::info!(%listen, "control plane listening (in-memory store)");
    axum::serve(listener, api::router(svc))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
