//! Run the control-plane API: `pt-control-plane [config.toml]`
//! (default `config/control-plane.toml`).
//!
//! With a `[store]` URL (or `PT_DATABASE_URL`), state lives in CockroachDB or PostgreSQL:
//! migrations are applied and reserved capacity is rebuilt at startup. Without one, state
//! is in memory and lost on restart.
//!
//! `pt-control-plane keygen` prints a new snapshot signing key and its public key.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use pt_control_plane::clock::SystemClock;
use pt_control_plane::planner::MemoryPlanner;
use pt_control_plane::sql::SqlStore;
use pt_control_plane::store::Store;
use pt_control_plane::{app_with_usage, in_memory, with_store, ControlPlaneConfig, Service};
use pt_telemetry::clickhouse::ClickHouseUsageStore;
use pt_telemetry::UsageBackend;
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
        let id = pt_entitlement::SnapshotSigner::from_hex(&seed)?.key_id();
        println!("# key id {id}. To rotate (docs/12 §6): add public_key to the verifiers'");
        println!("# extra_public_keys, then switch signing_key, then remove the old key.");
        println!("signing_key = \"{seed}\"   # control plane [entitlements]");
        println!(
            "public_key  = \"{public}\"   # gateway [entitlements] and PT_SNAPSHOT_PUBLIC_KEY"
        );
        return Ok(());
    }
    let path = arg.unwrap_or_else(|| "config/control-plane.toml".into());
    let config = ControlPlaneConfig::load(&path)?;
    match config.store.as_ref().and_then(|s| s.resolved_url()) {
        Some(url) => {
            let store_cfg = config.store.clone().expect("checked above");
            let store = SqlStore::connect_checked(
                &url,
                store_cfg.max_connections,
                store_cfg.allow_insecure_transport,
            )
            .await
            .context("connecting to the control-plane database")?;
            if store_cfg.migrate {
                store.migrate().await.context("applying migrations")?;
            }
            let svc = with_store(config, store, SystemClock)
                .await
                .context("restoring reserved capacity")?;
            serve(svc, "sql").await
        }
        None => {
            tracing::warn!("no [store] configured: state is in memory and lost on restart");
            serve(in_memory(config, SystemClock), "in-memory").await
        }
    }
}

/// Run the background loops and the HTTP API until Ctrl-C.
async fn serve<S: Store>(
    svc: Arc<Service<S, MemoryPlanner, SystemClock>>,
    store_kind: &str,
) -> anyhow::Result<()> {
    let listen = svc.config.server.listen.clone();
    let interval = Duration::from_secs(svc.config.server.lifecycle_interval_secs);
    let retention_ms = svc.config.telemetry.retention_days * 86_400_000;

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

    let usage = match svc.config.telemetry.resolved_clickhouse() {
        Some(ch) => {
            let store = ClickHouseUsageStore::new(ch, svc.config.telemetry.retention_days)
                .context("configuring ClickHouse")?;
            store
                .migrate()
                .await
                .context("creating the ClickHouse usage table")?;
            UsageBackend::ClickHouse(store)
        }
        None => {
            tracing::warn!(
                "no [telemetry.clickhouse] configured: usage is in memory and lost on restart"
            );
            UsageBackend::default()
        }
    };
    tracing::info!(usage = usage.kind(), "usage store");
    let (routes, telemetry) = app_with_usage(svc.clone(), usage);

    // Finalise last month's invoices once its grace period has passed (ADR-018).
    let (billing_svc, billing_tel) = (svc.clone(), telemetry.clone());
    let every = Duration::from_secs(svc.config.billing.finalize_interval_secs);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        loop {
            tick.tick().await;
            match pt_control_plane::billing::finalize_due(&billing_svc, &billing_tel).await {
                Ok(done) if !done.is_empty() => {
                    tracing::info!(invoices = done.len(), "finalised invoices")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "invoice finalisation failed; retrying later"),
            }
        }
    });
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(3_600));
        loop {
            tick.tick().await;
            let now = pt_telemetry::Directory::now_ms(&telemetry.directory);
            match telemetry
                .store
                .prune(now.saturating_sub(retention_ms))
                .await
            {
                Ok(0) => {}
                Ok(removed) => tracing::info!(removed, "pruned old usage records"),
                Err(e) => tracing::warn!(error = %e, "usage pruning failed; retrying next hour"),
            }
        }
    });

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("binding {listen}"))?;
    tracing::info!(%listen, store = store_kind, "control plane listening");
    axum::serve(listener, routes)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
