//! Run the control-plane API: `pt-control-plane [config.toml]`
//! (default `config/control-plane.toml`).
//!
//! With a `[store]` URL (or `PT_DATABASE_URL`), state lives in CockroachDB or PostgreSQL:
//! migrations are applied and reserved capacity is rebuilt at startup. Without one, state
//! is in memory and lost on restart.
//!
//! `pt-control-plane keygen` prints a new snapshot signing key and its public key.

use std::sync::Arc;

use anyhow::Context;
use pt_control_plane::clock::SystemClock;
use pt_control_plane::planner::CapacityPlanner;
use pt_control_plane::sql::SqlStore;
use pt_control_plane::store::Store;
use pt_control_plane::{
    app_with_usage, in_memory, restrict, with_sql, ControlPlaneConfig, Service, Surface,
};
use pt_telemetry::clickhouse::ClickHouseUsageStore;
use pt_telemetry::UsageBackend;

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
    // Fail here, with a message, if the Vault token is missing (ADR-038).
    pt_control_plane::signing::Signer::from_config(&config)
        .map_err(anyhow::Error::msg)
        .context("configuring the snapshot signer")?;
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
            let svc = with_sql(config, store, SystemClock)
                .await
                .context("starting on the shared store")?;
            serve(svc, "sql").await
        }
        None => {
            tracing::warn!("no [store] configured: state is in memory and lost on restart");
            serve(in_memory(config, SystemClock), "in-memory").await
        }
    }
}

/// Run the background loops and the HTTP API until Ctrl-C.
async fn serve<S: Store, P: CapacityPlanner>(
    svc: Arc<Service<S, P, SystemClock>>,
    store_kind: &str,
) -> anyhow::Result<()> {
    let listen = svc.config.server.listen.clone();
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

    // The version poller runs everywhere; lifecycle, failover, reconciliation, invoices,
    // and pruning run only on the instance holding the leader lease (ADR-023).
    let holder = format!("cp-{}", uuid::Uuid::new_v4().simple());
    tracing::info!(%holder, "instance id");
    tokio::spawn(pt_control_plane::background::run(
        svc.clone(),
        telemetry,
        holder,
    ));

    let server = svc.config.server.clone();
    match &server.internal {
        None => serve_on(&listen, server.tls.as_ref(), routes, store_kind, "all").await,
        Some(internal) => {
            // Customer API and region traffic on separate listeners (ADR-034).
            let customer = restrict(routes.clone(), Surface::Customer);
            let region = restrict(routes, Surface::Internal);
            tokio::try_join!(
                serve_on(
                    &listen,
                    server.tls.as_ref(),
                    customer,
                    store_kind,
                    "customer"
                ),
                serve_on(
                    &internal.listen,
                    internal.tls.as_ref(),
                    region,
                    store_kind,
                    "internal"
                ),
            )?;
            Ok(())
        }
    }
}

/// Serve `routes` on `listen`, over TLS if `tls` is set, until Ctrl-C.
async fn serve_on(
    listen: &str,
    tls: Option<&pt_control_plane::tls::ServerTlsConfig>,
    routes: axum::Router,
    store_kind: &str,
    surface: &str,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding {listen}"))?;
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    match tls {
        Some(tls) => {
            let config = pt_control_plane::tls::server_config(tls)
                .map_err(anyhow::Error::msg)
                .with_context(|| format!("loading TLS for the {surface} listener"))?;
            let mtls = tls.client_ca.is_some();
            tracing::info!(%listen, store = store_kind, surface, tls = true, client_certificates = mtls, "control plane listening");
            let listener = pt_control_plane::tls::TlsListener::new(listener, config)?;
            axum::serve(listener, routes)
                .with_graceful_shutdown(shutdown)
                .await?;
        }
        None => {
            tracing::info!(%listen, store = store_kind, surface, tls = false, "control plane listening");
            axum::serve(listener, routes)
                .with_graceful_shutdown(shutdown)
                .await?;
        }
    }
    Ok(())
}
