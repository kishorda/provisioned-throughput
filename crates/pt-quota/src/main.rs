//! Run the regional Quota Coordinator: `pt-quota-coordinator [config.toml]`
//! (default `config/quota.toml`). Without `[election]`, it's the region's only coordinator
//! (ADR-012). With it, replicas run active/standby on a Kubernetes Lease (ADR-027).

use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use pt_quota::config::QuotaConfig;
use pt_quota::election::Leadership;
use pt_quota::{api, Coordinator};
use tokio::sync::watch;

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
    let (stop_tx, stop_rx) = watch::channel(false);

    let (leadership, election) = match &config.election {
        None => {
            // A restart: gateways may still hold grants from before, so warm up.
            coordinator.begin_term(Instant::now(), true);
            (Leadership::always(), None)
        }
        Some(settings) => {
            let leadership = Leadership::elected();
            let task = elect(settings, &coordinator, &leadership, stop_rx.clone()).await?;
            (leadership, Some(task))
        }
    };

    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    tracing::info!(listen = %config.listen, lease_ttl_ms = config.lease_ttl_ms, elected = config.election.is_some(), "quota coordinator listening");
    tokio::spawn(async move {
        pt_quota::election::terminated().await;
        tracing::info!("shutting down");
        let _ = stop_tx.send(true);
    });
    let mut stop = stop_rx;
    axum::serve(
        listener,
        api::router(coordinator, leadership, &config.token),
    )
    .with_graceful_shutdown(async move {
        let _ = stop.wait_for(|s| *s).await;
    })
    .await?;
    // The election task releases the lease on shutdown, so a standby takes over at once.
    if let Some(task) = election {
        let _ = task.await;
    }
    Ok(())
}

#[cfg(feature = "kube")]
async fn elect(
    settings: &pt_quota::config::ElectionSettings,
    coordinator: &Arc<Coordinator>,
    leadership: &Arc<Leadership>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    use pt_quota::election::{kube_lease::KubeLease, run, Elector};
    let config = settings.config()?;
    let backend = KubeLease::connect(
        &settings.namespace,
        &settings.lease_name,
        config.lease_duration,
    )
    .await
    .context("connecting to the Kubernetes API for leader election")?;
    tracing::info!(identity = %config.identity, lease = %settings.lease_name, "joining the coordinator election");
    let elector = Elector::new(backend, config);
    Ok(tokio::spawn(run(
        elector,
        coordinator.clone(),
        leadership.clone(),
        async move {
            let _ = stop.wait_for(|s| *s).await;
        },
    )))
}

#[cfg(not(feature = "kube"))]
async fn elect(
    _: &pt_quota::config::ElectionSettings,
    _: &Arc<Coordinator>,
    _: &Arc<Leadership>,
    _: watch::Receiver<bool>,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    anyhow::bail!("[election] needs the `kube` feature")
}
