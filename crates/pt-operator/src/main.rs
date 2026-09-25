//! Run the Regional Capacity Controller against the current kubeconfig or in-cluster
//! service account.
//!
//! To load warm spares for region failovers, set `PT_CONTROL_PLANE_URL`, `PT_REGION`,
//! `PT_REGION_TOKEN`, and `PT_SNAPSHOT_PUBLIC_KEY` (and optionally `PT_SNAPSHOT_CACHE`), so
//! the controller follows the region's entitlement snapshot.
//!
//! Replicas elect a leader through a Kubernetes Lease (`leader.rs`, ADR-029); only the
//! leader reconciles. Set `PT_LEADER_ELECTION=false` to run one replica without it.

use pt_election::kube_lease::KubeLease;
use pt_election::{lead, terminated, Elector, Ended};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let client = kube::Client::try_default().await?;
    let snapshot = match pt_operator::failover::SnapshotSource::from_env() {
        Some(source) => {
            let region = source.region.clone();
            let (follower, rx) = pt_operator::failover::SnapshotFollower::new(source)?;
            if follower.load_cache() {
                tracing::info!("loaded cached entitlement snapshot");
            }
            tokio::spawn(follower.run());
            tracing::info!(%region, "following entitlement snapshots for failover demand");
            Some(rx)
        }
        None => {
            tracing::warn!("no snapshot source configured; warm spares won't load on failover");
            None
        }
    };
    let election = pt_operator::leader::from_env(|k| std::env::var(k).ok())?;
    let Some(lease) = election else {
        tracing::warn!("leader election is off: run only one replica");
        tracing::info!("pt-operator starting");
        return pt_operator::controller::run(client, snapshot).await;
    };
    tracing::info!(identity = %lease.election.identity, lease = %lease.name, "standing by for leadership");
    let backend = KubeLease::new(
        client.clone(),
        &lease.namespace,
        &lease.name,
        lease.election.lease_duration,
    );
    let work = move || async move {
        tracing::info!("pt-operator starting");
        if let Err(e) = pt_operator::controller::run(client, snapshot).await {
            tracing::error!(error = %e, "controller stopped");
        }
    };
    match lead(Elector::new(backend, lease.election), terminated(), work).await {
        // Exit, so the pod restarts as a standby with fresh caches.
        Ended::Lost => anyhow::bail!("lost leadership"),
        Ended::Finished | Ended::Shutdown => Ok(()),
    }
}
