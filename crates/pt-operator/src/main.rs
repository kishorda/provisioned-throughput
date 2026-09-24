//! Run the Regional Capacity Controller against the current kubeconfig or in-cluster
//! service account.
//!
//! To load warm spares for region failovers, set `PT_CONTROL_PLANE_URL`, `PT_REGION`,
//! `PT_REGION_TOKEN`, and `PT_SNAPSHOT_PUBLIC_KEY` (and optionally `PT_SNAPSHOT_CACHE`), so
//! the controller follows the region's entitlement snapshot.

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
    tracing::info!("pt-operator starting");
    pt_operator::controller::run(client, snapshot).await
}
