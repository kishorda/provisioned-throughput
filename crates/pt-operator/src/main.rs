//! Run the Regional Capacity Controller against the current kubeconfig or in-cluster
//! service account.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let client = kube::Client::try_default().await?;
    tracing::info!("pt-operator starting");
    pt_operator::controller::run(client).await
}
