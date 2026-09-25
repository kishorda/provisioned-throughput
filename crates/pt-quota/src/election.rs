//! Coordinator leadership (ADR-027), on the shared election in `pt-election`.
//!
//! Only the lease holder answers renewals. Taking over a released lease warms up
//! ([`crate::Coordinator::begin_term`]): gateways still hold the old leader's grants.

use std::future::Future;
use std::sync::Arc;

pub use pt_election::*;

use crate::Coordinator;

/// Run the election until `shutdown` resolves, then release the lease. On acquiring,
/// starts a new term on the coordinator.
pub async fn run<B: LeaseBackend>(
    elector: Elector<B>,
    coordinator: Arc<Coordinator>,
    leadership: Arc<Leadership>,
    shutdown: impl Future<Output = ()>,
) {
    let identity = elector.config().identity.clone();
    pt_election::run(
        elector,
        leadership,
        move |change, now| match change {
            Change::Acquired { warm_up } => {
                coordinator.begin_term(now, warm_up);
                tracing::info!(%identity, warm_up, "leading the quota coordinator");
            }
            Change::Resumed => tracing::info!("resumed leading the quota coordinator"),
            Change::Lost => tracing::warn!("lost quota coordinator leadership; standing by"),
            Change::None => {}
        },
        shutdown,
    )
    .await
}
