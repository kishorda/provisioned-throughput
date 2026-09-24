//! Global control plane: the customer API for Provisioned Throughput (docs/12).
//!
//! Customers create, update, and delete Provisioned Throughput for a model. Each resource
//! is a capacity reservation plus one data-plane deployment with an inference API key.

pub mod api;
pub mod clock;
pub mod config;
pub mod failover;
pub mod model;
pub mod planner;
pub mod pricing;
pub mod quote;
pub mod quote_api;
pub mod service;
pub mod sql;
pub mod store;
pub mod telemetry;
pub mod validate;

use std::sync::Arc;

pub use config::ControlPlaneConfig;
pub use service::Service;

use clock::Clock;
use planner::MemoryPlanner;
use store::MemoryStore;

/// The full control-plane HTTP app: the customer API, quotes, entitlement snapshots, and
/// telemetry.
pub fn app<S: store::Store, P: planner::CapacityPlanner, C: Clock>(
    svc: Arc<Service<S, P, C>>,
) -> (axum::Router, Arc<telemetry::CpTelemetry<S, P, C>>) {
    let (telemetry_routes, tel) = telemetry::telemetry(svc.clone());
    let quotes = quote_api::router(svc.clone(), tel.clone());
    (api::router(svc).merge(telemetry_routes).merge(quotes), tel)
}

/// A service backed by the in-memory store and planner.
pub fn in_memory<C: Clock>(
    config: ControlPlaneConfig,
    clock: C,
) -> Arc<Service<MemoryStore, MemoryPlanner, C>> {
    let planner = MemoryPlanner::new(&config.capacity);
    Arc::new(Service::new(MemoryStore::default(), planner, clock, config))
}

/// A service over `store` with the in-memory planner, its reserved capacity rebuilt from the
/// store's live reservations.
pub async fn with_store<S: store::Store, C: Clock>(
    config: ControlPlaneConfig,
    store: S,
    clock: C,
) -> Result<Arc<Service<S, MemoryPlanner, C>>, store::StoreError> {
    let planner = MemoryPlanner::new(&config.capacity);
    let svc = Arc::new(Service::new(store, planner, clock, config));
    svc.restore_capacity().await?;
    Ok(svc)
}
