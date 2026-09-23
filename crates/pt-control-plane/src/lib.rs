//! Global control plane: the customer API for Provisioned Throughput (docs/12).
//!
//! Customers create, update, and delete Provisioned Throughput for a model. Each resource
//! is a capacity reservation plus one data-plane deployment with an inference API key.

pub mod api;
pub mod clock;
pub mod config;
pub mod model;
pub mod planner;
pub mod pricing;
pub mod service;
pub mod store;
pub mod validate;

use std::sync::Arc;

pub use config::ControlPlaneConfig;
pub use service::Service;

use clock::Clock;
use planner::MemoryPlanner;
use store::MemoryStore;

/// A service backed by the in-memory store and planner.
pub fn in_memory<C: Clock>(
    config: ControlPlaneConfig,
    clock: C,
) -> Arc<Service<MemoryStore, MemoryPlanner, C>> {
    let planner = MemoryPlanner::new(&config.capacity);
    Arc::new(Service::new(MemoryStore::default(), planner, clock, config))
}
