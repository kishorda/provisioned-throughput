//! Customer-facing usage and SLA telemetry (docs/09).
//!
//! Gateways push usage records here. Customers query their own Provisioned Throughput:
//! - `usage`: a time series and summary of traffic, throttles, tokens, utilisation,
//!   latency percentiles, cache hit rate, and shape drift;
//! - `sla`: monthly attainment and service credits, computed with the published SLA rules;
//! - `sessions`: one agent session's calls.
//!
//! Reports are computed from the same records customers can see, so they can reproduce
//! them. The store is in-memory for now, behind [`store::UsageStore`].

pub mod api;
pub mod backend;
pub mod clickhouse;
pub mod directory;
pub mod sessions;
pub mod sla;
pub mod stats;
pub mod store;
pub mod usage;

pub use backend::UsageBackend;
pub use directory::{Directory, DirectoryError, ExclusionWindow, ReservationInfo};
pub use store::{MemoryUsageStore, UsageError, UsageStore};

/// Store plus directory: everything the API needs.
pub struct Telemetry<U, D> {
    pub store: U,
    pub directory: D,
}
