//! Quota Coordinator (docs/04 §6, ADR-003, ADR-012).
//!
//! Each reservation's regional entitlement is split across the gateway replicas that serve
//! it. Gateways renew leases every ~250 ms, reporting demand. The coordinator grants each a
//! rate so that the sum of unexpired grants never exceeds the entitlement. Gateways admit
//! locally against their lease, so the coordinator is never on the request path.
//!
//! State is soft: after a restart, it's rebuilt from renewals within one lease period.
//! Replicas run active/standby: only the holder of the election lease answers renewals
//! ([`election`], ADR-027).

pub mod allocator;
pub mod api;
pub mod config;
pub mod coordinator;
pub mod election;
pub mod wire;

pub use coordinator::{Coordinator, CoordinatorConfig};
