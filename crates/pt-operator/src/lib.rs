//! Regional Capacity Controller (docs/03 §2.2, docs/06, docs/08).
//!
//! For each `ModelPool` it:
//! 1. sums the pool's `PoolAllocation`s and sizes the provisioned floor from the
//!    `PerformanceProfile` (docs/06 §2);
//! 2. applies a `DynamoGraphDeployment` with floor + failure headroom + maintenance slots +
//!    hot spares;
//! 3. applies a PodDisruptionBudget per role that never lets voluntary disruption go below
//!    floor + failure headroom (docs/06 §6);
//! 4. reports sizing and conditions in `ModelPool.status`. It also sends them to the control
//!    plane for the system dashboard ([`report`], ADR-043).
//!
//! With a snapshot source configured, it also follows the region's entitlement snapshot
//! and loads warm spares for active failover demand (docs/07 §4, [`failover`]).

pub mod controller;
pub mod drain;
pub mod failover;
pub mod holds;
pub mod leader;
pub mod plan;
pub mod render;
pub mod report;
pub mod sizing;
