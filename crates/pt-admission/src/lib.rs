//! Admission control for Provisioned Throughput.
//!
//! Implements docs/04 §3–4 and ADR-002: admit on a WU estimate against a debt-based token
//! bucket, fall through the reservation's boundary policies (burst → queue → spillover →
//! reject), and settle the bucket with actual WU when the request completes.
//!
//! All time-dependent methods take `now: Instant` explicitly so behaviour is deterministic
//! under test.

pub mod bucket;
pub mod burst;
pub mod estimator;
pub mod limiter;
pub mod policy;
pub mod prefix;

pub use bucket::DebtBucket;
pub use burst::BurstBank;
pub use estimator::{OutputEstimator, QuantileSketch};
pub use limiter::{
    AdmitRequest, AdmitTicket, Decision, LimiterConfig, LimiterStatus, QueueSlot,
    ReservationLimiter,
};
pub use policy::{BoundaryPolicy, BurstPolicy, QueuePolicy};
pub use prefix::{Prediction, PrefixCache, PrefixCacheConfig, PrefixKeys};
