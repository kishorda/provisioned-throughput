//! Capacity feasibility (docs/06 §1). The service asks the planner before it commits capacity.
//!
//! [`MemoryPlanner`] tracks sellable CUs per (region, model) from configuration, for one
//! instance. [`crate::sql_planner::SqlPlanner`] keeps the same counters in the database, so
//! several control-plane instances share them (ADR-023). The real Capacity Planner places
//! CUs into pools per tier and plans headroom. The interface is the same.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use pt_core::Shape;

use crate::config::CapacityConfig;
use crate::model::RegionShare;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("{model} is not offered in {region}")]
    NotOffered { region: String, model: String },
    #[error("{region} has {available} CUs of {model} available; {requested} requested")]
    CapacityUnavailable {
        region: String,
        model: String,
        requested: u32,
        available: u32,
    },
    #[error("{region} can serve contexts up to {max_context} tokens for {model}; the shape needs {needed}")]
    ShapeUnsupported {
        region: String,
        model: String,
        max_context: u64,
        needed: u64,
    },
    /// The planner's store couldn't be reached. Nothing was reserved; retry.
    #[error("capacity planner unavailable: {0}")]
    Unavailable(String),
}

/// A pool whose reserved count doesn't match its live reservations.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Drift {
    pub region: String,
    pub model: String,
    pub reserved: u32,
    pub expected: u32,
    /// Set once the same drift was seen on two runs in a row and was corrected. A drift
    /// seen once may be a sale in flight (capacity reserved, reservation not yet saved).
    pub corrected: bool,
}

/// Pools whose drift was seen on the previous reconcile run, as (reserved, expected).
pub(crate) type PendingDrift = Mutex<HashMap<(String, String), (u32, u32)>>;

/// Decide what to do with one pool: `Some((drift, fix))` when it drifts, where `fix` means
/// the same drift was already seen last run.
pub(crate) fn judge_drift(
    pending: &PendingDrift,
    key: &(String, String),
    reserved: u32,
    expected: u32,
) -> Option<(Drift, bool)> {
    let mut pending = pending.lock().unwrap_or_else(|e| e.into_inner());
    if reserved == expected {
        pending.remove(key);
        return None;
    }
    let fix = pending.get(key) == Some(&(reserved, expected));
    if fix {
        pending.remove(key);
    } else {
        pending.insert(key.clone(), (reserved, expected));
    }
    Some((
        Drift {
            region: key.0.clone(),
            model: key.1.clone(),
            reserved,
            expected,
            corrected: fix,
        },
        fix,
    ))
}

pub trait CapacityPlanner: Send + Sync + 'static {
    /// Reserve `shares` of `model` for a workload of `shape`, all or nothing.
    fn reserve(
        &self,
        model: &str,
        shares: &[RegionShare],
        shape: &Shape,
    ) -> impl Future<Output = Result<(), PlanError>> + Send;

    /// Return capacity previously reserved.
    fn release(&self, model: &str, shares: &[RegionShare]) -> impl Future<Output = ()> + Send;

    /// Check that `regions` can serve `shape` without reserving anything.
    fn check_shape(
        &self,
        model: &str,
        regions: &[RegionShare],
        shape: &Shape,
    ) -> impl Future<Output = Result<(), PlanError>> + Send;

    /// Unreserved CUs of `model` in `region`, or `None` if it isn't offered there.
    fn available_cus(&self, region: &str, model: &str) -> impl Future<Output = Option<u32>> + Send;

    /// Re-establish capacity already sold (at startup, from the store). Never fails: if
    /// configured capacity has shrunk below what's sold, the pool is overcommitted and
    /// reports no availability, and new sales fail until it's fixed. A planner whose counts
    /// are already durable does nothing.
    fn restore(&self, model: &str, shares: &[RegionShare]) -> impl Future<Output = ()> + Send;

    /// Compare reserved counts with `expected` (live reservations' shares and headroom per
    /// (region, model)). A pool is corrected only when the same drift shows on two runs in a
    /// row, so a sale in flight is never undone. Returns every drift found.
    fn reconcile(
        &self,
        expected: &HashMap<(String, String), u32>,
    ) -> impl Future<Output = Result<Vec<Drift>, PlanError>> + Send;
}

#[derive(Debug)]
struct Pool {
    capacity: u32,
    reserved: u32,
    max_context: u64,
}

#[derive(Debug, Default)]
pub struct MemoryPlanner {
    pools: Mutex<HashMap<(String, String), Pool>>,
    pending: PendingDrift,
}

impl MemoryPlanner {
    pub fn new(capacity: &[CapacityConfig]) -> Self {
        let pools = capacity
            .iter()
            .map(|c| {
                (
                    (c.region.clone(), c.model.clone()),
                    Pool {
                        capacity: c.cus,
                        reserved: 0,
                        max_context: c.max_context,
                    },
                )
            })
            .collect();
        Self {
            pools: Mutex::new(pools),
            pending: Default::default(),
        }
    }

    /// Unreserved CUs of `model` in `region`.
    pub fn available(&self, region: &str, model: &str) -> Option<u32> {
        let pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        pools
            .get(&(region.to_string(), model.to_string()))
            .map(|p| p.capacity.saturating_sub(p.reserved))
    }

    fn check(
        pools: &HashMap<(String, String), Pool>,
        model: &str,
        shares: &[RegionShare],
        shape: &Shape,
        count_capacity: bool,
    ) -> Result<(), PlanError> {
        for s in shares {
            let Some(pool) = pools.get(&(s.region.clone(), model.to_string())) else {
                return Err(PlanError::NotOffered {
                    region: s.region.clone(),
                    model: model.into(),
                });
            };
            if shape.context_ceiling > pool.max_context {
                return Err(PlanError::ShapeUnsupported {
                    region: s.region.clone(),
                    model: model.into(),
                    max_context: pool.max_context,
                    needed: shape.context_ceiling,
                });
            }
            let available = pool.capacity.saturating_sub(pool.reserved);
            if count_capacity && s.cus > available {
                return Err(PlanError::CapacityUnavailable {
                    region: s.region.clone(),
                    model: model.into(),
                    requested: s.cus,
                    available,
                });
            }
        }
        Ok(())
    }
}

impl CapacityPlanner for MemoryPlanner {
    async fn reserve(
        &self,
        model: &str,
        shares: &[RegionShare],
        shape: &Shape,
    ) -> Result<(), PlanError> {
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        Self::check(&pools, model, shares, shape, true)?;
        for s in shares {
            if let Some(p) = pools.get_mut(&(s.region.clone(), model.to_string())) {
                p.reserved += s.cus;
            }
        }
        Ok(())
    }

    async fn release(&self, model: &str, shares: &[RegionShare]) {
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        for s in shares {
            if let Some(p) = pools.get_mut(&(s.region.clone(), model.to_string())) {
                p.reserved = p.reserved.saturating_sub(s.cus);
            }
        }
    }

    async fn check_shape(
        &self,
        model: &str,
        regions: &[RegionShare],
        shape: &Shape,
    ) -> Result<(), PlanError> {
        let pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        Self::check(&pools, model, regions, shape, false)
    }

    async fn available_cus(&self, region: &str, model: &str) -> Option<u32> {
        self.available(region, model)
    }

    async fn restore(&self, model: &str, shares: &[RegionShare]) {
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        for s in shares {
            match pools.get_mut(&(s.region.clone(), model.to_string())) {
                Some(p) => {
                    p.reserved += s.cus;
                    if p.reserved > p.capacity {
                        tracing::error!(region = %s.region, %model, reserved = p.reserved, capacity = p.capacity, "sold capacity exceeds configured capacity");
                    }
                }
                None => {
                    tracing::error!(region = %s.region, %model, "sold capacity in a pool that's no longer configured")
                }
            }
        }
    }

    async fn reconcile(
        &self,
        expected: &HashMap<(String, String), u32>,
    ) -> Result<Vec<Drift>, PlanError> {
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::new();
        for (key, pool) in pools.iter_mut() {
            let want = expected.get(key).copied().unwrap_or(0);
            if let Some((drift, fix)) = judge_drift(&self.pending, key, pool.reserved, want) {
                if fix {
                    pool.reserved = want;
                }
                out.push(drift);
            }
        }
        Ok(out)
    }
}
