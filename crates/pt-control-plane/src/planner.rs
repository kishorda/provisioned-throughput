//! Capacity feasibility (docs/06 §1). The service asks the planner before it commits capacity.
//!
//! [`MemoryPlanner`] tracks sellable CUs per (region, model) from configuration. The real
//! Capacity Planner places CUs into pools per tier and plans headroom. The interface is the same.

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
        }
    }

    /// Unreserved CUs of `model` in `region`.
    pub fn available(&self, region: &str, model: &str) -> Option<u32> {
        let pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        pools
            .get(&(region.to_string(), model.to_string()))
            .map(|p| p.capacity - p.reserved)
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
            let available = pool.capacity - pool.reserved;
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
}
