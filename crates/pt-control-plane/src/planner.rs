//! Capacity feasibility (docs/06 §1). The service asks the planner before it commits capacity.
//!
//! Each (region, model) pool holds a number of replicas, counted in micro-replicas. A CU
//! draws a different amount at each tier ([`crate::capacity::Costs`], ADR-031), so every
//! call names the reservation's tier. [`MemoryPlanner`] keeps the counters for one
//! instance. [`crate::sql_planner::SqlPlanner`] keeps them in the database, so several
//! control-plane instances share them (ADR-023).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use pt_core::{Shape, Tier};

use jiff::Timestamp;

use crate::capacity::{Costs, Schedule, MICRO};
use crate::config::CapacityConfig;
use crate::model::RegionShare;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("{model} is not offered in {region}")]
    NotOffered { region: String, model: String },
    #[error(
        "{region} has {available} {tier} CUs of {model} available; {requested} requested{}",
        from_note(.available_from)
    )]
    CapacityUnavailable {
        region: String,
        model: String,
        tier: Tier,
        requested: u32,
        available: u32,
        /// The earliest start at which scheduled capacity would fit the request (ADR-037).
        available_from: Option<Timestamp>,
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

fn from_note(from: &Option<Timestamp>) -> String {
    match from {
        Some(t) => format!(". It fits from {t}, when scheduled capacity arrives"),
        None => String::new(),
    }
}

/// The earliest date after `at` when a pool with `free` micro-replicas at `at` can take
/// `need` more, as scheduled capacity arrives.
pub(crate) fn fits_from(
    schedule: &Schedule,
    region: &str,
    model: &str,
    at: Timestamp,
    free: u64,
    need: u64,
) -> Option<Timestamp> {
    let now_added = schedule.added_by(region, model, at);
    schedule
        .dates_after(region, model, at)
        .into_iter()
        .find(|d| free + (schedule.added_by(region, model, *d) - now_added) >= need)
}

/// A pool whose reserved micro-replicas don't match its live reservations.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Drift {
    pub region: String,
    pub model: String,
    /// Micro-replicas.
    pub reserved: u64,
    pub expected: u64,
    /// Set once the same drift was seen on two runs in a row and was corrected. A drift
    /// seen once may be a sale in flight (capacity reserved, reservation not yet saved).
    pub corrected: bool,
}

/// What one live reservation holds, for restoring counters at startup.
#[derive(Debug, Clone, PartialEq)]
pub struct Held {
    pub model: String,
    pub tier: Tier,
    pub shares: Vec<RegionShare>,
}

/// Pools whose drift was seen on the previous reconcile run, as (reserved, expected).
pub(crate) type PendingDrift = Mutex<HashMap<(String, String), (u64, u64)>>;

/// Decide what to do with one pool: `Some((drift, fix))` when it drifts, where `fix` means
/// the same drift was already seen last run.
pub(crate) fn judge_drift(
    pending: &PendingDrift,
    key: &(String, String),
    reserved: u64,
    expected: u64,
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

/// Micro-replicas every live reservation holds, per (region, model).
pub fn expected_micro(costs: &Costs, held: &[Held]) -> HashMap<(String, String), u64> {
    let mut out: HashMap<(String, String), u64> = HashMap::new();
    for h in held {
        for s in &h.shares {
            if let Some(m) = costs.share(&h.model, h.tier, s) {
                *out.entry((s.region.clone(), h.model.clone())).or_default() += m;
            }
        }
    }
    out
}

pub trait CapacityPlanner: Send + Sync + 'static {
    /// Reserve `shares` of `model` at `tier` for a workload of `shape` starting at `at`, all
    /// or nothing. Capacity scheduled to arrive by `at` counts (ADR-037).
    fn reserve(
        &self,
        model: &str,
        tier: Tier,
        shares: &[RegionShare],
        shape: &Shape,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), PlanError>> + Send;

    /// Return capacity previously reserved at `tier`.
    fn release(
        &self,
        model: &str,
        tier: Tier,
        shares: &[RegionShare],
    ) -> impl Future<Output = ()> + Send;

    /// Check that `regions` can serve `shape` without reserving anything.
    fn check_shape(
        &self,
        model: &str,
        regions: &[RegionShare],
        shape: &Shape,
    ) -> impl Future<Output = Result<(), PlanError>> + Send;

    /// Unreserved CUs of `model` at `tier` in `region` for a start at `at`, or `None` if it
    /// isn't offered there.
    fn available_cus(
        &self,
        region: &str,
        model: &str,
        tier: Tier,
        at: Timestamp,
    ) -> impl Future<Output = Option<u32>> + Send;

    /// A pool's configured capacity and reserved amount in micro-replicas, for the
    /// dashboard (no scheduled arrivals). `None` if the pool doesn't exist or can't be read.
    fn pool_micro(
        &self,
        region: &str,
        model: &str,
    ) -> impl Future<Output = Option<(u64, u64)>> + Send;

    /// Re-establish capacity already sold, from every live reservation (at startup). Never
    /// fails: a pool whose configured replicas have shrunk below what's sold is
    /// overcommitted, reports no availability, and new sales fail until it's fixed. A
    /// planner whose counts are durable only recomputes pools that have never been counted
    /// in replicas (after the upgrade to ADR-031).
    fn restore(&self, live: &[Held]) -> impl Future<Output = ()> + Send;

    /// Compare reserved micro-replicas with `expected` per (region, model). A pool is
    /// corrected only when the same drift shows on two runs in a row, so a sale in flight is
    /// never undone. Returns every drift found.
    fn reconcile(
        &self,
        expected: &HashMap<(String, String), u64>,
    ) -> impl Future<Output = Result<Vec<Drift>, PlanError>> + Send;
}

#[derive(Debug)]
struct Pool {
    /// Micro-replicas.
    capacity: u64,
    reserved: u64,
    max_context: u64,
}

#[derive(Debug, Default)]
pub struct MemoryPlanner {
    pools: Mutex<HashMap<(String, String), Pool>>,
    costs: Costs,
    schedule: Schedule,
    pending: PendingDrift,
}

impl MemoryPlanner {
    pub fn new(capacity: &[CapacityConfig], costs: Costs, schedule: Schedule) -> Self {
        let pools = capacity
            .iter()
            .map(|c| {
                (
                    (c.region.clone(), c.model.clone()),
                    Pool {
                        capacity: u64::from(c.replicas) * MICRO,
                        reserved: 0,
                        max_context: c.max_context,
                    },
                )
            })
            .collect();
        Self {
            pools: Mutex::new(pools),
            costs,
            schedule,
            pending: Default::default(),
        }
    }

    /// Unreserved CUs of `model` at `tier` in `region`, from the configured replicas alone
    /// (no scheduled additions).
    pub fn available(&self, region: &str, model: &str, tier: Tier) -> Option<u32> {
        self.available_at(region, model, tier, Timestamp::MIN)
    }

    /// Unreserved CUs for a start at `at`, counting capacity scheduled by then.
    pub fn available_at(
        &self,
        region: &str,
        model: &str,
        tier: Tier,
        at: Timestamp,
    ) -> Option<u32> {
        let free = self.free_at(region, model, at)?;
        self.costs.cus_in(region, model, tier, free)
    }

    /// Unreserved micro-replicas in a pool, from the configured replicas alone.
    pub fn available_micro(&self, region: &str, model: &str) -> Option<u64> {
        self.free_at(region, model, Timestamp::MIN)
    }

    fn free_at(&self, region: &str, model: &str, at: Timestamp) -> Option<u64> {
        let pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        pools
            .get(&(region.to_string(), model.to_string()))
            .map(|p| {
                (p.capacity + self.schedule.added_by(region, model, at)).saturating_sub(p.reserved)
            })
    }

    #[allow(clippy::too_many_arguments)]
    fn check(
        &self,
        pools: &HashMap<(String, String), Pool>,
        model: &str,
        tier: Tier,
        shares: &[RegionShare],
        shape: &Shape,
        count_capacity: Option<Timestamp>,
    ) -> Result<(), PlanError> {
        for s in shares {
            let not_offered = || PlanError::NotOffered {
                region: s.region.clone(),
                model: model.into(),
            };
            let Some(pool) = pools.get(&(s.region.clone(), model.to_string())) else {
                return Err(not_offered());
            };
            if shape.context_ceiling > pool.max_context {
                return Err(PlanError::ShapeUnsupported {
                    region: s.region.clone(),
                    model: model.into(),
                    max_context: pool.max_context,
                    needed: shape.context_ceiling,
                });
            }
            let Some(at) = count_capacity else {
                continue;
            };
            let need = self.costs.share(model, tier, s).ok_or_else(not_offered)?;
            let free = (pool.capacity + self.schedule.added_by(&s.region, model, at))
                .saturating_sub(pool.reserved);
            if need > free {
                return Err(PlanError::CapacityUnavailable {
                    region: s.region.clone(),
                    model: model.into(),
                    tier,
                    requested: s.cus,
                    available: self.costs.cus_in(&s.region, model, tier, free).unwrap_or(0),
                    available_from: fits_from(&self.schedule, &s.region, model, at, free, need),
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
        tier: Tier,
        shares: &[RegionShare],
        shape: &Shape,
        at: Timestamp,
    ) -> Result<(), PlanError> {
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        self.check(&pools, model, tier, shares, shape, Some(at))?;
        for s in shares {
            let need = self.costs.share(model, tier, s).unwrap_or(0);
            if let Some(p) = pools.get_mut(&(s.region.clone(), model.to_string())) {
                p.reserved += need;
            }
        }
        Ok(())
    }

    async fn release(&self, model: &str, tier: Tier, shares: &[RegionShare]) {
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        for s in shares {
            let need = self.costs.share(model, tier, s).unwrap_or(0);
            if let Some(p) = pools.get_mut(&(s.region.clone(), model.to_string())) {
                p.reserved = p.reserved.saturating_sub(need);
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
        self.check(&pools, model, Tier::Standard, regions, shape, None)
    }

    async fn available_cus(
        &self,
        region: &str,
        model: &str,
        tier: Tier,
        at: Timestamp,
    ) -> Option<u32> {
        self.available_at(region, model, tier, at)
    }

    async fn pool_micro(&self, region: &str, model: &str) -> Option<(u64, u64)> {
        let pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        pools
            .get(&(region.to_string(), model.to_string()))
            .map(|p| (p.capacity, p.reserved))
    }

    async fn restore(&self, live: &[Held]) {
        let expected = expected_micro(&self.costs, live);
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        for ((region, model), micro) in expected {
            match pools.get_mut(&(region.clone(), model.clone())) {
                Some(p) => {
                    p.reserved += micro;
                    if p.reserved > p.capacity {
                        tracing::error!(%region, %model, reserved = p.reserved, capacity = p.capacity, "sold capacity exceeds configured replicas");
                    }
                }
                None => {
                    tracing::error!(%region, %model, "sold capacity in a pool that's no longer configured")
                }
            }
        }
    }

    async fn reconcile(
        &self,
        expected: &HashMap<(String, String), u64>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::micro_per_cu;

    fn planner() -> MemoryPlanner {
        let capacity = [CapacityConfig {
            region: "r".into(),
            model: "m".into(),
            replicas: 8,
            max_context: 32_768,
            profile: "p".into(),
        }];
        // The development B200 profile: 58,000 Standard and 35,500 Agentic WU/s a replica.
        let mut costs = Costs::default();
        costs.insert("r", "m", Tier::Standard, micro_per_cu(1_000.0, 58_000.0));
        costs.insert("r", "m", Tier::Agentic, micro_per_cu(1_000.0, 35_500.0));
        MemoryPlanner::new(&capacity, costs, Schedule::default())
    }

    fn share(cus: u32) -> Vec<RegionShare> {
        vec![RegionShare {
            region: "r".into(),
            cus,
        }]
    }

    fn shape() -> Shape {
        Shape {
            input_p95: 1_000,
            input_max: 4_000,
            output_p95: 100,
            context_ceiling: 8_000,
            cache_hit_ratio: 0.0,
            burst_factor: 1.0,
        }
    }

    #[tokio::test]
    async fn tiers_draw_different_amounts_from_one_pool() {
        let p = planner();
        assert_eq!(p.available("r", "m", Tier::Standard), Some(371));
        assert_eq!(p.available("r", "m", Tier::Agentic), Some(227));
        // 200 Standard CUs use 54% of the pool, leaving 104 Agentic CUs, not 27.
        p.reserve("m", Tier::Standard, &share(200), &shape(), Timestamp::MIN)
            .await
            .unwrap();
        assert_eq!(p.available("r", "m", Tier::Agentic), Some(104));
        let err = p
            .reserve("m", Tier::Agentic, &share(105), &shape(), Timestamp::MIN)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            PlanError::CapacityUnavailable {
                region: "r".into(),
                model: "m".into(),
                tier: Tier::Agentic,
                requested: 105,
                available: 104,
                available_from: None,
            }
        );
        assert!(err.to_string().contains("104 agentic CUs"), "{err}");
        // Releasing at the tier it was reserved at returns exactly what it took.
        p.release("m", Tier::Standard, &share(200)).await;
        assert_eq!(p.available_micro("r", "m"), Some(8 * MICRO));
    }

    #[tokio::test]
    async fn a_tier_the_pool_cannot_cost_is_not_offered() {
        let p = planner();
        assert!(matches!(
            p.reserve("m", Tier::Interactive, &share(1), &shape(), Timestamp::MIN)
                .await,
            Err(PlanError::NotOffered { .. })
        ));
        assert_eq!(p.available("r", "m", Tier::Interactive), None);
    }

    #[tokio::test]
    async fn restore_and_reconcile_count_micro_replicas() {
        let p = planner();
        let live = [
            Held {
                model: "m".into(),
                tier: Tier::Agentic,
                shares: share(10),
            },
            Held {
                model: "m".into(),
                tier: Tier::Standard,
                shares: share(10),
            },
        ];
        p.restore(&live).await;
        let used = 10 * 35_212 + 10 * 21_552;
        assert_eq!(p.available_micro("r", "m"), Some(8 * MICRO - used));
        let expected = expected_micro(&p.costs, &live);
        assert!(p.reconcile(&expected).await.unwrap().is_empty());
    }
}
