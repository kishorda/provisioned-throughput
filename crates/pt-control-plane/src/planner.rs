//! Capacity feasibility (docs/06 §1). The service asks the planner before it commits capacity.
//!
//! A region can have several pools for a model, for example on different GPU classes. Each
//! pool holds a number of replicas, counted in micro-replicas. A CU draws a different
//! amount at each tier and in each pool ([`crate::capacity::Costs`], ADR-031), so every call
//! names the reservation's tier.
//!
//! Each region share of a reservation is placed on exactly one pool (ADR-045). A [`Claim`]
//! either names its pool (a region the reservation already holds) or leaves the choice to
//! the planner, which picks the **best fit**: the pool with the least free capacity that
//! still fits, ties to the one listed first in configuration. [`place`] holds that rule, and
//! both planners use it. [`MemoryPlanner`] keeps the counters for one instance.
//! [`crate::sql_planner::SqlPlanner`] keeps them in the database, so several control-plane
//! instances share them (ADR-023).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use pt_core::{Shape, Tier};
use serde::{Deserialize, Serialize};

use jiff::Timestamp;

use crate::capacity::{Costs, Schedule, MICRO};
use crate::config::CapacityConfig;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("{model} is not offered in {region}")]
    NotOffered { region: String, model: String },
    #[error(
        "{region} has {available} {tier} CUs of {model} available in one pool; {requested} requested{}",
        from_note(.available_from)
    )]
    CapacityUnavailable {
        region: String,
        model: String,
        tier: Tier,
        requested: u32,
        /// The most any single pool could take: a share is placed on one pool.
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

/// CUs held on one pool in a region.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PoolShare {
    pub region: String,
    pub pool: String,
    pub cus: u32,
}

/// CUs to reserve in a region: on `pool`, or, with `None`, on whichever pool fits best.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub region: String,
    pub pool: Option<String>,
    pub cus: u32,
}

/// A pool as placement sees it: its configuration and current counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolView {
    pub region: String,
    pub pool: String,
    pub model: String,
    /// Micro-replicas configured (without scheduled arrivals) and reserved.
    pub capacity: u64,
    pub reserved: u64,
    pub max_context: u64,
}

impl PoolView {
    pub fn from_config(c: &CapacityConfig) -> Self {
        Self {
            region: c.region.clone(),
            pool: c.pool_id().to_string(),
            model: c.model.clone(),
            capacity: u64::from(c.replicas) * MICRO,
            reserved: 0,
            max_context: c.max_context,
        }
    }

    /// Unreserved micro-replicas for a start at `at`. Scheduled arrivals are added before
    /// reserved is subtracted, because later sales may hold capacity not yet arrived.
    pub fn free_at(&self, schedule: &Schedule, at: Timestamp) -> u64 {
        (self.capacity + schedule.added_by(&self.region, &self.pool, at))
            .saturating_sub(self.reserved)
    }
}

/// Where `claim` goes, and the micro-replicas it needs there. `pools` is in configuration
/// order, and may hold other regions' and models' pools. With `at = None`, only the shape
/// is checked and `Ok(None)` means some pool can serve it. Pure.
#[allow(clippy::too_many_arguments)]
pub fn place(
    claim: &Claim,
    model: &str,
    tier: Tier,
    shape: &Shape,
    at: Option<Timestamp>,
    pools: &[PoolView],
    costs: &Costs,
    schedule: &Schedule,
) -> Result<Option<(String, u64)>, PlanError> {
    let not_offered = || PlanError::NotOffered {
        region: claim.region.clone(),
        model: model.into(),
    };
    let candidates: Vec<&PoolView> = pools
        .iter()
        .filter(|p| p.region == claim.region && p.model == model)
        .filter(|p| claim.pool.as_ref().is_none_or(|n| *n == p.pool))
        .collect();
    if candidates.is_empty() {
        return Err(not_offered());
    }
    let serving: Vec<&PoolView> = candidates
        .iter()
        .copied()
        .filter(|p| shape.context_ceiling <= p.max_context)
        .collect();
    if serving.is_empty() {
        return Err(PlanError::ShapeUnsupported {
            region: claim.region.clone(),
            model: model.into(),
            max_context: candidates.iter().map(|p| p.max_context).max().unwrap_or(0),
            needed: shape.context_ceiling,
        });
    }
    let Some(at) = at else {
        return Ok(None);
    };
    let costed: Vec<(&PoolView, u64)> = serving
        .iter()
        .filter_map(|p| Some((*p, costs.per_cu(&p.region, &p.pool, tier)?)))
        .collect();
    if costed.is_empty() {
        return Err(not_offered());
    }
    let mut best: Option<(&PoolView, u64, u64)> = None; // (pool, need, slack)
    for (p, per_cu) in &costed {
        let need = per_cu * u64::from(claim.cus);
        let free = p.free_at(schedule, at);
        if free < need {
            continue;
        }
        let slack = free - need;
        if best.is_none_or(|(_, _, s)| slack < s) {
            best = Some((p, need, slack));
        }
    }
    if let Some((p, need, _)) = best {
        return Ok(Some((p.pool.clone(), need)));
    }
    let available = costed
        .iter()
        .map(|(p, _)| {
            costs
                .cus_in(&p.region, &p.pool, tier, p.free_at(schedule, at))
                .unwrap_or(0)
        })
        .max()
        .unwrap_or(0);
    let available_from = costed
        .iter()
        .filter_map(|(p, per_cu)| {
            fits_from(
                schedule,
                &p.region,
                &p.pool,
                at,
                p.free_at(schedule, at),
                per_cu * u64::from(claim.cus),
            )
        })
        .min();
    Err(PlanError::CapacityUnavailable {
        region: claim.region.clone(),
        model: model.into(),
        tier,
        requested: claim.cus,
        available,
        available_from,
    })
}

/// The earliest date after `at` when a pool with `free` micro-replicas at `at` can take
/// `need` more, as scheduled capacity arrives.
pub(crate) fn fits_from(
    schedule: &Schedule,
    region: &str,
    pool: &str,
    at: Timestamp,
    free: u64,
    need: u64,
) -> Option<Timestamp> {
    let now_added = schedule.added_by(region, pool, at);
    schedule
        .dates_after(region, pool, at)
        .into_iter()
        .find(|d| free + (schedule.added_by(region, pool, *d) - now_added) >= need)
}

/// Unreserved CUs at `tier` in the pool of `model` in `region` with the most room, for a
/// start at `at`: the largest share a single sale could place there.
pub fn largest_free_cus(
    pools: &[PoolView],
    region: &str,
    model: &str,
    tier: Tier,
    at: Timestamp,
    costs: &Costs,
    schedule: &Schedule,
) -> Option<u32> {
    pools
        .iter()
        .filter(|p| p.region == region && p.model == model)
        .filter_map(|p| costs.cus_in(&p.region, &p.pool, tier, p.free_at(schedule, at)))
        .max()
}

/// A pool whose reserved micro-replicas don't match its live reservations.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Drift {
    pub region: String,
    pub pool: String,
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
    pub tier: Tier,
    pub shares: Vec<PoolShare>,
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
            pool: key.1.clone(),
            reserved,
            expected,
            corrected: fix,
        },
        fix,
    ))
}

/// Micro-replicas every live reservation holds, per (region, pool).
pub fn expected_micro(costs: &Costs, held: &[Held]) -> HashMap<(String, String), u64> {
    let mut out: HashMap<(String, String), u64> = HashMap::new();
    for h in held {
        for s in &h.shares {
            if let Some(m) = costs.share(h.tier, s) {
                *out.entry((s.region.clone(), s.pool.clone())).or_default() += m;
            }
        }
    }
    out
}

pub trait CapacityPlanner: Send + Sync + 'static {
    /// Reserve `claims` of `model` at `tier` for a workload of `shape` starting at `at`, all
    /// or nothing, and return where each was placed (in the same order). Capacity scheduled
    /// to arrive by `at` counts (ADR-037).
    fn reserve(
        &self,
        model: &str,
        tier: Tier,
        claims: &[Claim],
        shape: &Shape,
        at: Timestamp,
    ) -> impl Future<Output = Result<Vec<PoolShare>, PlanError>> + Send;

    /// Return capacity previously reserved at `tier`.
    fn release(&self, tier: Tier, shares: &[PoolShare]) -> impl Future<Output = ()> + Send;

    /// Check that each claim's pool (or, unplaced, some pool in its region) can serve
    /// `shape`, without reserving anything.
    fn check_shape(
        &self,
        model: &str,
        claims: &[Claim],
        shape: &Shape,
    ) -> impl Future<Output = Result<(), PlanError>> + Send;

    /// The most unreserved CUs of `model` at `tier` any one pool in `region` has for a start
    /// at `at`, or `None` if it isn't offered there.
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
        pool: &str,
    ) -> impl Future<Output = Option<(u64, u64)>> + Send;

    /// Re-establish capacity already sold, from every live reservation (at startup). Never
    /// fails: a pool whose configured replicas have shrunk below what's sold is
    /// overcommitted, reports no availability, and new sales fail until it's fixed. A
    /// planner whose counts are durable only recomputes pools that have never been counted
    /// in replicas.
    fn restore(&self, live: &[Held]) -> impl Future<Output = ()> + Send;

    /// Compare reserved micro-replicas with `expected` per (region, pool). A pool is
    /// corrected only when the same drift shows on two runs in a row, so a sale in flight is
    /// never undone. Returns every drift found.
    fn reconcile(
        &self,
        expected: &HashMap<(String, String), u64>,
    ) -> impl Future<Output = Result<Vec<Drift>, PlanError>> + Send;
}

#[derive(Debug, Default)]
pub struct MemoryPlanner {
    /// In configuration order.
    pools: Mutex<Vec<PoolView>>,
    costs: Costs,
    schedule: Schedule,
    pending: PendingDrift,
}

impl MemoryPlanner {
    pub fn new(capacity: &[CapacityConfig], costs: Costs, schedule: Schedule) -> Self {
        Self {
            pools: Mutex::new(capacity.iter().map(PoolView::from_config).collect()),
            costs,
            schedule,
            pending: Default::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<PoolView>> {
        self.pools.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The most unreserved CUs of `model` at `tier` in one pool in `region`, from the
    /// configured replicas alone (no scheduled additions).
    pub fn available(&self, region: &str, model: &str, tier: Tier) -> Option<u32> {
        self.available_at(region, model, tier, Timestamp::MIN)
    }

    /// Like [`Self::available`], for a start at `at`, counting capacity scheduled by then.
    pub fn available_at(
        &self,
        region: &str,
        model: &str,
        tier: Tier,
        at: Timestamp,
    ) -> Option<u32> {
        largest_free_cus(
            &self.lock(),
            region,
            model,
            tier,
            at,
            &self.costs,
            &self.schedule,
        )
    }

    /// Unreserved micro-replicas in a pool, from the configured replicas alone.
    pub fn available_micro(&self, region: &str, pool: &str) -> Option<u64> {
        self.lock()
            .iter()
            .find(|p| p.region == region && p.pool == pool)
            .map(|p| p.capacity.saturating_sub(p.reserved))
    }
}

impl CapacityPlanner for MemoryPlanner {
    async fn reserve(
        &self,
        model: &str,
        tier: Tier,
        claims: &[Claim],
        shape: &Shape,
        at: Timestamp,
    ) -> Result<Vec<PoolShare>, PlanError> {
        let mut pools = self.lock();
        // Place every claim on a scratch copy, so a later failure changes nothing.
        let mut scratch = pools.clone();
        let mut placed = Vec::new();
        for c in claims {
            let (pool, need) = place(
                c,
                model,
                tier,
                shape,
                Some(at),
                &scratch,
                &self.costs,
                &self.schedule,
            )?
            .expect("placement with a start date");
            if let Some(p) = scratch
                .iter_mut()
                .find(|p| p.region == c.region && p.pool == pool)
            {
                p.reserved += need;
            }
            placed.push(PoolShare {
                region: c.region.clone(),
                pool,
                cus: c.cus,
            });
        }
        *pools = scratch;
        Ok(placed)
    }

    async fn release(&self, tier: Tier, shares: &[PoolShare]) {
        let mut pools = self.lock();
        for s in shares {
            let need = self.costs.share(tier, s).unwrap_or(0);
            if let Some(p) = pools
                .iter_mut()
                .find(|p| p.region == s.region && p.pool == s.pool)
            {
                p.reserved = p.reserved.saturating_sub(need);
            }
        }
    }

    async fn check_shape(
        &self,
        model: &str,
        claims: &[Claim],
        shape: &Shape,
    ) -> Result<(), PlanError> {
        let pools = self.lock();
        for c in claims {
            place(
                c,
                model,
                Tier::Standard,
                shape,
                None,
                &pools,
                &self.costs,
                &self.schedule,
            )?;
        }
        Ok(())
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

    async fn pool_micro(&self, region: &str, pool: &str) -> Option<(u64, u64)> {
        self.lock()
            .iter()
            .find(|p| p.region == region && p.pool == pool)
            .map(|p| (p.capacity, p.reserved))
    }

    async fn restore(&self, live: &[Held]) {
        let expected = expected_micro(&self.costs, live);
        let mut pools = self.lock();
        for ((region, pool), micro) in expected {
            match pools
                .iter_mut()
                .find(|p| p.region == region && p.pool == pool)
            {
                Some(p) => {
                    p.reserved += micro;
                    if p.reserved > p.capacity {
                        tracing::error!(%region, %pool, reserved = p.reserved, capacity = p.capacity, "sold capacity exceeds configured replicas");
                    }
                }
                None => {
                    tracing::error!(%region, %pool, "sold capacity in a pool that's no longer configured")
                }
            }
        }
    }

    async fn reconcile(
        &self,
        expected: &HashMap<(String, String), u64>,
    ) -> Result<Vec<Drift>, PlanError> {
        let mut pools = self.lock();
        let mut out = Vec::new();
        for pool in pools.iter_mut() {
            let key = (pool.region.clone(), pool.pool.clone());
            let want = expected.get(&key).copied().unwrap_or(0);
            if let Some((drift, fix)) = judge_drift(&self.pending, &key, pool.reserved, want) {
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

    fn pool(id: &str, replicas: u32, max_context: u64) -> CapacityConfig {
        CapacityConfig {
            region: "r".into(),
            model: "m".into(),
            pool: Some(id.into()),
            replicas,
            max_context,
            profile: "p".into(),
        }
    }

    /// The development B200 profile: 58,000 Standard and 35,500 Agentic WU/s a replica.
    fn costs(pools: &[&str]) -> Costs {
        let mut costs = Costs::default();
        for p in pools {
            costs.insert("r", p, Tier::Standard, micro_per_cu(1_000.0, 58_000.0));
            costs.insert("r", p, Tier::Agentic, micro_per_cu(1_000.0, 35_500.0));
        }
        costs
    }

    fn planner() -> MemoryPlanner {
        MemoryPlanner::new(&[pool("m", 8, 32_768)], costs(&["m"]), Schedule::default())
    }

    fn claim(cus: u32) -> Vec<Claim> {
        vec![Claim {
            region: "r".into(),
            pool: None,
            cus,
        }]
    }

    fn on(pool: &str, cus: u32) -> Vec<PoolShare> {
        vec![PoolShare {
            region: "r".into(),
            pool: pool.into(),
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
        let placed = p
            .reserve("m", Tier::Standard, &claim(200), &shape(), Timestamp::MIN)
            .await
            .unwrap();
        assert_eq!(placed, on("m", 200));
        assert_eq!(p.available("r", "m", Tier::Agentic), Some(104));
        let err = p
            .reserve("m", Tier::Agentic, &claim(105), &shape(), Timestamp::MIN)
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
        p.release(Tier::Standard, &on("m", 200)).await;
        assert_eq!(p.available_micro("r", "m"), Some(8 * MICRO));
    }

    #[tokio::test]
    async fn a_tier_the_pool_cannot_cost_is_not_offered() {
        let p = planner();
        assert!(matches!(
            p.reserve("m", Tier::Interactive, &claim(1), &shape(), Timestamp::MIN)
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
                tier: Tier::Agentic,
                shares: on("m", 10),
            },
            Held {
                tier: Tier::Standard,
                shares: on("m", 10),
            },
        ];
        p.restore(&live).await;
        let used = 10 * 35_212 + 10 * 21_552;
        assert_eq!(p.available_micro("r", "m"), Some(8 * MICRO - used));
        let expected = expected_micro(&p.costs, &live);
        assert!(p.reconcile(&expected).await.unwrap().is_empty());
    }

    fn two_pools() -> MemoryPlanner {
        // "big" first in config: it wins ties.
        MemoryPlanner::new(
            &[pool("big", 8, 32_768), pool("small", 4, 32_768)],
            costs(&["big", "small"]),
            Schedule::default(),
        )
    }

    #[tokio::test]
    async fn best_fit_packs_small_sales_and_keeps_room_for_large_ones() {
        let p = two_pools();
        // 4 replicas hold 185 Standard CUs, 8 hold 371. 100 fits both: the smaller wins.
        let a = p
            .reserve("m", Tier::Standard, &claim(100), &shape(), Timestamp::MIN)
            .await
            .unwrap();
        assert_eq!(a, on("small", 100));
        // 300 fits only the big pool.
        let b = p
            .reserve("m", Tier::Standard, &claim(300), &shape(), Timestamp::MIN)
            .await
            .unwrap();
        assert_eq!(b, on("big", 300));
        // 90 left in "small" and 71 in "big": 80 goes to "small", the only one it fits.
        let c = p
            .reserve("m", Tier::Standard, &claim(80), &shape(), Timestamp::MIN)
            .await
            .unwrap();
        assert_eq!(c, on("small", 80));
        // A share never splits: 71 is the most one pool can take.
        let err = p
            .reserve("m", Tier::Standard, &claim(72), &shape(), Timestamp::MIN)
            .await
            .unwrap_err();
        assert!(
            matches!(err, PlanError::CapacityUnavailable { available: 71, .. }),
            "{err}"
        );
        assert_eq!(p.available("r", "m", Tier::Standard), Some(71));
    }

    #[tokio::test]
    async fn ties_go_to_the_pool_listed_first_and_named_pools_are_honoured() {
        let p = MemoryPlanner::new(
            &[pool("new", 4, 32_768), pool("old", 4, 32_768)],
            costs(&["new", "old"]),
            Schedule::default(),
        );
        let first = p
            .reserve("m", Tier::Standard, &claim(10), &shape(), Timestamp::MIN)
            .await
            .unwrap();
        assert_eq!(first, on("new", 10));
        let named = vec![Claim {
            region: "r".into(),
            pool: Some("old".into()),
            cus: 10,
        }];
        let placed = p
            .reserve("m", Tier::Standard, &named, &shape(), Timestamp::MIN)
            .await
            .unwrap();
        assert_eq!(placed, on("old", 10));
        // A named pool that doesn't serve the model isn't offered.
        let bad = vec![Claim {
            region: "r".into(),
            pool: Some("gone".into()),
            cus: 1,
        }];
        assert!(matches!(
            p.reserve("m", Tier::Standard, &bad, &shape(), Timestamp::MIN)
                .await,
            Err(PlanError::NotOffered { .. })
        ));
    }

    #[tokio::test]
    async fn context_and_all_or_nothing_across_pools() {
        let p = MemoryPlanner::new(
            &[pool("short", 8, 16_384), pool("long", 2, 131_072)],
            costs(&["short", "long"]),
            Schedule::default(),
        );
        let mut long = shape();
        long.context_ceiling = 65_536;
        // Only the long-context pool serves it, even though it's the smaller.
        let placed = p
            .reserve("m", Tier::Standard, &claim(10), &long, Timestamp::MIN)
            .await
            .unwrap();
        assert_eq!(placed, on("long", 10));
        let mut huge = shape();
        huge.context_ceiling = 200_000;
        assert!(matches!(
            p.check_shape("m", &claim(1), &huge).await,
            Err(PlanError::ShapeUnsupported {
                max_context: 131_072,
                ..
            })
        ));
        // Two claims: the second fails, so the first isn't kept.
        let mut other = shape();
        other.context_ceiling = 8_000;
        let two = vec![
            Claim {
                region: "r".into(),
                pool: Some("short".into()),
                cus: 10,
            },
            Claim {
                region: "r".into(),
                pool: Some("long".into()),
                cus: 1_000,
            },
        ];
        assert!(p
            .reserve("m", Tier::Standard, &two, &other, Timestamp::MIN)
            .await
            .is_err());
        assert_eq!(p.available_micro("r", "short"), Some(8 * MICRO));
    }

    #[test]
    fn scheduled_arrivals_count_per_pool() {
        let mut schedule = Schedule::default();
        let later: Timestamp = "2026-12-01T00:00:00Z".parse().unwrap();
        schedule.add("r", "small", later, 4);
        let pools: Vec<PoolView> = [pool("big", 8, 32_768), pool("small", 4, 32_768)]
            .iter()
            .map(PoolView::from_config)
            .collect();
        let costs = costs(&["big", "small"]);
        let err = place(
            &claim(372)[0],
            "m",
            Tier::Standard,
            &shape(),
            Some(Timestamp::MIN),
            &pools,
            &costs,
            &schedule,
        )
        .unwrap_err();
        // Neither pool fits now; "small" fits once its 4 replicas arrive (8 → 371 CUs is
        // not enough for 372, so it never fits there either): no date.
        assert!(matches!(
            err,
            PlanError::CapacityUnavailable {
                available: 371,
                available_from: None,
                ..
            }
        ));
        let err = place(
            &claim(200)[0],
            "m",
            Tier::Standard,
            &shape(),
            Some(Timestamp::MIN),
            &pools[1..],
            &costs,
            &schedule,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            PlanError::CapacityUnavailable { available_from: Some(t), .. } if t == later
        ));
    }
}
