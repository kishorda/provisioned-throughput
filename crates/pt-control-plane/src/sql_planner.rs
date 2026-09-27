//! Capacity counters in the database, shared by control-plane instances (ADR-023).
//!
//! `capacity_pools` holds each (region, model) pool's sellable capacity and how much is
//! reserved, in micro-replicas (ADR-031). A reservation locks its pools (in a fixed order,
//! so two sales can't deadlock), checks shape and capacity, and raises `reserved_micro`
//! with a conditional UPDATE, all in one transaction. Two instances can't oversell.
//!
//! Reserving and saving the reservation are separate transactions. If an instance dies
//! between them, the pool keeps capacity nobody holds. The leader's [`reconcile`] finds such
//! drift against live reservations and corrects it once it has lasted two runs.
//!
//! After the upgrade to replica counting, `micro_counted` is false on existing pools. The
//! first instance to start recomputes their reserved micro-replicas from live
//! reservations, once, under a row lock ([`restore`]).
//!
//! [`reconcile`]: crate::planner::CapacityPlanner::reconcile
//! [`restore`]: crate::planner::CapacityPlanner::restore

use std::collections::HashMap;

use pt_core::{Shape, Tier};
use sqlx::postgres::PgPool;
use sqlx::Row;

use jiff::Timestamp;

use crate::capacity::{Costs, Schedule, MICRO};
use crate::config::CapacityConfig;
use crate::model::RegionShare;
use crate::planner::{
    expected_micro, fits_from, judge_drift, CapacityPlanner, Drift, Held, PendingDrift, PlanError,
};

pub struct SqlPlanner {
    pool: PgPool,
    costs: Costs,
    /// Replicas scheduled to arrive (ADR-037). From configuration, which every instance
    /// shares, so it isn't stored.
    schedule: Schedule,
    pending: PendingDrift,
}

fn unavailable(e: sqlx::Error) -> PlanError {
    PlanError::Unavailable(e.to_string())
}

/// Shares sorted by region, so every transaction locks pools in the same order.
fn ordered(shares: &[RegionShare]) -> Vec<&RegionShare> {
    let mut v: Vec<&RegionShare> = shares.iter().collect();
    v.sort_by(|a, b| a.region.cmp(&b.region));
    v
}

impl SqlPlanner {
    /// Share `pool` (the store's) and load pool sizes from configuration. Capacity and
    /// maximum context follow the configuration; reserved amounts are kept.
    pub async fn new(
        pool: PgPool,
        capacity: &[CapacityConfig],
        costs: Costs,
        schedule: Schedule,
    ) -> Result<Self, PlanError> {
        for c in capacity {
            // A pool created now has nothing sold, so it's already counted.
            sqlx::query(
                "INSERT INTO capacity_pools
                     (region, model, capacity, reserved, max_context,
                      capacity_micro, reserved_micro, micro_counted)
                 VALUES ($1, $2, 0, 0, $3, $4, 0, true)
                 ON CONFLICT (region, model) DO UPDATE
                     SET capacity_micro = excluded.capacity_micro,
                         max_context = excluded.max_context",
            )
            .bind(&c.region)
            .bind(&c.model)
            .bind(c.max_context as i64)
            .bind((u64::from(c.replicas) * MICRO) as i64)
            .execute(&pool)
            .await
            .map_err(unavailable)?;
        }
        // Pools no longer configured stop selling. Keep them while anything is reserved.
        let rows =
            sqlx::query("SELECT region, model, reserved_micro, micro_counted FROM capacity_pools")
                .fetch_all(&pool)
                .await
                .map_err(unavailable)?;
        for r in rows {
            let (region, model): (String, String) = (r.get("region"), r.get("model"));
            if capacity
                .iter()
                .any(|c| c.region == region && c.model == model)
            {
                continue;
            }
            let (reserved, counted): (i64, bool) =
                (r.get("reserved_micro"), r.get("micro_counted"));
            if reserved == 0 && counted {
                sqlx::query(
                    "DELETE FROM capacity_pools
                     WHERE region = $1 AND model = $2 AND reserved_micro = 0 AND micro_counted",
                )
                .bind(&region)
                .bind(&model)
                .execute(&pool)
                .await
                .map_err(unavailable)?;
            } else {
                sqlx::query(
                    "UPDATE capacity_pools SET capacity_micro = 0 WHERE region = $1 AND model = $2",
                )
                .bind(&region)
                .bind(&model)
                .execute(&pool)
                .await
                .map_err(unavailable)?;
                tracing::error!(%region, %model, reserved, "sold capacity in a pool that's no longer configured");
            }
        }
        Ok(Self {
            pool,
            costs,
            schedule,
            pending: Default::default(),
        })
    }

    /// Unreserved CUs of `model` at `tier` in `region`, from the configured replicas alone
    /// (no scheduled additions).
    pub async fn available(
        &self,
        region: &str,
        model: &str,
        tier: Tier,
    ) -> Result<Option<u32>, PlanError> {
        self.available_at(region, model, tier, Timestamp::MIN).await
    }

    /// Unreserved CUs for a start at `at`, counting capacity scheduled by then.
    pub async fn available_at(
        &self,
        region: &str,
        model: &str,
        tier: Tier,
        at: Timestamp,
    ) -> Result<Option<u32>, PlanError> {
        // Add what's scheduled before subtracting: sales that start later may already
        // hold capacity that hasn't arrived yet.
        let added = self.schedule.added_by(region, model, at);
        Ok(self
            .pool_micro(region, model)
            .await?
            .and_then(|(cap, res)| {
                let free = (cap + added).saturating_sub(res);
                self.costs.cus_in(region, model, tier, free)
            }))
    }

    /// A pool's (capacity, reserved) micro-replicas.
    async fn pool_micro(&self, region: &str, model: &str) -> Result<Option<(u64, u64)>, PlanError> {
        let row = sqlx::query(
            "SELECT capacity_micro, reserved_micro FROM capacity_pools
             WHERE region = $1 AND model = $2",
        )
        .bind(region)
        .bind(model)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(row.map(|r| {
            let (cap, res): (i64, i64) = (r.get("capacity_micro"), r.get("reserved_micro"));
            (cap.max(0) as u64, res.max(0) as u64)
        }))
    }

    /// Unreserved micro-replicas in a pool.
    pub async fn available_micro(
        &self,
        region: &str,
        model: &str,
    ) -> Result<Option<u64>, PlanError> {
        let row = sqlx::query(
            "SELECT capacity_micro, reserved_micro FROM capacity_pools
             WHERE region = $1 AND model = $2",
        )
        .bind(region)
        .bind(model)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(row.map(|r| {
            let (cap, res): (i64, i64) = (r.get("capacity_micro"), r.get("reserved_micro"));
            (cap - res).max(0) as u64
        }))
    }

    /// Check a pool exists and serves `shape`. Returns (capacity, reserved) micro-replicas.
    async fn check_pool<'e, E: sqlx::PgExecutor<'e>>(
        executor: E,
        model: &str,
        share: &RegionShare,
        shape: &Shape,
        lock: bool,
    ) -> Result<(u64, u64), PlanError> {
        let sql = if lock {
            "SELECT capacity_micro, reserved_micro, max_context FROM capacity_pools
             WHERE region = $1 AND model = $2 FOR UPDATE"
        } else {
            "SELECT capacity_micro, reserved_micro, max_context FROM capacity_pools
             WHERE region = $1 AND model = $2"
        };
        let row = sqlx::query(sql)
            .bind(&share.region)
            .bind(model)
            .fetch_optional(executor)
            .await
            .map_err(unavailable)?
            .ok_or_else(|| PlanError::NotOffered {
                region: share.region.clone(),
                model: model.into(),
            })?;
        let max_context: i64 = row.get("max_context");
        if shape.context_ceiling > max_context as u64 {
            return Err(PlanError::ShapeUnsupported {
                region: share.region.clone(),
                model: model.into(),
                max_context: max_context as u64,
                needed: shape.context_ceiling,
            });
        }
        let (cap, res): (i64, i64) = (row.get("capacity_micro"), row.get("reserved_micro"));
        Ok((cap.max(0) as u64, res.max(0) as u64))
    }
}

impl CapacityPlanner for SqlPlanner {
    async fn reserve(
        &self,
        model: &str,
        tier: Tier,
        shares: &[RegionShare],
        shape: &Shape,
        at: Timestamp,
    ) -> Result<(), PlanError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        for s in ordered(shares) {
            let (capacity, reserved) = Self::check_pool(&mut *tx, model, s, shape, true).await?;
            let need = self
                .costs
                .share(model, tier, s)
                .ok_or_else(|| PlanError::NotOffered {
                    region: s.region.clone(),
                    model: model.into(),
                })?;
            let added = self.schedule.added_by(&s.region, model, at);
            let done = sqlx::query(
                "UPDATE capacity_pools SET reserved_micro = reserved_micro + $3
                 WHERE region = $1 AND model = $2
                   AND reserved_micro + $3 <= capacity_micro + $4",
            )
            .bind(&s.region)
            .bind(model)
            .bind(need as i64)
            .bind(added as i64)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            if done.rows_affected() == 0 {
                let free = (capacity + added).saturating_sub(reserved);
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
        tx.commit().await.map_err(unavailable)
    }

    async fn release(&self, model: &str, tier: Tier, shares: &[RegionShare]) {
        let result: Result<(), sqlx::Error> = async {
            let mut tx = self.pool.begin().await?;
            for s in ordered(shares) {
                let need = self.costs.share(model, tier, s).unwrap_or(0);
                sqlx::query(
                    "UPDATE capacity_pools SET reserved_micro = GREATEST(reserved_micro - $3, 0)
                     WHERE region = $1 AND model = $2",
                )
                .bind(&s.region)
                .bind(model)
                .bind(need as i64)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await
        }
        .await;
        if let Err(e) = result {
            // The pool keeps the capacity until the leader's reconcile frees it.
            tracing::error!(error = %e, %model, "capacity release failed; reconcile will correct it");
        }
    }

    async fn check_shape(
        &self,
        model: &str,
        regions: &[RegionShare],
        shape: &Shape,
    ) -> Result<(), PlanError> {
        for s in regions {
            Self::check_pool(&self.pool, model, s, shape, false).await?;
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
        match self.available_at(region, model, tier, at).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "capacity lookup failed");
                None
            }
        }
    }

    /// The database is authoritative, except for pools not yet counted in replicas (after
    /// the upgrade). Those are set from `live` once, by whichever instance gets there first.
    async fn restore(&self, live: &[Held]) {
        let expected = expected_micro(&self.costs, live);
        let result: Result<u64, sqlx::Error> = async {
            let mut tx = self.pool.begin().await?;
            let rows = sqlx::query(
                "SELECT region, model FROM capacity_pools WHERE NOT micro_counted FOR UPDATE",
            )
            .fetch_all(&mut *tx)
            .await?;
            let mut counted = 0;
            for r in rows {
                let key: (String, String) = (r.get("region"), r.get("model"));
                let micro = expected.get(&key).copied().unwrap_or(0);
                counted += sqlx::query(
                    "UPDATE capacity_pools SET reserved_micro = $3, micro_counted = true
                     WHERE region = $1 AND model = $2 AND NOT micro_counted",
                )
                .bind(&key.0)
                .bind(&key.1)
                .bind(micro as i64)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            }
            tx.commit().await?;
            Ok(counted)
        }
        .await;
        match result {
            Ok(0) => {}
            Ok(n) => tracing::info!(pools = n, "counted reserved capacity in replicas"),
            // Another instance may have done it; reconcile corrects anything left.
            Err(e) => tracing::warn!(error = %e, "counting reserved capacity in replicas failed"),
        }
    }

    async fn reconcile(
        &self,
        expected: &HashMap<(String, String), u64>,
    ) -> Result<Vec<Drift>, PlanError> {
        let rows = sqlx::query("SELECT region, model, reserved_micro FROM capacity_pools")
            .fetch_all(&self.pool)
            .await
            .map_err(unavailable)?;
        let mut out = Vec::new();
        for r in rows {
            let key: (String, String) = (r.get("region"), r.get("model"));
            let reserved: i64 = r.get("reserved_micro");
            let want = expected.get(&key).copied().unwrap_or(0);
            let Some((mut drift, fix)) =
                judge_drift(&self.pending, &key, reserved.max(0) as u64, want)
            else {
                continue;
            };
            if fix {
                // Only if nothing changed since this run read it.
                let done = sqlx::query(
                    "UPDATE capacity_pools SET reserved_micro = $4
                     WHERE region = $1 AND model = $2 AND reserved_micro = $3",
                )
                .bind(&key.0)
                .bind(&key.1)
                .bind(reserved)
                .bind(want as i64)
                .execute(&self.pool)
                .await
                .map_err(unavailable)?;
                drift.corrected = done.rows_affected() == 1;
                if drift.corrected {
                    tracing::warn!(region = %key.0, model = %key.1, reserved, expected = want, "corrected reserved capacity");
                }
            }
            out.push(drift);
        }
        Ok(out)
    }
}
