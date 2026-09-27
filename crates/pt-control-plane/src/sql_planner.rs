//! Capacity counters in the database, shared by control-plane instances (ADR-023).
//!
//! `pool_capacity` holds each (region, pool)'s sellable capacity and how much is reserved,
//! in micro-replicas (ADR-031, ADR-045). A reservation locks the pools it could use (in a
//! fixed order, region then pool, so two sales can't deadlock), places each claim with the
//! same rule as the memory planner ([`place`]), and raises `reserved_micro` with a
//! conditional UPDATE, all in one transaction. Two instances can't oversell.
//!
//! Reserving and saving the reservation are separate transactions. If an instance dies
//! between them, the pool keeps capacity nobody holds. The leader's [`reconcile`] finds such
//! drift against live reservations and corrects it once it has lasted two runs.
//!
//! Pools whose counts predate replica counting have `micro_counted` false. The first
//! instance to start recomputes their reserved micro-replicas from live reservations, once,
//! under a row lock ([`restore`]).
//!
//! [`reconcile`]: crate::planner::CapacityPlanner::reconcile
//! [`restore`]: crate::planner::CapacityPlanner::restore

use std::collections::HashMap;

use pt_core::{Shape, Tier};
use sqlx::postgres::PgPool;
use sqlx::Row;

use jiff::Timestamp;

use crate::capacity::{Costs, Schedule};
use crate::config::CapacityConfig;
use crate::planner::{
    expected_micro, judge_drift, largest_free_cus, place, CapacityPlanner, Claim, Drift, Held,
    PendingDrift, PlanError, PoolShare, PoolView,
};

pub struct SqlPlanner {
    pool: PgPool,
    /// Configured pools in configuration order (the placement tie-break). Counts come from
    /// the database.
    pools: Vec<PoolView>,
    costs: Costs,
    /// Replicas scheduled to arrive (ADR-037). From configuration, which every instance
    /// shares, so it isn't stored.
    schedule: Schedule,
    pending: PendingDrift,
}

fn unavailable(e: sqlx::Error) -> PlanError {
    PlanError::Unavailable(e.to_string())
}

impl SqlPlanner {
    /// Share `pool` (the store's) and load pool sizes from configuration. Capacity, model,
    /// and maximum context follow the configuration; reserved amounts are kept.
    pub async fn new(
        pool: PgPool,
        capacity: &[CapacityConfig],
        costs: Costs,
        schedule: Schedule,
    ) -> Result<Self, PlanError> {
        let pools: Vec<PoolView> = capacity.iter().map(PoolView::from_config).collect();
        for p in &pools {
            // A pool created now has nothing sold, so it's already counted.
            sqlx::query(
                "INSERT INTO pool_capacity
                     (region, pool, model, capacity_micro, reserved_micro, max_context,
                      micro_counted)
                 VALUES ($1, $2, $3, $4, 0, $5, true)
                 ON CONFLICT (region, pool) DO UPDATE
                     SET capacity_micro = excluded.capacity_micro,
                         model = excluded.model,
                         max_context = excluded.max_context",
            )
            .bind(&p.region)
            .bind(&p.pool)
            .bind(&p.model)
            .bind(p.capacity as i64)
            .bind(p.max_context as i64)
            .execute(&pool)
            .await
            .map_err(unavailable)?;
        }
        // Pools no longer configured stop selling. Keep them while anything is reserved.
        let rows =
            sqlx::query("SELECT region, pool, reserved_micro, micro_counted FROM pool_capacity")
                .fetch_all(&pool)
                .await
                .map_err(unavailable)?;
        for r in rows {
            let (region, id): (String, String) = (r.get("region"), r.get("pool"));
            if pools.iter().any(|p| p.region == region && p.pool == id) {
                continue;
            }
            let (reserved, counted): (i64, bool) =
                (r.get("reserved_micro"), r.get("micro_counted"));
            if reserved == 0 && counted {
                sqlx::query(
                    "DELETE FROM pool_capacity
                     WHERE region = $1 AND pool = $2 AND reserved_micro = 0 AND micro_counted",
                )
                .bind(&region)
                .bind(&id)
                .execute(&pool)
                .await
                .map_err(unavailable)?;
            } else {
                sqlx::query(
                    "UPDATE pool_capacity SET capacity_micro = 0 WHERE region = $1 AND pool = $2",
                )
                .bind(&region)
                .bind(&id)
                .execute(&pool)
                .await
                .map_err(unavailable)?;
                tracing::error!(%region, pool = %id, reserved, "sold capacity in a pool that's no longer configured");
            }
        }
        Ok(Self {
            pool,
            pools,
            costs,
            schedule,
            pending: Default::default(),
        })
    }

    /// Current counts for the configured pools of `model` in `region` (all of the region's
    /// pools of the model when `lock`, locked in pool order).
    async fn views<'e, E: sqlx::PgExecutor<'e>>(
        &self,
        executor: E,
        region: &str,
        model: &str,
        lock: bool,
    ) -> Result<Vec<PoolView>, PlanError> {
        let sql = if lock {
            "SELECT pool, capacity_micro, reserved_micro FROM pool_capacity
             WHERE region = $1 AND model = $2 ORDER BY pool FOR UPDATE"
        } else {
            "SELECT pool, capacity_micro, reserved_micro FROM pool_capacity
             WHERE region = $1 AND model = $2"
        };
        let rows = sqlx::query(sql)
            .bind(region)
            .bind(model)
            .fetch_all(executor)
            .await
            .map_err(unavailable)?;
        let counts: HashMap<String, (i64, i64)> = rows
            .iter()
            .map(|r| {
                (
                    r.get::<String, _>("pool"),
                    (r.get("capacity_micro"), r.get("reserved_micro")),
                )
            })
            .collect();
        // Configuration order, with the database's counts.
        Ok(self
            .pools
            .iter()
            .filter(|p| p.region == region && p.model == model)
            .filter_map(|p| {
                let (cap, res) = counts.get(&p.pool)?;
                Some(PoolView {
                    capacity: (*cap).max(0) as u64,
                    reserved: (*res).max(0) as u64,
                    ..p.clone()
                })
            })
            .collect())
    }

    /// The most unreserved CUs of `model` at `tier` in one pool in `region` for a start at
    /// `at`, counting capacity scheduled by then.
    pub async fn available_at(
        &self,
        region: &str,
        model: &str,
        tier: Tier,
        at: Timestamp,
    ) -> Result<Option<u32>, PlanError> {
        let views = self.views(&self.pool, region, model, false).await?;
        Ok(largest_free_cus(
            &views,
            region,
            model,
            tier,
            at,
            &self.costs,
            &self.schedule,
        ))
    }

    /// Like [`Self::available_at`], from the configured replicas alone.
    pub async fn available(
        &self,
        region: &str,
        model: &str,
        tier: Tier,
    ) -> Result<Option<u32>, PlanError> {
        self.available_at(region, model, tier, Timestamp::MIN).await
    }

    /// A pool's (capacity, reserved) micro-replicas.
    pub async fn pool_counts(
        &self,
        region: &str,
        pool: &str,
    ) -> Result<Option<(u64, u64)>, PlanError> {
        let row = sqlx::query(
            "SELECT capacity_micro, reserved_micro FROM pool_capacity
             WHERE region = $1 AND pool = $2",
        )
        .bind(region)
        .bind(pool)
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
        pool: &str,
    ) -> Result<Option<u64>, PlanError> {
        Ok(self
            .pool_counts(region, pool)
            .await?
            .map(|(cap, res)| cap.saturating_sub(res)))
    }
}

impl CapacityPlanner for SqlPlanner {
    async fn reserve(
        &self,
        model: &str,
        tier: Tier,
        claims: &[Claim],
        shape: &Shape,
        at: Timestamp,
    ) -> Result<Vec<PoolShare>, PlanError> {
        // Lock regions in order; each region's pools are locked in pool order.
        let mut order: Vec<usize> = (0..claims.len()).collect();
        order.sort_by(|a, b| claims[*a].region.cmp(&claims[*b].region));
        let mut placed: Vec<Option<PoolShare>> = vec![None; claims.len()];
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        for i in order {
            let c = &claims[i];
            let views = self.views(&mut *tx, &c.region, model, true).await?;
            let (pool, need) = place(
                c,
                model,
                tier,
                shape,
                Some(at),
                &views,
                &self.costs,
                &self.schedule,
            )?
            .expect("placement with a start date");
            let added = self.schedule.added_by(&c.region, &pool, at);
            let done = sqlx::query(
                "UPDATE pool_capacity SET reserved_micro = reserved_micro + $3
                 WHERE region = $1 AND pool = $2
                   AND reserved_micro + $3 <= capacity_micro + $4",
            )
            .bind(&c.region)
            .bind(&pool)
            .bind(need as i64)
            .bind(added as i64)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            if done.rows_affected() == 0 {
                // The rows are locked, so this means the counts changed underneath; retry.
                return Err(PlanError::Unavailable(format!(
                    "pool {pool} in {} changed during the sale",
                    c.region
                )));
            }
            placed[i] = Some(PoolShare {
                region: c.region.clone(),
                pool,
                cus: c.cus,
            });
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(placed.into_iter().flatten().collect())
    }

    async fn release(&self, tier: Tier, shares: &[PoolShare]) {
        let mut ordered: Vec<&PoolShare> = shares.iter().collect();
        ordered.sort_by(|a, b| (&a.region, &a.pool).cmp(&(&b.region, &b.pool)));
        let result: Result<(), sqlx::Error> = async {
            let mut tx = self.pool.begin().await?;
            for s in ordered {
                let need = self.costs.share(tier, s).unwrap_or(0);
                sqlx::query(
                    "UPDATE pool_capacity SET reserved_micro = GREATEST(reserved_micro - $3, 0)
                     WHERE region = $1 AND pool = $2",
                )
                .bind(&s.region)
                .bind(&s.pool)
                .bind(need as i64)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await
        }
        .await;
        if let Err(e) = result {
            // The pool keeps the capacity until the leader's reconcile frees it.
            tracing::error!(error = %e, "capacity release failed; reconcile will correct it");
        }
    }

    async fn check_shape(
        &self,
        model: &str,
        claims: &[Claim],
        shape: &Shape,
    ) -> Result<(), PlanError> {
        for c in claims {
            let views = self.views(&self.pool, &c.region, model, false).await?;
            place(
                c,
                model,
                Tier::Standard,
                shape,
                None,
                &views,
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
        match self.available_at(region, model, tier, at).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "capacity lookup failed");
                None
            }
        }
    }

    async fn pool_micro(&self, region: &str, pool: &str) -> Option<(u64, u64)> {
        match self.pool_counts(region, pool).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "capacity lookup failed");
                None
            }
        }
    }

    /// The database is authoritative, except for pools not yet counted in replicas. Those
    /// are set from `live` once, by whichever instance gets there first.
    async fn restore(&self, live: &[Held]) {
        let expected = expected_micro(&self.costs, live);
        let result: Result<u64, sqlx::Error> = async {
            let mut tx = self.pool.begin().await?;
            let rows = sqlx::query(
                "SELECT region, pool FROM pool_capacity WHERE NOT micro_counted
                 ORDER BY region, pool FOR UPDATE",
            )
            .fetch_all(&mut *tx)
            .await?;
            let mut counted = 0;
            for r in rows {
                let key: (String, String) = (r.get("region"), r.get("pool"));
                let micro = expected.get(&key).copied().unwrap_or(0);
                counted += sqlx::query(
                    "UPDATE pool_capacity SET reserved_micro = $3, micro_counted = true
                     WHERE region = $1 AND pool = $2 AND NOT micro_counted",
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
        let rows = sqlx::query("SELECT region, pool, reserved_micro FROM pool_capacity")
            .fetch_all(&self.pool)
            .await
            .map_err(unavailable)?;
        let mut out = Vec::new();
        for r in rows {
            let key: (String, String) = (r.get("region"), r.get("pool"));
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
                    "UPDATE pool_capacity SET reserved_micro = $4
                     WHERE region = $1 AND pool = $2 AND reserved_micro = $3",
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
                    tracing::warn!(region = %key.0, pool = %key.1, reserved, expected = want, "corrected reserved capacity");
                }
            }
            out.push(drift);
        }
        Ok(out)
    }
}
