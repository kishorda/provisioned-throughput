//! Capacity counters in the database, shared by control-plane instances (ADR-023).
//!
//! `capacity_pools` holds each (region, model) pool's sellable CUs and how many are
//! reserved. A reservation locks its pools (in a fixed order, so two sales can't deadlock),
//! checks shape and capacity, and raises `reserved` with a conditional UPDATE, all in one
//! transaction. Two instances can't oversell.
//!
//! Reserving and saving the reservation are separate transactions. If an instance dies
//! between them, the pool keeps capacity nobody holds. The leader's [`reconcile`] finds such
//! drift against live reservations and corrects it once it has lasted two runs.
//!
//! [`reconcile`]: crate::planner::CapacityPlanner::reconcile

use std::collections::HashMap;

use pt_core::Shape;
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::config::CapacityConfig;
use crate::model::RegionShare;
use crate::planner::{judge_drift, CapacityPlanner, Drift, PendingDrift, PlanError};

pub struct SqlPlanner {
    pool: PgPool,
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
    /// maximum context follow the configuration; reserved counts are kept.
    pub async fn new(pool: PgPool, capacity: &[CapacityConfig]) -> Result<Self, PlanError> {
        for c in capacity {
            sqlx::query(
                "INSERT INTO capacity_pools (region, model, capacity, reserved, max_context)
                 VALUES ($1, $2, $3, 0, $4)
                 ON CONFLICT (region, model) DO UPDATE
                     SET capacity = excluded.capacity, max_context = excluded.max_context",
            )
            .bind(&c.region)
            .bind(&c.model)
            .bind(c.cus as i32)
            .bind(c.max_context as i64)
            .execute(&pool)
            .await
            .map_err(unavailable)?;
        }
        // Pools no longer configured stop selling. Keep them while anything is reserved.
        let rows = sqlx::query("SELECT region, model, reserved FROM capacity_pools")
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
            let reserved: i32 = r.get("reserved");
            if reserved == 0 {
                sqlx::query(
                    "DELETE FROM capacity_pools WHERE region = $1 AND model = $2 AND reserved = 0",
                )
                .bind(&region)
                .bind(&model)
                .execute(&pool)
                .await
                .map_err(unavailable)?;
            } else {
                sqlx::query(
                    "UPDATE capacity_pools SET capacity = 0 WHERE region = $1 AND model = $2",
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
            pending: Default::default(),
        })
    }

    /// Unreserved CUs of `model` in `region`.
    pub async fn available(&self, region: &str, model: &str) -> Result<Option<u32>, PlanError> {
        let row = sqlx::query(
            "SELECT capacity, reserved FROM capacity_pools WHERE region = $1 AND model = $2",
        )
        .bind(region)
        .bind(model)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(row.map(|r| {
            let (cap, res): (i32, i32) = (r.get("capacity"), r.get("reserved"));
            (cap - res).max(0) as u32
        }))
    }

    /// Check a pool exists and serves `shape`. Returns (capacity, reserved).
    async fn check_pool<'e, E: sqlx::PgExecutor<'e>>(
        executor: E,
        model: &str,
        share: &RegionShare,
        shape: &Shape,
        lock: bool,
    ) -> Result<(u32, u32), PlanError> {
        let sql = if lock {
            "SELECT capacity, reserved, max_context FROM capacity_pools
             WHERE region = $1 AND model = $2 FOR UPDATE"
        } else {
            "SELECT capacity, reserved, max_context FROM capacity_pools
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
        let (cap, res): (i32, i32) = (row.get("capacity"), row.get("reserved"));
        Ok((cap as u32, res as u32))
    }
}

impl CapacityPlanner for SqlPlanner {
    async fn reserve(
        &self,
        model: &str,
        shares: &[RegionShare],
        shape: &Shape,
    ) -> Result<(), PlanError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        for s in ordered(shares) {
            let (capacity, reserved) = Self::check_pool(&mut *tx, model, s, shape, true).await?;
            let done = sqlx::query(
                "UPDATE capacity_pools SET reserved = reserved + $3
                 WHERE region = $1 AND model = $2 AND reserved + $3 <= capacity",
            )
            .bind(&s.region)
            .bind(model)
            .bind(s.cus as i32)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            if done.rows_affected() == 0 {
                return Err(PlanError::CapacityUnavailable {
                    region: s.region.clone(),
                    model: model.into(),
                    requested: s.cus,
                    available: capacity.saturating_sub(reserved),
                });
            }
        }
        tx.commit().await.map_err(unavailable)
    }

    async fn release(&self, model: &str, shares: &[RegionShare]) {
        let result: Result<(), sqlx::Error> = async {
            let mut tx = self.pool.begin().await?;
            for s in ordered(shares) {
                sqlx::query(
                    "UPDATE capacity_pools SET reserved = GREATEST(reserved - $3, 0)
                     WHERE region = $1 AND model = $2",
                )
                .bind(&s.region)
                .bind(model)
                .bind(s.cus as i32)
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

    async fn available_cus(&self, region: &str, model: &str) -> Option<u32> {
        match self.available(region, model).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "capacity lookup failed");
                None
            }
        }
    }

    /// The database is authoritative, so there's nothing to restore.
    async fn restore(&self, _model: &str, _shares: &[RegionShare]) {}

    async fn reconcile(
        &self,
        expected: &HashMap<(String, String), u32>,
    ) -> Result<Vec<Drift>, PlanError> {
        let rows = sqlx::query("SELECT region, model, reserved FROM capacity_pools")
            .fetch_all(&self.pool)
            .await
            .map_err(unavailable)?;
        let mut out = Vec::new();
        for r in rows {
            let key: (String, String) = (r.get("region"), r.get("model"));
            let reserved: i32 = r.get("reserved");
            let want = expected.get(&key).copied().unwrap_or(0);
            let Some((mut drift, fix)) = judge_drift(&self.pending, &key, reserved as u32, want)
            else {
                continue;
            };
            if fix {
                // Only if nothing changed since this run read it.
                let done = sqlx::query(
                    "UPDATE capacity_pools SET reserved = $4
                     WHERE region = $1 AND model = $2 AND reserved = $3",
                )
                .bind(&key.0)
                .bind(&key.1)
                .bind(reserved)
                .bind(want as i32)
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
