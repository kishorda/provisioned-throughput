//! Pool sizing (docs/06 §2).
//!
//! ```text
//! units      = Σ_r alloc_r / cap(tier_r)                          # replica-equivalents
//! burst      = z · sqrt( Σ_r (alloc_r · (burst_r − 1) / cap(tier_r))² )
//! floor      = ceil( (units + burst) / target_util )
//! desired    = floor + failure_k + maintenance_slots + hot_spares
//! minAvail   = floor + failure_k
//! ```
//!
//! Allocations on one pool can be on different tiers, and a replica delivers different
//! WU/s at each tier's SLO, so demand is converted to replica-equivalents per allocation
//! before summing. Disaggregated pools size prefill and decode separately, splitting demand
//! by `prefillShare`.
//!
//! **Failover** ([`size_with_failover`], docs/07 §4). While a region failover is active,
//! the pool's allocations carry extra demand. The pool is sized with it, and the increase
//! in the floor is absorbed first by hot spares (already serving; the router preempts
//! their PAYG), then by loading warm spares:
//!
//! ```text
//! needed_warm = max(0, floor_failover − floor − hot_spares)      # per role
//! loaded      = min(needed_warm, warm_spares), never below the previous loaded count
//! desired     = floor + failure_k + maintenance_slots + hot_spares + loaded
//! minAvail    = min(floor_failover + failure_k, desired − maintenance_slots)
//! ```
//!
//! Loaded warm spares are held until the failover ends, so the ramp-down doesn't churn GPUs.

use pt_core::cost::TierCapacity;
use pt_core::Tier;
use pt_crds::pool::{ModelPoolSpec, RoleReplicas};
use pt_crds::profile::PerformanceProfileSpec;
use pt_crds::PoolAllocationSpec;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Demand {
    pub wu_per_sec: f64,
    pub tier: Tier,
    pub burst_factor: f64,
}

impl From<&PoolAllocationSpec> for Demand {
    fn from(a: &PoolAllocationSpec) -> Self {
        Self {
            wu_per_sec: a.wu_per_sec,
            tier: a.tier,
            burst_factor: a.burst_factor,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sizing {
    pub floor: RoleReplicas,
    pub desired: RoleReplicas,
    pub min_available: RoleReplicas,
    pub allocated_wu_per_sec: f64,
    /// `desired` exceeded `maxReplicas` and was capped.
    pub shortfall: Option<Shortfall>,
    /// Warm spares loaded for an active failover. Included in `desired`.
    pub warm_loaded: RoleReplicas,
    /// Replicas the failover needs beyond hot and warm spares (per role, summed).
    pub failover_shortfall: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shortfall {
    pub needed: u32,
    pub max: u32,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SizingError {
    #[error("profile has no {role} capacity for tier {tier:?}")]
    NoCapacity { role: &'static str, tier: Tier },
    #[error("targetUtilization must be in (0, 1], got {0}")]
    BadUtilization(f64),
    #[error("prefillShare must be in (0, 1), got {0}")]
    BadPrefillShare(f64),
    #[error("allocation has negative or non-finite wuPerSec ({0})")]
    BadDemand(f64),
}

/// Replicas needed for one role, before headroom.
fn role_floor(
    demands: &[Demand],
    share: f64,
    capacity: &TierCapacity,
    role: &'static str,
    target_util: f64,
    z: f64,
) -> Result<u32, SizingError> {
    let mut units = 0.0;
    let mut burst_sq = 0.0;
    for d in demands {
        if !d.wu_per_sec.is_finite() || d.wu_per_sec < 0.0 {
            return Err(SizingError::BadDemand(d.wu_per_sec));
        }
        if d.wu_per_sec == 0.0 {
            continue;
        }
        let cap = capacity.for_tier(d.tier);
        if cap <= 0.0 {
            return Err(SizingError::NoCapacity { role, tier: d.tier });
        }
        let u = d.wu_per_sec * share / cap;
        units += u;
        burst_sq += (u * (d.burst_factor - 1.0).max(0.0)).powi(2);
    }
    let needed = (units + z * burst_sq.sqrt()) / target_util;
    // Tolerate floating-point noise just above an integer.
    Ok((needed - 1e-9).ceil().max(0.0) as u32)
}

pub fn size(
    pool: &ModelPoolSpec,
    profile: &PerformanceProfileSpec,
    demands: &[Demand],
) -> Result<Sizing, SizingError> {
    let util = pool.target_utilization;
    if !(util > 0.0 && util <= 1.0) {
        return Err(SizingError::BadUtilization(util));
    }
    let h = &pool.headroom;
    let allocated = demands.iter().map(|d| d.wu_per_sec).sum();

    let (floor, desired, min_available) = if pool.disaggregation.enabled {
        let d = &pool.disaggregation;
        if !(d.prefill_share > 0.0 && d.prefill_share < 1.0) {
            return Err(SizingError::BadPrefillShare(d.prefill_share));
        }
        let (pcap, dcap) = match &profile.role_capacity {
            Some(rc) => (rc.prefill, rc.decode),
            None => (profile.capacity, profile.capacity),
        };
        let p = role_floor(
            demands,
            d.prefill_share,
            &pcap,
            "prefill",
            util,
            pool.burst_z,
        )?
        .max(d.prefill_replicas_min);
        let dd = role_floor(
            demands,
            1.0 - d.prefill_share,
            &dcap,
            "decode",
            util,
            pool.burst_z,
        )?
        .max(d.decode_replicas_min);
        let extra = h.failure_domain_k + h.maintenance_slots + h.hot_spares;
        (
            RoleReplicas::disaggregated(p, dd),
            RoleReplicas::disaggregated(p + extra, dd + extra),
            RoleReplicas::disaggregated(p + h.failure_domain_k, dd + h.failure_domain_k),
        )
    } else {
        let n = role_floor(
            demands,
            1.0,
            &profile.capacity,
            "aggregated",
            util,
            pool.burst_z,
        )?;
        (
            RoleReplicas::aggregated(n),
            RoleReplicas::aggregated(n + h.failure_domain_k + h.maintenance_slots + h.hot_spares),
            RoleReplicas::aggregated(n + h.failure_domain_k),
        )
    };

    let mut sizing = Sizing {
        floor,
        desired,
        min_available,
        allocated_wu_per_sec: allocated,
        shortfall: None,
        warm_loaded: RoleReplicas::default(),
        failover_shortfall: 0,
    };
    apply_max_replicas(pool, &mut sizing);
    Ok(sizing)
}

fn apply_max_replicas(pool: &ModelPoolSpec, sizing: &mut Sizing) {
    if let Some(max) = pool.max_replicas {
        if sizing.desired.total > max {
            sizing.shortfall = Some(Shortfall {
                needed: sizing.desired.total,
                max,
            });
            sizing.desired = cap_total(sizing.desired, max);
        }
    }
}

/// Size a pool whose allocations carry extra failover demand (`extra[i]` WU/s for
/// `demands[i]`; missing entries are 0). `previous` is the warm spares loaded at the last
/// reconcile. With no failover demand, this is [`size`] and no warm spares are loaded.
pub fn size_with_failover(
    pool: &ModelPoolSpec,
    profile: &PerformanceProfileSpec,
    demands: &[Demand],
    extra: &[f64],
    previous: RoleReplicas,
) -> Result<Sizing, SizingError> {
    let base = size(pool, profile, demands)?;
    let extra_total: f64 = extra.iter().filter(|e| e.is_finite() && **e > 0.0).sum();
    if extra_total <= 0.0 {
        return Ok(base);
    }
    let with_failover: Vec<Demand> = demands
        .iter()
        .enumerate()
        .map(|(i, d)| Demand {
            wu_per_sec: d.wu_per_sec
                + extra
                    .get(i)
                    .copied()
                    .filter(|e| e.is_finite() && *e > 0.0)
                    .unwrap_or(0.0),
            ..*d
        })
        .collect();
    let failover = size(pool, profile, &with_failover)?;

    let h = &pool.headroom;
    let mut short = 0;
    let mut role = |base: u32, fo: u32, prev: u32| -> u32 {
        let needed = fo.saturating_sub(base).saturating_sub(h.hot_spares);
        short += needed.saturating_sub(h.warm_spares);
        needed.min(h.warm_spares).max(prev.min(h.warm_spares))
    };
    let base_desired = |floor: u32| floor + h.failure_domain_k + h.maintenance_slots + h.hot_spares;
    let (floor, loaded, desired, min_available) = if pool.disaggregation.enabled {
        let (bp, bd) = (base.floor.prefill, base.floor.decode);
        let (fp, fd) = (failover.floor.prefill, failover.floor.decode);
        let lp = role(bp, fp, previous.prefill);
        let ld = role(bd, fd, previous.decode);
        let (dp, dd) = (base_desired(bp) + lp, base_desired(bd) + ld);
        (
            base.floor,
            RoleReplicas::disaggregated(lp, ld),
            RoleReplicas::disaggregated(dp, dd),
            RoleReplicas::disaggregated(
                (fp + h.failure_domain_k).min(dp - h.maintenance_slots),
                (fd + h.failure_domain_k).min(dd - h.maintenance_slots),
            ),
        )
    } else {
        let b = base.floor.aggregated;
        let f = failover.floor.aggregated;
        let l = role(b, f, previous.aggregated);
        let d = base_desired(b) + l;
        (
            base.floor,
            RoleReplicas::aggregated(l),
            RoleReplicas::aggregated(d),
            RoleReplicas::aggregated((f + h.failure_domain_k).min(d - h.maintenance_slots)),
        )
    };
    let mut sizing = Sizing {
        floor,
        desired,
        min_available,
        allocated_wu_per_sec: base.allocated_wu_per_sec,
        shortfall: None,
        warm_loaded: loaded,
        failover_shortfall: short,
    };
    apply_max_replicas(pool, &mut sizing);
    Ok(sizing)
}

/// Scale replicas down to `max` in total, keeping the prefill:decode ratio roughly intact.
fn cap_total(r: RoleReplicas, max: u32) -> RoleReplicas {
    if r.aggregated > 0 || (r.prefill == 0 && r.decode == 0) {
        return RoleReplicas::aggregated(r.aggregated.min(max));
    }
    let prefill = ((r.prefill as f64 / r.total as f64) * max as f64).round() as u32;
    let prefill = prefill.clamp(u32::from(max >= 2), max.saturating_sub(1).max(1));
    RoleReplicas::disaggregated(prefill.min(max), max.saturating_sub(prefill))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pt_core::Coefficients;
    use pt_crds::pool::{Disaggregation, EngineSpec, Headroom, Payg};
    use pt_crds::profile::{Backend, EngineVersion, Parallelism, RoleCapacity};

    fn cap(i: f64, a: f64, s: f64) -> TierCapacity {
        TierCapacity {
            interactive: i,
            agentic: a,
            standard: s,
        }
    }

    fn profile() -> PerformanceProfileSpec {
        PerformanceProfileSpec {
            model: "m".into(),
            gpu_class: "B200".into(),
            engine: EngineVersion {
                backend: Backend::Trtllm,
                version: "1.2".into(),
            },
            parallelism: Parallelism {
                tp: 8,
                pp: 1,
                ep: 1,
            },
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 3.0,
                d: 0.0,
            },
            decode_modifiers: Default::default(),
            capacity: cap(40_000.0, 35_000.0, 60_000.0),
            role_capacity: None,
        }
    }

    fn pool() -> ModelPoolSpec {
        ModelPoolSpec {
            model: "m".into(),
            profile_ref: "p".into(),
            engine: EngineSpec {
                backend: Backend::Trtllm,
                version: "1.2".into(),
                image: "img".into(),
                extra_args: vec![],
            },
            isolation: Default::default(),
            disaggregation: Disaggregation::default(),
            headroom: Headroom {
                failure_domain_k: 2,
                maintenance_slots: 1,
                hot_spares: 1,
                warm_spares: 0,
            },
            target_utilization: 0.8,
            burst_z: 2.0,
            payg: Payg::default(),
            max_replicas: None,
        }
    }

    fn demand(wu: f64, tier: Tier, burst: f64) -> Demand {
        Demand {
            wu_per_sec: wu,
            tier,
            burst_factor: burst,
        }
    }

    #[test]
    fn steady_demand_single_tier() {
        // 100k WU/s interactive = 2.5 replica-units; / 0.8 = 3.125 → 4.
        let s = size(
            &pool(),
            &profile(),
            &[demand(100_000.0, Tier::Interactive, 1.0)],
        )
        .unwrap();
        assert_eq!(s.floor, RoleReplicas::aggregated(4));
        assert_eq!(s.desired, RoleReplicas::aggregated(4 + 2 + 1 + 1));
        assert_eq!(s.min_available, RoleReplicas::aggregated(6));
        assert_eq!(s.allocated_wu_per_sec, 100_000.0);
        assert!(s.shortfall.is_none());
    }

    #[test]
    fn exact_fit_does_not_round_up() {
        // 48k standard = 0.8 units; / 0.8 = exactly 1.
        let s = size(
            &pool(),
            &profile(),
            &[demand(48_000.0, Tier::Standard, 1.0)],
        )
        .unwrap();
        assert_eq!(s.floor.total, 1);
    }

    #[test]
    fn mixed_tiers_use_each_tiers_capacity() {
        // 35k agentic = 1 unit, 60k standard = 1 unit → 2 / 0.8 = 2.5 → 3.
        let s = size(
            &pool(),
            &profile(),
            &[
                demand(35_000.0, Tier::Agentic, 1.0),
                demand(60_000.0, Tier::Standard, 1.0),
            ],
        )
        .unwrap();
        assert_eq!(s.floor.total, 3);
    }

    #[test]
    fn burst_allowance_grows_with_root_sum_square() {
        // Two tenants each 1 unit with burst factor 2: burst = 2·sqrt(1² + 1²) = 2.83.
        // (2 + 2.83) / 0.8 = 6.04 → 7. Linear addition would give (2 + 4)/0.8 → 8.
        let d = [
            demand(40_000.0, Tier::Interactive, 2.0),
            demand(40_000.0, Tier::Interactive, 2.0),
        ];
        assert_eq!(size(&pool(), &profile(), &d).unwrap().floor.total, 7);
    }

    #[test]
    fn empty_pool_keeps_headroom_only() {
        let s = size(&pool(), &profile(), &[]).unwrap();
        assert_eq!(s.floor.total, 0);
        assert_eq!(s.desired.total, 4);
    }

    #[test]
    fn disaggregated_roles_sized_separately() {
        let mut p = pool();
        p.disaggregation = Disaggregation {
            enabled: true,
            prefill_share: 0.3,
            prefill_replicas_min: 2,
            ..Default::default()
        };
        let mut prof = profile();
        prof.role_capacity = Some(RoleCapacity {
            prefill: cap(120_000.0, 100_000.0, 150_000.0),
            decode: cap(20_000.0, 18_000.0, 30_000.0),
        });
        // 200k interactive: prefill 60k/120k = 0.5 → /0.8 → 1, raised to min 2.
        // decode 140k/20k = 7 → /0.8 = 8.75 → 9.
        let s = size(&p, &prof, &[demand(200_000.0, Tier::Interactive, 1.0)]).unwrap();
        assert_eq!(s.floor, RoleReplicas::disaggregated(2, 9));
        assert_eq!(s.desired, RoleReplicas::disaggregated(6, 13));
        assert_eq!(s.min_available, RoleReplicas::disaggregated(4, 11));
    }

    #[test]
    fn max_replicas_caps_and_reports_shortfall() {
        let mut p = pool();
        p.max_replicas = Some(5);
        let s = size(&p, &profile(), &[demand(100_000.0, Tier::Interactive, 1.0)]).unwrap();
        assert_eq!(s.desired.total, 5);
        assert_eq!(s.shortfall, Some(Shortfall { needed: 8, max: 5 }));
    }

    #[test]
    fn missing_tier_capacity_is_an_error() {
        let mut prof = profile();
        prof.capacity.agentic = 0.0;
        let err = size(&pool(), &prof, &[demand(1.0, Tier::Agentic, 1.0)]).unwrap_err();
        assert_eq!(
            err,
            SizingError::NoCapacity {
                role: "aggregated",
                tier: Tier::Agentic
            }
        );
    }

    #[test]
    fn failover_loads_warm_spares_beyond_hot_spares() {
        let mut p = pool(); // k 2, maintenance 1, hot 1
        p.headroom.warm_spares = 3;
        let d = [demand(100_000.0, Tier::Interactive, 1.0)]; // floor 4
        let none = RoleReplicas::default();

        // No failover demand: the same as `size`.
        let s = size_with_failover(&p, &profile(), &d, &[0.0], none).unwrap();
        assert_eq!(s, size(&p, &profile(), &d).unwrap());
        assert_eq!(s.warm_loaded.total, 0);

        // +32k WU/s: floor 4 → 5. The hot spare covers it; no warm spare loads.
        let s = size_with_failover(&p, &profile(), &d, &[32_000.0], none).unwrap();
        assert_eq!(s.warm_loaded.total, 0);
        assert_eq!(s.desired.total, 8);
        assert_eq!(
            s.min_available.total,
            5 + 2,
            "hot spare now carries provisioned work"
        );

        // +100k: floor 4 → 7. One hot spare, then two warm spares.
        let s = size_with_failover(&p, &profile(), &d, &[100_000.0], none).unwrap();
        assert_eq!(s.floor.total, 4, "the floor stays the normal floor");
        assert_eq!(s.warm_loaded, RoleReplicas::aggregated(2));
        assert_eq!(s.desired.total, 8 + 2);
        assert_eq!(s.min_available.total, 9);
        assert_eq!(s.failover_shortfall, 0);

        // Ramping down: the loaded spares are held while any failover demand remains.
        let s =
            size_with_failover(&p, &profile(), &d, &[1.0], RoleReplicas::aggregated(2)).unwrap();
        assert_eq!(s.warm_loaded.total, 2);
        // Over: released.
        let s = size_with_failover(&p, &profile(), &d, &[], RoleReplicas::aggregated(2)).unwrap();
        assert_eq!(s.warm_loaded.total, 0);
        assert_eq!(s.desired.total, 8);

        // +300k: 400k / 40k / 0.8 = 12.5, so floor 4 → 13. Hot 1 + warm 3 cover 4 of 9;
        // 5 short.
        let s = size_with_failover(&p, &profile(), &d, &[300_000.0], none).unwrap();
        assert_eq!(s.warm_loaded.total, 3);
        assert_eq!(s.failover_shortfall, 5);
        assert_eq!(
            s.min_available.total,
            11 - 1,
            "never above desired − maintenance"
        );

        // maxReplicas still caps.
        p.max_replicas = Some(9);
        let s = size_with_failover(&p, &profile(), &d, &[100_000.0], none).unwrap();
        assert_eq!(s.desired.total, 9);
        assert_eq!(s.shortfall, Some(Shortfall { needed: 10, max: 9 }));
    }

    #[test]
    fn failover_warm_spares_per_role_when_disaggregated() {
        let mut p = pool();
        p.headroom.warm_spares = 2;
        p.disaggregation = Disaggregation {
            enabled: true,
            prefill_share: 0.3,
            ..Default::default()
        };
        let mut prof = profile();
        prof.role_capacity = Some(RoleCapacity {
            prefill: cap(120_000.0, 100_000.0, 150_000.0),
            decode: cap(20_000.0, 18_000.0, 30_000.0),
        });
        let d = [demand(100_000.0, Tier::Interactive, 1.0)];
        // Base: prefill 30k/120k/0.8 → 1, decode 70k/20k/0.8 = 4.375 → 5.
        // Failover +100k: prefill 60k → 1, decode 140k → 9. Decode needs 4 − 1 hot = 3.
        let s = size_with_failover(&p, &prof, &d, &[100_000.0], RoleReplicas::default()).unwrap();
        assert_eq!(s.warm_loaded, RoleReplicas::disaggregated(0, 2));
        assert_eq!(s.failover_shortfall, 1);
        assert_eq!(s.desired, RoleReplicas::disaggregated(1 + 4, 5 + 4 + 2));
    }

    #[test]
    fn cap_total_keeps_both_roles() {
        let r = cap_total(RoleReplicas::disaggregated(6, 13), 10);
        assert_eq!(r.total, 10);
        assert!(r.prefill >= 1 && r.decode >= 1);
        assert_eq!(r.prefill, 3);
    }
}
