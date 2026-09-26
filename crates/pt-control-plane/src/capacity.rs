//! What a CU costs a region's pool, per tier (docs/06 §2, ADR-031).
//!
//! A pool's sellable capacity is counted in replicas, the unit the capacity controller
//! deploys. A CU is a fixed WU/s, but a replica delivers fewer WU/s at a tighter latency
//! tier: in the development profile, 58,000 WU/s at Standard and 35,500 at Agentic. So an
//! Agentic CU needs about 1.6 times the hardware of a Standard one. The cost of one CU at
//! tier T in a pool is:
//!
//! ```text
//! replicas per CU = wu_per_cu ÷ (profile capacity at T × target utilisation)
//! ```
//!
//! This is the same target utilisation quotes use ([`crate::quote::TARGET_UTILISATION`]),
//! and it matches how the operator sizes a pool's floor. Counters are in micro-replicas
//! (integers, so SQL arithmetic is exact). Each CU's cost is rounded *up*, so rounding never
//! sells more than the pool holds.

use std::collections::HashMap;

use pt_core::Tier;

use crate::config::ControlPlaneConfig;
use crate::model::RegionShare;
use crate::quote::TARGET_UTILISATION;

/// Micro-replicas in a replica.
pub const MICRO: u64 = 1_000_000;

/// Micro-replicas one CU needs at a tier whose replicas each deliver `capacity_wu_per_s`.
pub fn micro_per_cu(wu_per_cu: f64, capacity_wu_per_s: f64) -> u64 {
    (MICRO as f64 * wu_per_cu / (capacity_wu_per_s * TARGET_UTILISATION)).ceil() as u64
}

/// Each pool's cost per CU, per tier, from its profile.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Costs {
    per_cu: HashMap<(String, String, Tier), u64>,
}

const TIERS: [Tier; 3] = [Tier::Interactive, Tier::Agentic, Tier::Standard];

impl Costs {
    /// From each `[[capacity]]` entry's profile and `telemetry.wu_per_cu`. The config must
    /// be valid (every pool names a profile with positive capacity per tier).
    pub fn from_config(config: &ControlPlaneConfig) -> Self {
        let mut per_cu = HashMap::new();
        for c in &config.capacity {
            let Some(profile) = config.profile(&c.profile) else {
                continue;
            };
            for tier in TIERS {
                let cap = profile.capacity_wu_per_s.for_tier(tier);
                if cap > 0.0 {
                    per_cu.insert(
                        (c.region.clone(), c.model.clone(), tier),
                        micro_per_cu(config.telemetry.wu_per_cu, cap),
                    );
                }
            }
        }
        Self { per_cu }
    }

    /// Set one pool's cost per CU at a tier (for tests and tools).
    pub fn insert(&mut self, region: &str, model: &str, tier: Tier, micro_per_cu: u64) {
        self.per_cu
            .insert((region.into(), model.into(), tier), micro_per_cu);
    }

    /// Micro-replicas one CU of `model` at `tier` needs in `region`.
    pub fn per_cu(&self, region: &str, model: &str, tier: Tier) -> Option<u64> {
        self.per_cu
            .get(&(region.to_string(), model.to_string(), tier))
            .copied()
    }

    /// Micro-replicas `share` needs, or `None` if the pool doesn't exist.
    pub fn share(&self, model: &str, tier: Tier, share: &RegionShare) -> Option<u64> {
        self.per_cu(&share.region, model, tier)
            .map(|c| c * u64::from(share.cus))
    }

    /// Whole CUs at `tier` that `micro` micro-replicas can hold in `region`.
    pub fn cus_in(&self, region: &str, model: &str, tier: Tier, micro: u64) -> Option<u32> {
        self.per_cu(region, model, tier)
            .map(|c| (micro / c).min(u64::from(u32::MAX)) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cu_costs_more_at_a_tighter_tier() {
        // The development B200 profile: 58,000 / 35,500 WU/s per replica.
        let standard = micro_per_cu(1_000.0, 58_000.0);
        let agentic = micro_per_cu(1_000.0, 35_500.0);
        assert_eq!(standard, 21_552); // 1000 / (58000 × 0.8) = 0.02155 replicas, rounded up
        assert_eq!(agentic, 35_212);
        assert!((agentic as f64 / standard as f64 - 58.0 / 35.5).abs() < 0.001);
        // Eight replicas hold 371 Standard CUs or 227 Agentic ones.
        assert_eq!(8 * MICRO / standard, 371);
        assert_eq!(8 * MICRO / agentic, 227);
    }
}
