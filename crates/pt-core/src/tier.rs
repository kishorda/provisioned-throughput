//! SLO tiers and CU pricing (docs/02 §3, docs/11 §4).

use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    Interactive,
    Agentic,
    Standard,
}

impl Tier {
    /// p95 TPOT target in seconds. Used to estimate KV residency at admission.
    /// Placeholder until calibration runs.
    pub fn tpot_target_s(self) -> f64 {
        match self {
            Tier::Interactive => 0.040,
            Tier::Agentic => 0.030,
            Tier::Standard => 0.080,
        }
    }

    /// CU price multiplier relative to the Standard base price.
    pub fn price_multiplier(self) -> f64 {
        match self {
            Tier::Standard => 1.0,
            Tier::Interactive => 1.25,
            Tier::Agentic => 1.5,
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolIsolation {
    #[default]
    Shared,
    /// Dedicated workers, idle capacity backfilled with preemptible PAYG.
    Dedicated,
    /// Dedicated workers with no backfill.
    StrictDedicated,
}

/// Surcharge for strict-dedicated, as a multiple of the base (Standard) CU price.
pub const STRICT_DEDICATED_SURCHARGE: f64 = 0.3;

/// CU price as a multiple of the Standard base price.
pub fn cu_price_multiplier(tier: Tier, isolation: PoolIsolation) -> f64 {
    let surcharge = match isolation {
        PoolIsolation::StrictDedicated => STRICT_DEDICATED_SURCHARGE,
        PoolIsolation::Shared | PoolIsolation::Dedicated => 0.0,
    };
    tier.price_multiplier() + surcharge
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_dedicated_adds_base_surcharge() {
        let cases = [
            (Tier::Standard, 1.3),
            (Tier::Interactive, 1.55),
            (Tier::Agentic, 1.8),
        ];
        for (tier, want) in cases {
            let got = cu_price_multiplier(tier, PoolIsolation::StrictDedicated);
            assert!((got - want).abs() < 1e-9, "{tier:?}: {got}");
            assert!(got > cu_price_multiplier(tier, PoolIsolation::Shared));
        }
    }
}
