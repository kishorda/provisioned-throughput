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

    /// p95 TTFT target for one request, in milliseconds (docs/02 §3). Placeholder until
    /// calibration runs.
    ///
    /// - Interactive: 800 ms up to 8K input tokens, plus 100 ms per additional 8K.
    /// - Agentic: 500 ms when at least half the prompt is a cache hit, 1.5 s otherwise.
    /// - Standard: 3 s.
    pub fn ttft_target_ms(self, input_tokens: u64, cached_tokens: u64) -> f64 {
        match self {
            Tier::Interactive => {
                let extra_blocks = input_tokens.saturating_sub(8_192).div_ceil(8_192);
                800.0 + 100.0 * extra_blocks as f64
            }
            Tier::Agentic if cached_tokens * 2 >= input_tokens && input_tokens > 0 => 500.0,
            Tier::Agentic => 1_500.0,
            Tier::Standard => 3_000.0,
        }
    }

    /// p95 TPOT target in milliseconds. Short outputs (at most 64 tokens) have a tighter
    /// target on Interactive and Agentic, because agents wait on many short tool calls.
    pub fn tpot_target_ms(self, output_tokens: u64) -> f64 {
        let short = output_tokens <= 64;
        match self {
            Tier::Interactive if short => 30.0,
            Tier::Interactive => 40.0,
            Tier::Agentic if short => 20.0,
            Tier::Agentic => 30.0,
            Tier::Standard => 80.0,
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

/// Surcharge for the Multi-region SKU (reserved failover headroom in a paired region), as
/// a multiple of the base (Standard) CU price.
pub const MULTI_REGION_SURCHARGE: f64 = 0.2;

/// CU price as a multiple of the Standard base price: the tier multiplier plus any
/// surcharges. Surcharges add up.
pub fn cu_price_multiplier(tier: Tier, isolation: PoolIsolation, multi_region: bool) -> f64 {
    let isolation_surcharge = match isolation {
        PoolIsolation::StrictDedicated => STRICT_DEDICATED_SURCHARGE,
        PoolIsolation::Shared | PoolIsolation::Dedicated => 0.0,
    };
    let sku_surcharge = if multi_region {
        MULTI_REGION_SURCHARGE
    } else {
        0.0
    };
    tier.price_multiplier() + isolation_surcharge + sku_surcharge
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_targets() {
        assert_eq!(Tier::Interactive.ttft_target_ms(8_192, 0), 800.0);
        assert_eq!(Tier::Interactive.ttft_target_ms(8_193, 0), 900.0);
        assert_eq!(Tier::Interactive.ttft_target_ms(32_768, 0), 1_100.0);
        assert_eq!(Tier::Agentic.ttft_target_ms(10_000, 6_000), 500.0);
        assert_eq!(Tier::Agentic.ttft_target_ms(10_000, 1_000), 1_500.0);
        assert_eq!(Tier::Standard.ttft_target_ms(1, 0), 3_000.0);
        assert_eq!(Tier::Agentic.tpot_target_ms(64), 20.0);
        assert_eq!(Tier::Agentic.tpot_target_ms(65), 30.0);
        assert_eq!(Tier::Standard.tpot_target_ms(10), 80.0);
    }

    #[test]
    fn strict_dedicated_adds_base_surcharge() {
        let cases = [
            (Tier::Standard, 1.3),
            (Tier::Interactive, 1.55),
            (Tier::Agentic, 1.8),
        ];
        for (tier, want) in cases {
            let got = cu_price_multiplier(tier, PoolIsolation::StrictDedicated, false);
            assert!((got - want).abs() < 1e-9, "{tier:?}: {got}");
            assert!(got > cu_price_multiplier(tier, PoolIsolation::Shared, false));
        }
    }

    #[test]
    fn multi_region_adds_base_surcharge_and_stacks() {
        let cases = [
            (Tier::Standard, PoolIsolation::Shared, 1.2),
            (Tier::Interactive, PoolIsolation::Dedicated, 1.45),
            (Tier::Agentic, PoolIsolation::Shared, 1.7),
            (Tier::Agentic, PoolIsolation::StrictDedicated, 2.0),
        ];
        for (tier, isolation, want) in cases {
            let got = cu_price_multiplier(tier, isolation, true);
            assert!((got - want).abs() < 1e-9, "{tier:?} {isolation:?}: {got}");
        }
    }
}
