//! Work Unit (WU) cost model, docs/02 §2.
//!
//! ```text
//! WU = a·uncached_prefill + b·cached_prefill + c·decode·m_decode + d·KV_token_seconds
//! ```

use serde::{Deserialize, Serialize};

use crate::tier::Tier;

/// Fitted cost coefficients for one (model, GPU, engine version, parallelism).
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Coefficients {
    /// WU per uncached prefill token (compute-bound).
    pub a: f64,
    /// WU per cached prefill token. Expected to be much smaller than `a`.
    pub b: f64,
    /// WU per decode token (bandwidth-bound).
    pub c: f64,
    /// WU per KV-token-second of cache residency.
    pub d: f64,
}

/// Calibrated profile for one serving pool. Mirrors the `PerformanceProfile` CRD (docs/08 §2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerformanceProfile {
    pub name: String,
    pub coefficients: Coefficients,
    /// WU/s one replica sustains while meeting each tier's SLO.
    #[serde(default)]
    pub capacity_wu_per_s: TierCapacity,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct TierCapacity {
    pub interactive: f64,
    pub agentic: f64,
    pub standard: f64,
}

impl TierCapacity {
    pub fn for_tier(&self, tier: Tier) -> f64 {
        match tier {
            Tier::Interactive => self.interactive,
            Tier::Agentic => self.agentic,
            Tier::Standard => self.standard,
        }
    }
}

/// The measurable quantities a request consumes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkBreakdown {
    pub uncached_prefill_tokens: u64,
    pub cached_prefill_tokens: u64,
    pub decode_tokens: u64,
    pub kv_token_seconds: f64,
    /// Speculative-decoding / structured-output multiplier on decode cost. 1.0 when unused.
    pub decode_modifier: f64,
}

impl WorkBreakdown {
    pub fn new(uncached: u64, cached: u64, decode: u64, kv_token_seconds: f64) -> Self {
        Self {
            uncached_prefill_tokens: uncached,
            cached_prefill_tokens: cached,
            decode_tokens: decode,
            kv_token_seconds,
            decode_modifier: 1.0,
        }
    }

    pub fn prompt_tokens(&self) -> u64 {
        self.uncached_prefill_tokens + self.cached_prefill_tokens
    }
}

impl PerformanceProfile {
    /// WU cost of `work` on this pool.
    pub fn work_units(&self, work: &WorkBreakdown) -> f64 {
        let k = &self.coefficients;
        k.a * work.uncached_prefill_tokens as f64
            + k.b * work.cached_prefill_tokens as f64
            + k.c * work.decode_tokens as f64 * work.decode_modifier
            + k.d * work.kv_token_seconds
    }
}

/// Estimated KV residency for a request before it runs (docs/04 §3).
///
/// Resident KV grows from `input` to `input + decode` over the decode phase, so the average
/// is `input + decode/2`, held for `decode × tpot` seconds.
pub fn estimate_kv_token_seconds(input_tokens: u64, decode_tokens: u64, tpot_s: f64) -> f64 {
    let avg_resident = input_tokens as f64 + decode_tokens as f64 / 2.0;
    avg_resident * decode_tokens as f64 * tpot_s
}

/// Actual KV residency from measured decode duration.
pub fn measured_kv_token_seconds(
    input_tokens: u64,
    decode_tokens: u64,
    decode_duration_s: f64,
) -> f64 {
    let avg_resident = input_tokens as f64 + decode_tokens as f64 / 2.0;
    avg_resident * decode_duration_s.max(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> PerformanceProfile {
        PerformanceProfile {
            name: "test".into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 3.0,
                d: 0.001,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }
    }

    #[test]
    fn wu_weights_each_phase() {
        let w = WorkBreakdown::new(1000, 500, 100, 20_000.0);
        // 1000·1 + 500·0.1 + 100·3 + 20000·0.001
        assert!((profile().work_units(&w) - 1370.0).abs() < 1e-9);
    }

    #[test]
    fn decode_modifier_scales_decode_only() {
        let mut w = WorkBreakdown::new(0, 0, 100, 0.0);
        w.decode_modifier = 0.5;
        assert!((profile().work_units(&w) - 150.0).abs() < 1e-9);
    }

    #[test]
    fn long_context_costs_more_than_equal_tokens_split() {
        // One 100K prompt vs ten 10K prompts, same output each: KV residency makes the long one dearer.
        let p = profile();
        let tpot = 0.04;
        let long = WorkBreakdown::new(
            100_000,
            0,
            200,
            estimate_kv_token_seconds(100_000, 200, tpot),
        );
        let short = WorkBreakdown::new(10_000, 0, 20, estimate_kv_token_seconds(10_000, 20, tpot));
        assert!(p.work_units(&long) > 10.0 * p.work_units(&short));
    }

    #[test]
    fn kv_estimate_uses_average_residency() {
        // (1000 + 100/2) · 100 · 0.04 = 4200
        assert!((estimate_kv_token_seconds(1000, 100, 0.04) - 4200.0).abs() < 1e-9);
        assert!((measured_kv_token_seconds(1000, 100, 4.0) - 4200.0).abs() < 1e-9);
    }
}
