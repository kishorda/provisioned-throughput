//! Declared workload shape (docs/02 §4).

use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Shape {
    pub input_p95: u64,
    pub input_max: u64,
    pub output_p95: u64,
    pub context_ceiling: u64,
    #[serde(default)]
    pub cache_hit_ratio: f64,
    #[serde(default = "default_burst_factor")]
    pub burst_factor: f64,
}

fn default_burst_factor() -> f64 {
    1.0
}

impl Shape {
    /// Whether a request is in-shape. There is no grace margin: one token over a declared
    /// maximum is out-of-shape.
    pub fn is_in_shape(&self, input_tokens: u64, max_output_tokens: u64) -> bool {
        input_tokens <= self.input_max
            && input_tokens.saturating_add(max_output_tokens) <= self.context_ceiling
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape() -> Shape {
        Shape {
            input_p95: 8_000,
            input_max: 16_000,
            output_p95: 500,
            context_ceiling: 32_000,
            cache_hit_ratio: 0.0,
            burst_factor: 1.0,
        }
    }

    #[test]
    fn no_grace_margin() {
        let s = shape();
        assert!(s.is_in_shape(16_000, 100));
        assert!(!s.is_in_shape(16_001, 100));
        assert!(s.is_in_shape(16_000, 16_000));
        assert!(!s.is_in_shape(16_000, 16_001));
    }
}
