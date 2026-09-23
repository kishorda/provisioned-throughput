//! Output-length prediction (docs/04 §3).
//!
//! `decode_est = min(max_tokens, P90_output)`, where P90 comes from a streaming quantile
//! sketch of each deployment's actual output lengths. Before enough samples arrive, the
//! declared shape's output p95 is used.

use std::collections::BTreeMap;

/// Log-bucketed quantile sketch with bounded relative error (DDSketch-style).
#[derive(Debug, Clone)]
pub struct QuantileSketch {
    gamma_ln: f64,
    buckets: BTreeMap<i32, u64>,
    zeros: u64,
    count: u64,
}

impl QuantileSketch {
    /// `relative_accuracy` of 0.02 means quantiles are within ±2% of the true value.
    pub fn new(relative_accuracy: f64) -> Self {
        let gamma = (1.0 + relative_accuracy) / (1.0 - relative_accuracy);
        Self {
            gamma_ln: gamma.ln(),
            buckets: BTreeMap::new(),
            zeros: 0,
            count: 0,
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn add(&mut self, value: f64) {
        self.count += 1;
        if value <= 0.0 {
            self.zeros += 1;
            return;
        }
        let idx = (value.ln() / self.gamma_ln).ceil() as i32;
        *self.buckets.entry(idx).or_insert(0) += 1;
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let rank = (q.clamp(0.0, 1.0) * (self.count - 1) as f64).floor() as u64;
        if rank < self.zeros {
            return Some(0.0);
        }
        let mut seen = self.zeros;
        for (&idx, &n) in &self.buckets {
            seen += n;
            if seen > rank {
                // Midpoint of the bucket (gamma^(i-1), gamma^i] in log space, scaled so the
                // relative error is symmetric.
                let gamma = self.gamma_ln.exp();
                return Some(2.0 * (idx as f64 * self.gamma_ln).exp() / (gamma + 1.0));
            }
        }
        None
    }
}

/// Per-deployment output-length predictor.
#[derive(Debug, Clone)]
pub struct OutputEstimator {
    sketch: QuantileSketch,
    fallback: u64,
    min_samples: u64,
    quantile: f64,
}

impl OutputEstimator {
    /// `fallback` is the declared shape's output p95.
    pub fn new(fallback: u64) -> Self {
        Self {
            sketch: QuantileSketch::new(0.02),
            fallback,
            min_samples: 20,
            quantile: 0.9,
        }
    }

    pub fn record(&mut self, output_tokens: u64) {
        self.sketch.add(output_tokens as f64);
    }

    /// Predicted decode tokens, capped by the request's `max_tokens` when present.
    pub fn estimate(&self, max_tokens: Option<u64>) -> u64 {
        let predicted = if self.sketch.count() >= self.min_samples {
            self.sketch
                .quantile(self.quantile)
                .map_or(self.fallback, |v| v.ceil() as u64)
        } else {
            self.fallback
        };
        max_tokens.map_or(predicted, |m| predicted.min(m))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_quantiles_within_accuracy() {
        let mut s = QuantileSketch::new(0.02);
        for v in 1..=10_000 {
            s.add(v as f64);
        }
        for (q, want) in [(0.5, 5_000.0), (0.9, 9_000.0), (0.99, 9_900.0)] {
            let got = s.quantile(q).unwrap();
            assert!((got - want).abs() / want < 0.025, "q{q}: {got}");
        }
    }

    #[test]
    fn sketch_handles_zeros_and_empty() {
        let mut s = QuantileSketch::new(0.02);
        assert_eq!(s.quantile(0.5), None);
        s.add(0.0);
        s.add(0.0);
        s.add(100.0);
        assert_eq!(s.quantile(0.0), Some(0.0));
        assert!((s.quantile(1.0).unwrap() - 100.0).abs() < 2.5);
    }

    #[test]
    fn estimator_uses_fallback_until_warm() {
        let mut e = OutputEstimator::new(500);
        assert_eq!(e.estimate(None), 500);
        assert_eq!(e.estimate(Some(200)), 200);
        for _ in 0..20 {
            e.record(100);
        }
        let est = e.estimate(None);
        assert!((98..=102).contains(&est), "{est}");
        assert_eq!(e.estimate(Some(50)), 50);
    }
}
