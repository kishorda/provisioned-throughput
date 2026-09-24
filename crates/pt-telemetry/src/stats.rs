//! Small statistics helpers.

use serde::Serialize;

/// Nearest-rank percentile of already-sorted values: the smallest value with at least
/// `q` of the samples at or below it.
pub fn percentile(sorted: &[f64], q: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (q.clamp(0.0, 1.0) * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1])
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Percentiles {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub count: usize,
}

impl Percentiles {
    pub fn of(mut values: Vec<f64>) -> Option<Self> {
        values.retain(|v| v.is_finite());
        values.sort_by(f64::total_cmp);
        Some(Self {
            p50: percentile(&values, 0.50)?,
            p95: percentile(&values, 0.95)?,
            p99: percentile(&values, 0.99)?,
            count: values.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank() {
        let v: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&v, 0.95), Some(95.0));
        assert_eq!(percentile(&v, 0.5), Some(50.0));
        assert_eq!(percentile(&v, 1.0), Some(100.0));
        assert_eq!(percentile(&v, 0.0), Some(1.0));
        assert_eq!(percentile(&[7.0], 0.95), Some(7.0));
        assert_eq!(percentile(&[], 0.5), None);
        // 19 fast, 1 slow: p95 is still fast (the slow one is the top 5%).
        let mut w = vec![1.0; 19];
        w.push(100.0);
        assert_eq!(percentile(&w, 0.95), Some(1.0));
    }
}
