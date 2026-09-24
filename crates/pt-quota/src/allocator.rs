//! Split one reservation's entitlement across gateways.
//!
//! ```text
//! share_i = floor + fair_i + leftover / n
//!   floor    = E · floor_fraction / n          every active gateway can admit a first request
//!   fair_i   = max-min fair split of E · (1 − floor_fraction), capped at demand_i
//!   leftover = what demand didn't use, split evenly so traffic can ramp anywhere
//! ```
//!
//! Shares always sum to exactly `E`.

pub fn allocate(entitlement: f64, floor_fraction: f64, demands: &[f64]) -> Vec<f64> {
    let n = demands.len();
    if n == 0 || entitlement <= 0.0 {
        return vec![0.0; n];
    }
    let floor_fraction = floor_fraction.clamp(0.0, 1.0);
    let floor = entitlement * floor_fraction / n as f64;
    let pool = entitlement * (1.0 - floor_fraction);

    // Max-min fair water-filling: satisfy the smallest demands first, then split the rest
    // equally among the gateways still wanting more.
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| demands[a].total_cmp(&demands[b]));
    let mut fair = vec![0.0; n];
    let mut remaining = pool;
    for (k, &i) in order.iter().enumerate() {
        let equal = remaining / (n - k) as f64;
        let give = demands[i].max(0.0).min(equal);
        fair[i] = give;
        remaining -= give;
    }
    let leftover_each = remaining / n as f64;
    fair.into_iter()
        .map(|f| floor + f + leftover_each)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: &[f64], b: &[f64]) {
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b) {
            assert!((x - y).abs() < 1e-6, "{a:?} vs {b:?}");
        }
    }

    #[test]
    fn idle_gateways_split_evenly() {
        close(&allocate(1_000.0, 0.1, &[0.0, 0.0]), &[500.0, 500.0]);
    }

    #[test]
    fn busy_gateway_gets_the_pool_idle_one_keeps_its_floor() {
        // Floor 50 each. The pool of 900 goes to the busy gateway.
        close(&allocate(1_000.0, 0.1, &[5_000.0, 0.0]), &[950.0, 50.0]);
    }

    #[test]
    fn max_min_fair_under_contention() {
        // Pool 900: the 100-demand gateway gets 100, the other two split 800.
        close(
            &allocate(1_000.0, 0.1, &[100.0, 2_000.0, 2_000.0]),
            &[
                100.0 + 100.0 / 3.0,
                400.0 + 100.0 / 3.0,
                400.0 + 100.0 / 3.0,
            ],
        );
    }

    #[test]
    fn unused_pool_is_shared() {
        // Demands 100 + 200 of a 900 pool: 600 left, 300 each.
        close(&allocate(1_000.0, 0.1, &[100.0, 200.0]), &[450.0, 550.0]);
    }

    #[test]
    fn always_sums_to_entitlement() {
        for demands in [
            vec![1.0],
            vec![0.0; 7],
            vec![3.0, 1e9, 0.5, 42.0],
            vec![-5.0, 10.0],
        ] {
            let s: f64 = allocate(777.0, 0.2, &demands).iter().sum();
            assert!((s - 777.0).abs() < 1e-6, "{demands:?}: {s}");
        }
        assert!(allocate(0.0, 0.1, &[1.0]).iter().all(|&x| x == 0.0));
        assert!(allocate(10.0, 0.1, &[]).is_empty());
    }
}
