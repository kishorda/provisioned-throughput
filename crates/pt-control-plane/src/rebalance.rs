//! Moving a reservation's split toward where its demand is (docs/07 §3, ADR-024).
//!
//! A reservation with shares in several regions is sold as a fixed split, but a global
//! endpoint sends traffic wherever clients are. When demand doesn't match the split, one
//! region throttles while another idles. The leader moves the *effective* split (what
//! gateways enforce) toward demand. The contract, total CUs, and price don't change.
//!
//! [`target_split`] is deterministic and starts from the contract every time, so the split
//! moves back when demand evens out. It moves one CU at a time from the region with the
//! least demand per CU to the one with the most, while that lowers the busiest region's
//! load by at least [`MIN_GAIN`], within `max_shift` CUs of the contract and never below
//! 1 CU in a region.

use std::collections::HashMap;

use pt_core::{Outcome, UsageRecord};

use crate::model::RegionShare;

/// A move must leave the receiver at least this much busier than the donor would be, so
/// small imbalances don't churn the split.
pub const MIN_GAIN: f64 = 1.1;

/// Attempted WU of one request: what it cost if it ran, its estimate if it was refused.
/// Throttled requests count, so a region that's rejecting traffic shows its real demand.
pub fn attempted_wu(r: &UsageRecord) -> f64 {
    match r.outcome {
        Outcome::Rejected(_) => r.wu_estimated,
        _ if r.wu_actual > 0.0 => r.wu_actual,
        _ => r.wu_estimated,
    }
}

/// The split `contract` should move to for `demand` (WU per region), moving at most
/// `max_shift` CUs away from the contract.
pub fn target_split(
    contract: &[RegionShare],
    demand: &HashMap<String, f64>,
    max_shift: u32,
) -> Vec<RegionShare> {
    let mut split: Vec<RegionShare> = contract.to_vec();
    let d = |r: &RegionShare| demand.get(&r.region).copied().unwrap_or(0.0).max(0.0);
    let base = |r: &RegionShare| {
        contract
            .iter()
            .find(|c| c.region == r.region)
            .map_or(0, |c| c.cus)
    };
    let mut moved = 0;
    while moved < max_shift {
        // The busiest region that may still grow.
        let receiver = (0..split.len())
            .filter(|&i| split[i].cus < base(&split[i]) + max_shift)
            .max_by(|&a, &b| {
                let (ua, ub) = (
                    d(&split[a]) / f64::from(split[a].cus),
                    d(&split[b]) / f64::from(split[b].cus),
                );
                ua.total_cmp(&ub)
            });
        let Some(r) = receiver else { break };
        // The quietest other region that may still shrink.
        let donor = (0..split.len())
            .filter(|&i| i != r)
            .filter(|&i| split[i].cus > base(&split[i]).saturating_sub(max_shift).max(1))
            .min_by(|&a, &b| {
                let (ua, ub) = (
                    d(&split[a]) / f64::from(split[a].cus),
                    d(&split[b]) / f64::from(split[b].cus),
                );
                ua.total_cmp(&ub)
            });
        let Some(k) = donor else { break };
        // Move only if the donor, one CU lighter, is still clearly less loaded than the
        // receiver is now: that lowers the highest load by a margin worth a change.
        let receiver_load = d(&split[r]) / f64::from(split[r].cus);
        let donor_after = d(&split[k]) / f64::from(split[k].cus - 1);
        if donor_after * MIN_GAIN >= receiver_load {
            break;
        }
        split[r].cus += 1;
        split[k].cus -= 1;
        moved += 1;
    }
    split
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shares(v: &[(&str, u32)]) -> Vec<RegionShare> {
        v.iter()
            .map(|(r, c)| RegionShare {
                region: r.to_string(),
                cus: *c,
            })
            .collect()
    }

    fn demand(v: &[(&str, f64)]) -> HashMap<String, f64> {
        v.iter().map(|(r, d)| (r.to_string(), *d)).collect()
    }

    #[test]
    fn moves_toward_demand_within_the_budget() {
        let contract = shares(&[("eu-west", 6), ("eu-central", 4)]);
        // Demand 20/80: ideally 2/8, but only 2 CUs (20% of 10) may move.
        let t = target_split(
            &contract,
            &demand(&[("eu-west", 20.0), ("eu-central", 80.0)]),
            2,
        );
        assert_eq!(t, shares(&[("eu-west", 4), ("eu-central", 6)]));
        // A smaller imbalance moves less: 55/45 over 6/4 is already close.
        let t = target_split(
            &contract,
            &demand(&[("eu-west", 55.0), ("eu-central", 45.0)]),
            2,
        );
        assert_eq!(
            t,
            shares(&[("eu-west", 6), ("eu-central", 4)]),
            "no move helps"
        );
        // Balanced demand keeps the contract, whatever the previous split was.
        let t = target_split(
            &contract,
            &demand(&[("eu-west", 60.0), ("eu-central", 40.0)]),
            2,
        );
        assert_eq!(t, contract);
    }

    #[test]
    fn keeps_at_least_one_cu_and_handles_idle_regions() {
        let contract = shares(&[("a", 2), ("b", 8)]);
        // All demand in b: a can give only 1 (it keeps 1 CU).
        let t = target_split(&contract, &demand(&[("b", 100.0)]), 5);
        assert_eq!(t, shares(&[("a", 1), ("b", 9)]));
        // No demand at all: unchanged.
        assert_eq!(target_split(&contract, &HashMap::new(), 5), contract);
        // Budget 0: unchanged.
        assert_eq!(
            target_split(&contract, &demand(&[("a", 100.0)]), 0),
            contract
        );
    }

    #[test]
    fn three_regions_balance_load() {
        let contract = shares(&[("a", 4), ("b", 4), ("c", 4)]);
        // Demand 6:3:3 per the same CU count; up to 2 CUs may move.
        let t = target_split(
            &contract,
            &demand(&[("a", 60.0), ("b", 30.0), ("c", 30.0)]),
            2,
        );
        assert_eq!(t.iter().map(|s| s.cus).sum::<u32>(), 12, "total unchanged");
        assert_eq!(t[0].cus, 6);
        assert_eq!((t[1].cus, t[2].cus), (3, 3));
    }
}
