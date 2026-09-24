//! CU pricing (docs/02 §3, docs/11 §4). Amounts are in minor currency units.

use jiff::Timestamp;
use pt_core::{cu_price_multiplier, PoolIsolation, Tier};

use crate::model::{Price, Sku};

/// Monthly price of one CU: base × (tier multiplier + strict-dedicated surcharge +
/// Multi-region surcharge).
pub fn per_cu_monthly(base: u64, tier: Tier, isolation: PoolIsolation, sku: Sku) -> u64 {
    (base as f64 * cu_price_multiplier(tier, isolation, sku == Sku::MultiRegion)).round() as u64
}

pub fn price(
    currency: &str,
    base: u64,
    tier: Tier,
    isolation: PoolIsolation,
    sku: Sku,
    cus: u32,
) -> Price {
    let per_cu = per_cu_monthly(base, tier, isolation, sku);
    Price {
        currency: currency.to_string(),
        per_cu_monthly: per_cu,
        monthly: per_cu * u64::from(cus),
    }
}

/// Charge for adding `added_cus` now, for the rest of the current term.
pub fn prorated_increase(
    per_cu_monthly: u64,
    added_cus: u32,
    term_months: u8,
    term_start: Timestamp,
    term_end: Timestamp,
    now: Timestamp,
) -> u64 {
    let term = term_end.duration_since(term_start).as_secs_f64();
    if term <= 0.0 {
        return 0;
    }
    let remaining = term_end.duration_since(now).as_secs_f64().clamp(0.0, term);
    let full_term = per_cu_monthly as f64 * f64::from(added_cus) * f64::from(term_months);
    (full_term * remaining / term).round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_and_isolation_multipliers() {
        let base = 10_000;
        assert_eq!(
            per_cu_monthly(base, Tier::Standard, PoolIsolation::Shared, Sku::Regional),
            10_000
        );
        assert_eq!(
            per_cu_monthly(
                base,
                Tier::Interactive,
                PoolIsolation::Dedicated,
                Sku::Regional
            ),
            12_500
        );
        assert_eq!(
            per_cu_monthly(base, Tier::Agentic, PoolIsolation::Shared, Sku::Regional),
            15_000
        );
        assert_eq!(
            per_cu_monthly(
                base,
                Tier::Agentic,
                PoolIsolation::StrictDedicated,
                Sku::Regional
            ),
            18_000
        );
        assert_eq!(
            price(
                "USD",
                base,
                Tier::Agentic,
                PoolIsolation::Shared,
                Sku::Regional,
                4
            )
            .monthly,
            60_000
        );
        // Multi-region: + 0.2 × base, on top of any other surcharge.
        assert_eq!(
            per_cu_monthly(base, Tier::Agentic, PoolIsolation::Shared, Sku::MultiRegion),
            17_000
        );
        assert_eq!(
            per_cu_monthly(
                base,
                Tier::Agentic,
                PoolIsolation::StrictDedicated,
                Sku::MultiRegion
            ),
            20_000
        );
    }

    #[test]
    fn increase_is_prorated_over_remaining_term() {
        let start: Timestamp = "2026-10-01T00:00:00Z".parse().unwrap();
        let end: Timestamp = "2027-01-01T00:00:00Z".parse().unwrap(); // 92 days
        let halfway = start + (end.duration_since(start) / 2);
        // 2 CUs × 10,000/month × 3 months = 60,000 for a full term; half remains.
        assert_eq!(prorated_increase(10_000, 2, 3, start, end, halfway), 30_000);
        assert_eq!(prorated_increase(10_000, 2, 3, start, end, start), 60_000);
        assert_eq!(prorated_increase(10_000, 2, 3, start, end, end), 0);
    }
}
