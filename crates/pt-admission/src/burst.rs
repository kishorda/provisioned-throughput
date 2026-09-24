//! Burst credit (docs/04 §4–5). Burst is free within the cap.

use std::time::Instant;

use crate::bucket::DebtBucket;
use crate::policy::BurstPolicy;

/// Banked unused entitlement, spendable above the provisioned rate up to a rate ceiling.
#[derive(Debug, Clone)]
pub struct BurstBank {
    credit: f64,
    max_credit: f64,
    reserve: f64,
    /// Limits burst spending to `(max_rate_multiple − 1) × entitlement`, so provisioned plus
    /// burst never exceeds `max_rate_multiple × entitlement`.
    ceiling: DebtBucket,
}

impl BurstBank {
    /// Starts empty: credit is earned by leaving entitlement unused.
    pub fn new(entitlement_wu_s: f64, policy: &BurstPolicy, now: Instant) -> Self {
        let max_credit = entitlement_wu_s * policy.max_credit_seconds;
        let extra_rate = entitlement_wu_s * (policy.max_rate_multiple - 1.0).max(0.0);
        Self {
            credit: 0.0,
            max_credit,
            reserve: max_credit * policy.continuation_reserve.clamp(0.0, 1.0),
            ceiling: DebtBucket::new(extra_rate, extra_rate, 0.0, now),
        }
    }

    pub fn credit(&self) -> f64 {
        self.credit
    }

    /// A bank for a new entitlement that keeps this one's credit and ceiling level, clamped
    /// to the new limits. Rebuilding with `new` would refill the ceiling on every resize and
    /// let frequent resizes (quota leases) escape the burst rate cap.
    pub fn rescaled(&mut self, entitlement_wu_s: f64, policy: &BurstPolicy, now: Instant) -> Self {
        let mut b = Self::new(entitlement_wu_s, policy, now);
        b.credit = self.credit.min(b.max_credit);
        b.ceiling.set_level(self.ceiling.level(now));
        b
    }

    pub fn accrue(&mut self, wu: f64) {
        self.credit = (self.credit + wu).min(self.max_credit);
    }

    /// Whether `wu` can be drawn now. Non-continuation calls can't dip into the reserve.
    pub fn can_draw(&mut self, wu: f64, continuation: bool, now: Instant) -> bool {
        self.ceiling.refill(now);
        let usable = if continuation {
            self.credit
        } else {
            self.credit - self.reserve
        };
        usable >= wu && self.ceiling.can_take_strict(wu)
    }

    pub fn draw(&mut self, wu: f64) {
        self.credit -= wu;
        self.ceiling.debit(wu);
    }

    /// Apply `actual − estimated` for a burst request.
    pub fn settle(&mut self, delta: f64) {
        self.credit = (self.credit - delta).min(self.max_credit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn policy() -> BurstPolicy {
        BurstPolicy {
            max_credit_seconds: 10.0,
            max_rate_multiple: 2.0,
            continuation_reserve: 0.25,
        }
    }

    #[test]
    fn rescale_keeps_ceiling_level() {
        let t0 = Instant::now();
        let mut b = BurstBank::new(100.0, &policy(), t0);
        b.accrue(1_000.0);
        b.draw(100.0); // ceiling empty
        let mut b = b.rescaled(120.0, &policy(), t0);
        assert!(
            !b.can_draw(10.0, true, t0),
            "a resize must not refill the ceiling"
        );
        assert_eq!(b.credit(), 900.0);
        let b = b.rescaled(10.0, &policy(), t0);
        assert_eq!(b.credit(), 100.0, "clamped to the smaller cap");
    }

    #[test]
    fn credit_is_capped() {
        let t0 = Instant::now();
        let mut b = BurstBank::new(100.0, &policy(), t0);
        b.accrue(5_000.0);
        assert_eq!(b.credit(), 1_000.0);
    }

    #[test]
    fn reserve_is_for_continuations() {
        let t0 = Instant::now();
        let mut b = BurstBank::new(100.0, &policy(), t0);
        b.accrue(300.0); // reserve is 250
        let later = t0 + Duration::from_secs(1);
        assert!(!b.can_draw(60.0, false, later));
        assert!(b.can_draw(50.0, false, later));
        assert!(b.can_draw(100.0, true, later));
    }

    #[test]
    fn rate_ceiling_limits_spending() {
        let t0 = Instant::now();
        let mut b = BurstBank::new(100.0, &policy(), t0);
        b.accrue(1_000.0);
        // Ceiling bucket holds 100 WU (one second of the extra 1× rate).
        assert!(b.can_draw(100.0, true, t0));
        b.draw(100.0);
        assert!(!b.can_draw(10.0, true, t0));
        assert!(b.can_draw(10.0, true, t0 + Duration::from_millis(100)));
    }
}
