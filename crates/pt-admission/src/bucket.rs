//! Debt-based WU token bucket (ADR-002).

use std::time::{Duration, Instant};

/// A token bucket in WU that refills at the entitlement rate and may go into bounded debt.
///
/// Requests are admitted on an estimate. When the request completes, the difference between
/// actual and estimated WU is settled into the bucket, so long-run consumption converges on
/// the entitlement even when individual estimates are wrong.
#[derive(Debug, Clone)]
pub struct DebtBucket {
    rate: f64,
    capacity: f64,
    max_debt: f64,
    level: f64,
    last: Instant,
}

impl DebtBucket {
    /// `rate` in WU/s. `capacity` and `max_debt` in WU. Starts full.
    pub fn new(rate: f64, capacity: f64, max_debt: f64, now: Instant) -> Self {
        Self {
            rate,
            capacity,
            max_debt,
            level: capacity,
            last: now,
        }
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }

    pub fn capacity(&self) -> f64 {
        self.capacity
    }

    /// Current level after refilling to `now`. Negative means the bucket is in debt.
    pub fn level(&mut self, now: Instant) -> f64 {
        self.refill(now);
        self.level
    }

    /// Refill to `now` and return the WU that overflowed capacity. Overflow is unused
    /// entitlement, which accrues as burst credit.
    pub fn refill(&mut self, now: Instant) -> f64 {
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = self.last.max(now);
        let filled = self.level + self.rate * dt;
        let overflow = (filled - self.capacity.max(self.level)).max(0.0);
        self.level = filled.min(self.capacity);
        overflow
    }

    /// Whether a request estimated at `wu` can be admitted now.
    ///
    /// The bucket must hold a positive balance, and admitting must not push it past the debt
    /// bound. A full bucket admits any request, so one larger than `capacity + max_debt`
    /// isn't starved forever; the debt it creates is repaid from refill.
    pub fn can_admit(&self, wu: f64) -> bool {
        self.level > 0.0 && (self.level - wu >= -self.max_debt || self.level >= self.capacity)
    }

    /// Time until `can_admit(wu)` would become true, assuming no other traffic.
    pub fn time_until_admit(&self, wu: f64) -> Duration {
        if self.can_admit(wu) {
            return Duration::ZERO;
        }
        // Smallest level that satisfies can_admit: just above zero, and either enough to stay
        // within the debt bound or a full bucket.
        let target = (wu - self.max_debt).min(self.capacity).max(f64::EPSILON);
        let secs = ((target - self.level) / self.rate).max(0.0);
        Duration::from_secs_f64(secs)
    }

    pub fn debit(&mut self, wu: f64) {
        self.level -= wu;
    }

    /// Apply `actual − estimated`. A positive delta deepens debt, a negative one refunds.
    pub fn settle(&mut self, delta: f64) {
        self.level = (self.level - delta).min(self.capacity);
    }

    /// A strict variant for rate limiters: can take `wu` without any debt.
    /// Requests larger than capacity need a full bucket.
    pub fn can_take_strict(&self, wu: f64) -> bool {
        self.level >= wu.min(self.capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(start: Instant, secs: f64) -> Instant {
        start + Duration::from_secs_f64(secs)
    }

    #[test]
    fn refills_to_capacity_and_reports_overflow() {
        let t0 = Instant::now();
        let mut b = DebtBucket::new(100.0, 100.0, 200.0, t0);
        b.debit(100.0);
        assert_eq!(b.refill(at(t0, 0.5)), 0.0);
        assert!((b.level(at(t0, 0.5)) - 50.0).abs() < 1e-9);
        // 1.5 s later: +150, of which 100 overflows capacity.
        let overflow = b.refill(at(t0, 2.0));
        assert!((overflow - 100.0).abs() < 1e-9);
        assert!((b.level(at(t0, 2.0)) - 100.0).abs() < 1e-9);
    }

    #[test]
    fn admits_into_bounded_debt_only() {
        let t0 = Instant::now();
        let mut b = DebtBucket::new(100.0, 100.0, 200.0, t0);
        assert!(b.can_admit(250.0)); // full bucket
        b.debit(250.0); // level −150
        assert!(!b.can_admit(1.0)); // in debt: no positive balance
        let wait = b.time_until_admit(1.0);
        assert!((wait.as_secs_f64() - 1.5).abs() < 1e-6);
        b.refill(at(t0, 1.6)); // level +10
        assert!(b.can_admit(150.0)); // 10 − 150 = −140 ≥ −200
        assert!(!b.can_admit(250.0)); // would breach the debt bound
    }

    #[test]
    fn huge_request_waits_for_full_bucket() {
        let t0 = Instant::now();
        let mut b = DebtBucket::new(100.0, 100.0, 200.0, t0);
        b.debit(50.0);
        assert!(!b.can_admit(1_000.0));
        assert!((b.time_until_admit(1_000.0).as_secs_f64() - 0.5).abs() < 1e-6);
        b.refill(at(t0, 0.5));
        assert!(b.can_admit(1_000.0));
    }

    #[test]
    fn settlement_refunds_and_charges() {
        let t0 = Instant::now();
        let mut b = DebtBucket::new(100.0, 100.0, 200.0, t0);
        b.debit(80.0);
        b.settle(-30.0); // actual was 30 less than estimated
        assert!((b.level(t0) - 50.0).abs() < 1e-9);
        b.settle(120.0); // actual was 120 more
        assert!((b.level(t0) + 70.0).abs() < 1e-9);
        b.settle(-1_000.0); // refunds never exceed capacity
        assert!((b.level(t0) - 100.0).abs() < 1e-9);
    }

    #[test]
    fn long_run_consumption_converges_on_entitlement() {
        // Estimates are 50% too low, but settlement keeps a greedy client near the entitlement.
        let t0 = Instant::now();
        let rate = 1_000.0;
        let mut b = DebtBucket::new(rate, rate, 2.0 * rate, t0);
        let (est, actual) = (100.0, 150.0);
        let mut consumed = 0.0;
        let horizon = 60.0;
        let mut t = 0.0;
        while t < horizon {
            let now = at(t0, t);
            b.refill(now);
            if b.can_admit(est) {
                b.debit(est);
                b.settle(actual - est);
                consumed += actual;
            }
            t += 0.001;
        }
        let allowed = rate * horizon + rate; // plus the initial full bucket
        assert!(
            consumed <= allowed * 1.02 + 3.0 * rate,
            "consumed {consumed}"
        );
        assert!(consumed >= rate * horizon * 0.95, "consumed {consumed}");
    }
}
