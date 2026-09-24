//! Per-reservation admission: provisioned bucket plus the boundary-policy chain.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pt_core::{RejectReason, TrafficClass};

use crate::bucket::DebtBucket;
use crate::burst::BurstBank;
use crate::policy::BoundaryPolicy;

/// Bucket sizing for one reservation's entitlement.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LimiterConfig {
    /// Entitlement in WU/s (CUs × WU/s per CU), for this gateway's share.
    pub entitlement_wu_s: f64,
    /// Bucket capacity in seconds of entitlement.
    pub capacity_s: f64,
    /// Debt bound in seconds of entitlement (ADR-002 default: 2 s).
    pub max_debt_s: f64,
}

impl LimiterConfig {
    pub fn new(entitlement_wu_s: f64) -> Self {
        Self {
            entitlement_wu_s,
            capacity_s: 1.0,
            max_debt_s: 2.0,
        }
    }
}

/// One admission attempt.
#[derive(Debug, Clone, Copy)]
pub struct AdmitRequest {
    pub estimated_wu: f64,
    /// `x-pt-priority: continuation`. The only intra-tenant priority at launch.
    pub continuation: bool,
    /// When the gateway fully received the request. The queue deadline counts from here.
    pub received_at: Instant,
}

#[derive(Debug)]
pub enum Decision {
    Admit(AdmitTicket),
    /// Wait `wait`, then call [`ReservationLimiter::admit`] again passing the slot back.
    Queue(QueueSlot),
    Reject {
        reason: RejectReason,
        retry_after: Duration,
    },
}

/// Proof of admission, needed to settle actual WU.
#[derive(Debug)]
#[must_use = "settle the ticket with actual WU when the request completes"]
pub struct AdmitTicket {
    pub class: TrafficClass,
    pub estimated_wu: f64,
    pub queued_for: Duration,
}

/// A place in the gateway queue. Dropping it (for example when the client disconnects
/// while waiting) releases its share of the queue depth.
#[derive(Debug)]
pub struct QueueSlot {
    pub wait: Duration,
    wu: f64,
    state: Arc<Mutex<State>>,
}

impl Drop for QueueSlot {
    fn drop(&mut self) {
        let mut st = lock(&self.state);
        st.queued_wu = (st.queued_wu - self.wu).max(0.0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LimiterStatus {
    pub entitlement_wu_s: f64,
    pub level_wu: f64,
    pub burst_credit_wu: Option<f64>,
    pub queued_wu: f64,
}

#[derive(Debug)]
struct State {
    config: LimiterConfig,
    policy: BoundaryPolicy,
    provisioned: DebtBucket,
    burst: Option<BurstBank>,
    queued_wu: f64,
}

impl State {
    /// Refill the provisioned bucket and bank any overflow as burst credit.
    fn refill(&mut self, now: Instant) {
        let overflow = self.provisioned.refill(now);
        if let Some(b) = self.burst.as_mut() {
            b.accrue(overflow);
        }
    }
}

/// Admission state for one reservation on one gateway. Cheap to clone; clones share state,
/// including changes made by [`ReservationLimiter::reconfigure`].
#[derive(Debug, Clone)]
pub struct ReservationLimiter {
    state: Arc<Mutex<State>>,
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    // Admission state stays consistent even if a holder panicked mid-update, so recover.
    state.lock().unwrap_or_else(|e| e.into_inner())
}

impl ReservationLimiter {
    pub fn new(config: LimiterConfig, policy: BoundaryPolicy, now: Instant) -> Self {
        let rate = config.entitlement_wu_s;
        let provisioned = DebtBucket::new(
            rate,
            rate * config.capacity_s,
            rate * config.max_debt_s,
            now,
        );
        let burst = policy.burst.as_ref().map(|p| BurstBank::new(rate, p, now));
        Self {
            state: Arc::new(Mutex::new(State {
                config,
                policy,
                provisioned,
                burst,
                queued_wu: 0.0,
            })),
        }
    }

    pub fn policy(&self) -> BoundaryPolicy {
        lock(&self.state).policy.clone()
    }

    pub fn config(&self) -> LimiterConfig {
        lock(&self.state).config
    }

    /// Change the entitlement or policy in place, for example when an entitlement snapshot
    /// resizes the reservation. The current bucket level (including debt) and burst credit
    /// carry over, clamped to the new limits, so a resize never hands out a fresh full bucket.
    /// In-flight requests settle against the same state.
    pub fn reconfigure(&self, config: LimiterConfig, policy: BoundaryPolicy, now: Instant) {
        let mut st = lock(&self.state);
        if st.config == config && st.policy == policy {
            return;
        }
        st.refill(now);
        let rate = config.entitlement_wu_s;
        let level = st.provisioned.level(now);
        let mut provisioned = DebtBucket::new(
            rate,
            rate * config.capacity_s,
            rate * config.max_debt_s,
            now,
        );
        provisioned.set_level(level);
        let credit = st.burst.as_ref().map_or(0.0, BurstBank::credit);
        let burst = policy.burst.as_ref().map(|p| {
            let mut b = BurstBank::new(rate, p, now);
            b.accrue(credit);
            b
        });
        st.provisioned = provisioned;
        st.burst = burst;
        st.config = config;
        st.policy = policy;
    }

    /// Try to admit. Pass back the [`QueueSlot`] from a previous `Queue` decision, if any.
    pub fn admit(&self, req: &AdmitRequest, slot: Option<QueueSlot>, now: Instant) -> Decision {
        let queued_for = now.saturating_duration_since(req.received_at);
        // Leave the queue before re-evaluating, so our own entry doesn't count against depth.
        drop(slot);

        let mut st = lock(&self.state);
        st.refill(now);
        let wu = req.estimated_wu;

        if st.provisioned.can_admit(wu) {
            st.provisioned.debit(wu);
            return admitted(TrafficClass::Provisioned, wu, queued_for);
        }

        if let Some(burst) = st.burst.as_mut() {
            if burst.can_draw(wu, req.continuation, now) {
                burst.draw(wu);
                return admitted(TrafficClass::Burst, wu, queued_for);
            }
        }

        let retry_after = st.provisioned.time_until_admit(wu);
        let mut reason = RejectReason::EntitlementExhausted;

        let st = &mut *st;
        if let Some(q) = &st.policy.queue {
            let deadline = req.received_at + Duration::from_millis(q.deadline_ms);
            let remaining = deadline.saturating_duration_since(now);
            let max_depth = q.max_depth_wu_seconds * st.config.entitlement_wu_s;
            if retry_after > remaining {
                reason = RejectReason::QueueDeadline;
            } else if st.queued_wu + wu > max_depth {
                reason = RejectReason::QueueFull;
            } else {
                st.queued_wu += wu;
                return Decision::Queue(QueueSlot {
                    // Wake at least every 50 ms to re-check, since other requests may settle
                    // with refunds before the projected time.
                    wait: retry_after
                        .min(Duration::from_millis(50))
                        .max(Duration::from_millis(1)),
                    wu,
                    state: Arc::clone(&self.state),
                });
            }
        }

        if st.policy.spillover {
            return admitted(TrafficClass::Spillover, wu, queued_for);
        }

        Decision::Reject {
            reason,
            retry_after,
        }
    }

    /// Settle a completed (or cancelled) request with its actual WU.
    pub fn settle(&self, ticket: AdmitTicket, actual_wu: f64, now: Instant) {
        let delta = actual_wu - ticket.estimated_wu;
        let mut st = lock(&self.state);
        st.refill(now);
        match ticket.class {
            TrafficClass::Provisioned => st.provisioned.settle(delta),
            TrafficClass::Burst => {
                if let Some(b) = st.burst.as_mut() {
                    b.settle(delta);
                }
            }
            // Spillover is billed at PAYG and doesn't touch the entitlement.
            TrafficClass::Spillover | TrafficClass::Payg => {}
        }
    }

    pub fn status(&self, now: Instant) -> LimiterStatus {
        let mut st = lock(&self.state);
        st.refill(now);
        LimiterStatus {
            entitlement_wu_s: st.config.entitlement_wu_s,
            level_wu: st.provisioned.level(now),
            burst_credit_wu: st.burst.as_ref().map(BurstBank::credit),
            queued_wu: st.queued_wu,
        }
    }
}

fn admitted(class: TrafficClass, estimated_wu: f64, queued_for: Duration) -> Decision {
    Decision::Admit(AdmitTicket {
        class,
        estimated_wu,
        queued_for,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{BurstPolicy, QueuePolicy};

    fn at(t0: Instant, secs: f64) -> Instant {
        t0 + Duration::from_secs_f64(secs)
    }

    fn req(wu: f64, received_at: Instant) -> AdmitRequest {
        AdmitRequest {
            estimated_wu: wu,
            continuation: false,
            received_at,
        }
    }

    fn class_of(d: &Decision) -> Option<TrafficClass> {
        match d {
            Decision::Admit(t) => Some(t.class),
            _ => None,
        }
    }

    /// Put the provisioned bucket (capacity 100) into 150 WU of debt with one large request,
    /// so the next request needs 1.5 s of refill.
    fn exhaust(l: &ReservationLimiter, now: Instant) {
        let Decision::Admit(t) = l.admit(&req(250.0, now), None, now) else {
            panic!("full bucket should admit");
        };
        assert_eq!(t.class, TrafficClass::Provisioned);
        l.settle(t, 250.0, now);
    }

    #[test]
    fn reject_only_policy_returns_retry_after() {
        let t0 = Instant::now();
        let l = ReservationLimiter::new(LimiterConfig::new(100.0), BoundaryPolicy::default(), t0);
        exhaust(&l, t0);
        match l.admit(&req(50.0, t0), None, t0) {
            Decision::Reject {
                reason,
                retry_after,
            } => {
                assert_eq!(reason, RejectReason::EntitlementExhausted);
                assert!(retry_after > Duration::ZERO);
            }
            d => panic!("expected reject, got {d:?}"),
        }
    }

    #[test]
    fn spillover_after_exhaustion_and_does_not_settle_entitlement() {
        let t0 = Instant::now();
        let policy = BoundaryPolicy {
            spillover: true,
            ..Default::default()
        };
        let l = ReservationLimiter::new(LimiterConfig::new(100.0), policy, t0);
        exhaust(&l, t0);
        let before = l.status(t0).level_wu;
        let d = l.admit(&req(50.0, t0), None, t0);
        assert_eq!(class_of(&d), Some(TrafficClass::Spillover));
        if let Decision::Admit(t) = d {
            l.settle(t, 500.0, t0);
        }
        assert_eq!(l.status(t0).level_wu, before);
    }

    #[test]
    fn burst_uses_banked_unused_entitlement() {
        let t0 = Instant::now();
        let policy = BoundaryPolicy {
            burst: Some(BurstPolicy::default()),
            ..Default::default()
        };
        let l = ReservationLimiter::new(LimiterConfig::new(100.0), policy, t0);
        // Idle for 20 s: bucket stays full, 2,000 WU of overflow banks as credit.
        // New sessions can use what's above the 1,500 WU continuation reserve.
        let t = at(t0, 20.0);
        assert!((l.status(t).burst_credit_wu.unwrap() - 2_000.0).abs() < 1e-6);
        exhaust(&l, t);
        let d = l.admit(&req(50.0, t), None, t);
        assert_eq!(class_of(&d), Some(TrafficClass::Burst));
    }

    #[test]
    fn continuation_can_use_reserve_new_session_cannot() {
        let t0 = Instant::now();
        let policy = BoundaryPolicy {
            burst: Some(BurstPolicy {
                continuation_reserve: 0.5,
                ..Default::default()
            }),
            ..Default::default()
        };
        // Credit cap 6,000; reserve 3,000. Bank 3,100 WU.
        let l = ReservationLimiter::new(LimiterConfig::new(100.0), policy, t0);
        let t = at(t0, 31.0);
        exhaust(&l, t);
        let new_session = AdmitRequest {
            estimated_wu: 200.0,
            continuation: false,
            received_at: t,
        };
        let cont = AdmitRequest {
            continuation: true,
            ..new_session
        };
        assert!(matches!(
            l.admit(&new_session, None, t),
            Decision::Reject { .. }
        ));
        assert_eq!(
            class_of(&l.admit(&cont, None, t)),
            Some(TrafficClass::Burst)
        );
    }

    #[test]
    fn queue_waits_then_admits_within_deadline() {
        let t0 = Instant::now();
        let policy = BoundaryPolicy {
            queue: Some(QueuePolicy::default()),
            ..Default::default()
        };
        let l = ReservationLimiter::new(LimiterConfig::new(100.0), policy, t0);
        exhaust(&l, t0);
        let r = req(50.0, t0);
        let mut now = t0;
        let mut slot = None;
        let ticket = loop {
            match l.admit(&r, slot.take(), now) {
                Decision::Admit(t) => break t,
                Decision::Queue(s) => {
                    assert!(l.status(now).queued_wu > 0.0);
                    now += s.wait;
                    slot = Some(s);
                }
                d => panic!("unexpected {d:?}"),
            }
        };
        assert_eq!(ticket.class, TrafficClass::Provisioned);
        assert!(ticket.queued_for > Duration::ZERO);
        assert!(ticket.queued_for <= Duration::from_millis(2_000));
        assert_eq!(l.status(now).queued_wu, 0.0);
    }

    #[test]
    fn queue_rejects_when_wait_exceeds_deadline() {
        let t0 = Instant::now();
        let policy = BoundaryPolicy {
            queue: Some(QueuePolicy {
                deadline_ms: 100,
                ..Default::default()
            }),
            ..Default::default()
        };
        let l = ReservationLimiter::new(LimiterConfig::new(100.0), policy, t0);
        exhaust(&l, t0);
        match l.admit(&req(50.0, t0), None, t0) {
            Decision::Reject { reason, .. } => assert_eq!(reason, RejectReason::QueueDeadline),
            d => panic!("expected reject, got {d:?}"),
        }
    }

    #[test]
    fn dropped_queue_slot_releases_depth() {
        let t0 = Instant::now();
        let policy = BoundaryPolicy {
            queue: Some(QueuePolicy::default()),
            ..Default::default()
        };
        let l = ReservationLimiter::new(LimiterConfig::new(100.0), policy, t0);
        exhaust(&l, t0);
        let d = l.admit(&req(50.0, t0), None, t0);
        assert!(matches!(d, Decision::Queue(_)));
        assert_eq!(l.status(t0).queued_wu, 50.0);
        drop(d);
        assert_eq!(l.status(t0).queued_wu, 0.0);
    }

    #[test]
    fn reconfigure_keeps_level_and_credit_under_new_limits() {
        let t0 = Instant::now();
        let policy = BoundaryPolicy {
            burst: Some(BurstPolicy::default()),
            ..Default::default()
        };
        let l = ReservationLimiter::new(LimiterConfig::new(100.0), policy.clone(), t0);
        let clone = l.clone();
        let t = at(t0, 10.0); // bank 1,000 WU of burst credit
        exhaust(&l, t); // level −150

        // Double the entitlement: the debt carries over, the refill rate doubles.
        l.reconfigure(LimiterConfig::new(200.0), policy.clone(), t);
        let s = clone.status(t);
        assert_eq!(s.entitlement_wu_s, 200.0);
        assert!((s.level_wu + 150.0).abs() < 1e-9);
        assert!((s.burst_credit_wu.unwrap() - 1_000.0).abs() < 1e-9);
        assert!((clone.status(at(t0, 11.0)).level_wu - 50.0).abs() < 1e-9);

        // Shrink below the current level: clamped to the new capacity.
        l.reconfigure(
            LimiterConfig::new(20.0),
            BoundaryPolicy::default(),
            at(t0, 20.0),
        );
        let s = l.status(at(t0, 20.0));
        assert_eq!(s.level_wu, 20.0);
        assert_eq!(s.burst_credit_wu, None);
        assert_eq!(l.policy(), BoundaryPolicy::default());
    }

    #[test]
    fn provisioned_settlement_refunds_overestimate() {
        let t0 = Instant::now();
        let l = ReservationLimiter::new(LimiterConfig::new(100.0), BoundaryPolicy::default(), t0);
        let Decision::Admit(t) = l.admit(&req(80.0, t0), None, t0) else {
            panic!()
        };
        assert!((l.status(t0).level_wu - 20.0).abs() < 1e-9);
        l.settle(t, 30.0, t0);
        assert!((l.status(t0).level_wu - 70.0).abs() < 1e-9);
    }
}
