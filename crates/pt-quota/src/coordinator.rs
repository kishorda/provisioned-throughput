//! Lease state and the renewal rule.
//!
//! Invariant: for every reservation, the sum of unexpired grants is at most the
//! entitlement. A gateway's target comes from [`allocate`], but it's only granted what the
//! other gateways' unexpired grants leave free. When a gateway joins or demand shifts, the
//! others shrink at their next renewal and the newcomer reaches its target one renewal
//! later, with no moment of overselling.
//!
//! The coordinator keeps a grant alive for `lease_ttl × 1.5` after issuing it, longer than
//! the gateway uses it, so network delay can't make the coordinator hand out capacity a
//! gateway still thinks it holds.
//!
//! **Terms** (ADR-027). With several replicas, only the elected leader serves. A new
//! leader starts a term with empty state ([`Coordinator::begin_term`]). If the previous
//! leader released its lease, or this is a restart, gateways may still hold its grants, so
//! the term starts with a *warm-up* of one grant hold. During warm-up, a gateway is
//! granted at most the unexpired lease it reports holding (`held_wu_s`), and one holding
//! none gets no lease and keeps its current rate. The previous leader's grants summed to
//! at most the entitlement, so these do too.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::allocator::allocate;
use crate::wire::{Lease, RenewRequest, RenewResponse};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CoordinatorConfig {
    pub lease_ttl: Duration,
    /// Share of each entitlement reserved as a floor across active gateways.
    pub floor_fraction: f64,
}

impl CoordinatorConfig {
    /// How long a grant counts as outstanding: 1.5 × the lease TTL, longer than gateways
    /// use it.
    pub fn grant_hold(&self) -> Duration {
        self.lease_ttl * 3 / 2
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            lease_ttl: Duration::from_secs(1),
            floor_fraction: 0.1,
        }
    }
}

#[derive(Debug)]
struct Gateway {
    demand: f64,
    last_seen: Instant,
    grant: f64,
    grant_expires: Instant,
}

#[derive(Debug, Default)]
struct Reservation {
    entitlement: f64,
    version: u64,
    /// Ordered, so allocation is deterministic.
    gateways: BTreeMap<String, Gateway>,
}

pub struct Coordinator {
    config: CoordinatorConfig,
    reservations: Mutex<HashMap<String, Reservation>>,
    /// Until then, grants are capped at what each gateway already holds.
    warm_until: Mutex<Option<Instant>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReservationView {
    pub id: String,
    pub entitlement_wu_s: f64,
    pub snapshot_version: u64,
    /// Sum of unexpired grants. Never above the entitlement.
    pub granted_wu_s: f64,
    pub gateways: Vec<GatewayView>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GatewayView {
    pub id: String,
    pub demand_wu_s: f64,
    pub grant_wu_s: f64,
}

impl Coordinator {
    pub fn new(config: CoordinatorConfig) -> Self {
        Self {
            config,
            reservations: Mutex::new(HashMap::new()),
            warm_until: Mutex::new(None),
        }
    }

    /// Start serving as leader: forget any earlier state. With `warm_up`, gateways may
    /// still hold grants this replica doesn't know about (a released lease or a restart).
    pub fn begin_term(&self, now: Instant, warm_up: bool) {
        let mut all = self.reservations.lock().unwrap_or_else(|e| e.into_inner());
        all.clear();
        *self.warm_until.lock().unwrap_or_else(|e| e.into_inner()) =
            warm_up.then(|| now + self.hold());
    }

    /// Whether the current term is still warming up.
    pub fn warming_up(&self, now: Instant) -> bool {
        self.warm_until
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some_and(|u| now < u)
    }

    /// How long the coordinator counts a grant as outstanding.
    fn hold(&self) -> Duration {
        self.config.grant_hold()
    }

    pub fn renew(&self, req: &RenewRequest, now: Instant) -> RenewResponse {
        let hold = self.hold();
        let warming = self.warming_up(now);
        let mut all = self.reservations.lock().unwrap_or_else(|e| e.into_inner());
        let mut leases = Vec::with_capacity(req.reservations.len());

        for d in &req.reservations {
            let r = all.entry(d.id.clone()).or_default();
            if d.snapshot_version >= r.version {
                r.entitlement = d.entitlement_wu_s.max(0.0);
                r.version = d.snapshot_version;
            }
            let demand = if d.demand_wu_s.is_finite() {
                d.demand_wu_s.max(0.0)
            } else {
                0.0
            };
            let g = r.gateways.entry(req.gateway_id.clone()).or_insert(Gateway {
                demand,
                last_seen: now,
                grant: 0.0,
                grant_expires: now,
            });
            g.demand = demand;
            g.last_seen = now;

            // Forget gateways that stopped renewing; their grants have expired.
            r.gateways
                .retain(|_, g| now.saturating_duration_since(g.last_seen) <= hold);

            let ids: Vec<&String> = r.gateways.keys().collect();
            let demands: Vec<f64> = r.gateways.values().map(|g| g.demand).collect();
            let targets = allocate(r.entitlement, self.config.floor_fraction, &demands);
            let pos = ids
                .iter()
                .position(|id| **id == req.gateway_id)
                .expect("just inserted");
            let others: f64 = r
                .gateways
                .iter()
                .filter(|(id, g)| **id != req.gateway_id && g.grant_expires > now)
                .map(|(_, g)| g.grant)
                .sum();
            let mut grant = targets[pos].min((r.entitlement - others).max(0.0));
            if warming {
                let held = if d.held_wu_s.is_finite() {
                    d.held_wu_s.max(0.0)
                } else {
                    0.0
                };
                grant = grant.min(held);
            }

            let active = r.gateways.len() as u32;
            let g = r.gateways.get_mut(&req.gateway_id).expect("present");
            g.grant = grant;
            g.grant_expires = now + hold;
            if warming && grant <= 0.0 {
                // Holds nothing we know of: no lease, so it keeps its current rate.
                continue;
            }
            leases.push(Lease {
                id: d.id.clone(),
                rate_wu_s: grant,
                active_gateways: active,
            });
        }

        // Drop reservations nobody renews any more.
        all.retain(|_, r| {
            r.gateways
                .retain(|_, g| now.saturating_duration_since(g.last_seen) <= hold);
            !r.gateways.is_empty()
        });

        RenewResponse {
            warming_up: warming,
            ttl_ms: self.config.lease_ttl.as_millis() as u64,
            leases,
        }
    }

    pub fn view(&self, now: Instant) -> Vec<ReservationView> {
        let all = self.reservations.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<ReservationView> = all
            .iter()
            .map(|(id, r)| ReservationView {
                id: id.clone(),
                entitlement_wu_s: r.entitlement,
                snapshot_version: r.version,
                granted_wu_s: r
                    .gateways
                    .values()
                    .filter(|g| g.grant_expires > now)
                    .map(|g| g.grant)
                    .sum(),
                gateways: r
                    .gateways
                    .iter()
                    .map(|(gid, g)| GatewayView {
                        id: gid.clone(),
                        demand_wu_s: g.demand,
                        grant_wu_s: if g.grant_expires > now { g.grant } else { 0.0 },
                    })
                    .collect(),
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::ReservationDemand;

    fn req(gw: &str, e: f64, version: u64, demand: f64) -> RenewRequest {
        RenewRequest {
            gateway_id: gw.into(),
            reservations: vec![ReservationDemand {
                id: "r".into(),
                entitlement_wu_s: e,
                snapshot_version: version,
                demand_wu_s: demand,
                held_wu_s: 0.0,
            }],
        }
    }

    fn grant(c: &Coordinator, gw: &str, e: f64, demand: f64, now: Instant) -> f64 {
        c.renew(&req(gw, e, 1, demand), now).leases[0].rate_wu_s
    }

    fn granted(c: &Coordinator, now: Instant) -> f64 {
        c.view(now)[0].granted_wu_s
    }

    #[test]
    fn single_gateway_gets_everything() {
        let c = Coordinator::new(CoordinatorConfig::default());
        let t = Instant::now();
        assert_eq!(grant(&c, "a", 1_000.0, 0.0, t), 1_000.0);
    }

    #[test]
    fn joining_gateway_never_oversells() {
        let c = Coordinator::new(CoordinatorConfig::default());
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        assert_eq!(grant(&c, "a", 1_000.0, 800.0, ms(0)), 1_000.0);

        // b joins with the same demand. a still holds 1,000, so b gets nothing yet.
        assert_eq!(grant(&c, "b", 1_000.0, 800.0, ms(10)), 0.0);
        assert!(granted(&c, ms(10)) <= 1_000.0);

        // a renews and shrinks to its fair share; b then gets the rest.
        assert_eq!(grant(&c, "a", 1_000.0, 800.0, ms(250)), 500.0);
        assert_eq!(grant(&c, "b", 1_000.0, 800.0, ms(260)), 500.0);
        assert!(granted(&c, ms(260)) <= 1_000.0 + 1e-9);
    }

    #[test]
    fn demand_shifts_capacity_between_gateways() {
        let c = Coordinator::new(CoordinatorConfig::default());
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        grant(&c, "a", 1_000.0, 0.0, ms(0));
        grant(&c, "b", 1_000.0, 0.0, ms(0));
        for step in 1..=4 {
            let t = step * 250;
            grant(&c, "a", 1_000.0, 5_000.0, ms(t));
            grant(&c, "b", 1_000.0, 0.0, ms(t + 5));
            assert!(granted(&c, ms(t + 5)) <= 1_000.0 + 1e-9);
        }
        let v = &c.view(ms(1_010))[0];
        let a = v.gateways.iter().find(|g| g.id == "a").unwrap().grant_wu_s;
        let b = v.gateways.iter().find(|g| g.id == "b").unwrap().grant_wu_s;
        assert!((a - 950.0).abs() < 1e-6, "busy gateway {a}");
        assert!((b - 50.0).abs() < 1e-6, "idle gateway keeps its floor {b}");
    }

    #[test]
    fn departed_gateway_share_is_reclaimed() {
        let c = Coordinator::new(CoordinatorConfig::default());
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        grant(&c, "a", 1_000.0, 0.0, ms(0));
        grant(&c, "b", 1_000.0, 0.0, ms(0));
        grant(&c, "a", 1_000.0, 0.0, ms(250));
        assert_eq!(grant(&c, "b", 1_000.0, 0.0, ms(250)), 500.0);
        // b stops renewing. After 1.5 × TTL its grant is released.
        assert_eq!(
            grant(&c, "a", 1_000.0, 0.0, ms(1_300)),
            500.0,
            "b's grant still held"
        );
        assert_eq!(grant(&c, "a", 1_000.0, 0.0, ms(1_800)), 1_000.0);
        assert_eq!(c.view(ms(1_800))[0].gateways.len(), 1);
    }

    #[test]
    fn entitlement_follows_the_newest_snapshot() {
        let c = Coordinator::new(CoordinatorConfig::default());
        let t = Instant::now();
        c.renew(&req("a", 2_000.0, 5, 0.0), t);
        // A gateway with an older snapshot can't roll the entitlement back.
        c.renew(&req("b", 1_000.0, 4, 0.0), t);
        assert_eq!(c.view(t)[0].entitlement_wu_s, 2_000.0);
        // A shrink leaves no room until the others renew under the new limit.
        let shrunk = c.renew(&req("b", 500.0, 6, 0.0), t).leases[0].rate_wu_s;
        assert_eq!(shrunk, 0.0);
        assert!(c.renew(&req("a", 500.0, 6, 0.0), t).leases[0].rate_wu_s <= 500.0);
    }

    #[test]
    fn idle_reservations_are_forgotten() {
        let c = Coordinator::new(CoordinatorConfig::default());
        let t0 = Instant::now();
        c.renew(&req("a", 1_000.0, 1, 0.0), t0);
        c.renew(
            &RenewRequest {
                gateway_id: "a".into(),
                reservations: vec![],
            },
            t0 + Duration::from_secs(2),
        );
        assert!(c.view(t0 + Duration::from_secs(2)).is_empty());
    }

    #[test]
    fn warm_up_never_grants_more_than_gateways_hold() {
        let c = Coordinator::new(CoordinatorConfig::default());
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        c.renew(&req("old", 1_000.0, 1, 0.0), ms(0));
        // A new term forgets that state.
        c.begin_term(ms(10), true);
        assert!(c.view(ms(10)).is_empty());
        // a and b hold 600 and 400 from the previous leader. a wants everything.
        let held = |gw: &str, demand: f64, held: f64| {
            let mut r = req(gw, 1_000.0, 1, demand);
            r.reservations[0].held_wu_s = held;
            r
        };
        let a = c.renew(&held("a", 5_000.0, 600.0), ms(20)).leases;
        assert_eq!(a[0].rate_wu_s, 600.0, "capped at what it holds");
        let b = c.renew(&held("b", 0.0, 400.0), ms(30)).leases;
        assert!(b[0].rate_wu_s <= 400.0);
        // A gateway holding nothing gets no lease during warm-up.
        assert!(c.renew(&held("new", 100.0, 0.0), ms(40)).leases.is_empty());
        // After one hold (1.5 s), normal allocation resumes.
        assert!(!c.warming_up(ms(1_510)));
        c.renew(&held("b", 0.0, 0.0), ms(1_510));
        c.renew(&held("new", 100.0, 0.0), ms(1_515));
        let a = c.renew(&held("a", 5_000.0, 0.0), ms(1_520)).leases;
        assert!(a[0].rate_wu_s > 600.0);
        assert!(granted(&c, ms(1_520)) <= 1_000.0 + 1e-9);
    }
}
