//! Runtime state: the current entitlements, swapped atomically when a snapshot arrives.
//!
//! Requests look up deployments by API-key hash in the current [`Entitlements`]. Applying
//! a new snapshot builds a new view, reusing each reservation's limiter (reconfigured in
//! place) and each deployment's output estimator. Bucket levels, debt, burst credit, and
//! in-flight settlements survive resizes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use pt_admission::{BoundaryPolicy, LimiterConfig, OutputEstimator, ReservationLimiter};
use pt_core::{ApproxTokenCounter, PerformanceProfile, Shape, Tier, TokenCounter};
use pt_entitlement::{sha256_hex, DeploymentEntitlement, ReservationEntitlement, Snapshot};
use pt_quota::wire::{RenewResponse, ReservationDemand};

use crate::config::{ConfigError, GatewayConfig};
use crate::usage::UsageSink;

pub struct Reservation {
    pub id: String,
    pub tenant: String,
    pub model: String,
    pub cus: u32,
    /// The region's full entitlement (CUs × WU/s per CU). With a Quota Coordinator, the
    /// limiter enforces only this gateway's share of it.
    pub entitlement_wu_s: f64,
    pub tier: Tier,
    pub profile: PerformanceProfile,
    pub shape: Shape,
    pub limiter: ReservationLimiter,
}

pub struct Deployment {
    pub id: String,
    pub reservation: Arc<Reservation>,
    estimator: Arc<Mutex<OutputEstimator>>,
    /// Cap on this deployment's share of the reservation's entitlement.
    pub max_share: Option<f64>,
    /// With a cap, a limiter at `max_share` × the reservation's local rate, checked before
    /// the reservation's own bucket. Over the cap is rejected; the reservation's boundary
    /// policy doesn't apply to it.
    pub cap: Option<ReservationLimiter>,
}

impl Deployment {
    /// The rate the cap should enforce now.
    fn cap_rate(&self) -> Option<f64> {
        self.max_share
            .map(|share| share * self.reservation.limiter.config().entitlement_wu_s)
    }
}

impl Deployment {
    pub fn estimator(&self) -> MutexGuard<'_, OutputEstimator> {
        self.estimator.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// One immutable view of what this gateway may serve.
pub struct Entitlements {
    /// Snapshot version, or 0 for static configuration.
    pub version: u64,
    pub source: EntitlementSource,
    /// When this gateway applied the view.
    pub applied_at: Instant,
    /// When the control plane generated the snapshot (RFC 3339). Survives cache reloads,
    /// so it shows how stale the entitlements really are.
    pub generated_at: Option<String>,
    /// Key hash → deployment, and when the key stops working (rotated-out keys only).
    by_key_hash: HashMap<String, (Arc<Deployment>, Option<u64>)>,
    by_deployment: HashMap<String, Arc<Deployment>>,
    reservations: HashMap<String, Arc<Reservation>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntitlementSource {
    Static,
    Snapshot {
        region: String,
    },
    /// Waiting for the first snapshot. Nothing is served.
    Pending {
        region: String,
    },
}

impl Entitlements {
    fn empty(source: EntitlementSource) -> Self {
        Self {
            version: 0,
            source,
            applied_at: Instant::now(),
            generated_at: None,
            by_key_hash: HashMap::new(),
            by_deployment: HashMap::new(),
            reservations: HashMap::new(),
        }
    }

    pub fn reservation_count(&self) -> usize {
        self.reservations.len()
    }

    pub fn deployment_count(&self) -> usize {
        self.by_deployment.len()
    }

    pub fn reservations(&self) -> impl Iterator<Item = &Arc<Reservation>> {
        self.reservations.values()
    }

    pub fn deployments(&self) -> impl Iterator<Item = &Arc<Deployment>> {
        self.by_deployment.values()
    }
}

/// This gateway's leases from the Quota Coordinator.
struct QuotaShares {
    assumed_gateways: u32,
    decay: Duration,
    leases: Mutex<HashMap<String, LeaseState>>,
    /// Smoothed demand per reservation, WU/s.
    demand: Mutex<HashMap<String, f64>>,
}

#[derive(Debug, Clone, Copy)]
struct LeaseState {
    rate: f64,
    expires_at: Instant,
    gateways: u32,
}

/// How a reservation's local rate is currently set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaMode {
    /// No coordinator: the full entitlement.
    Disabled,
    /// No lease yet: entitlement ÷ assumed gateways.
    Unleased,
    Lease,
    /// Lease expired: decaying towards 50% of entitlement ÷ gateways.
    Fallback,
}

impl QuotaMode {
    pub fn as_str(self) -> &'static str {
        match self {
            QuotaMode::Disabled => "disabled",
            QuotaMode::Unleased => "unleased",
            QuotaMode::Lease => "lease",
            QuotaMode::Fallback => "fallback",
        }
    }
}

impl QuotaShares {
    fn rate(&self, id: &str, entitlement: f64, now: Instant) -> (f64, QuotaMode) {
        let leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
        match leases.get(id) {
            None => (
                entitlement / f64::from(self.assumed_gateways),
                QuotaMode::Unleased,
            ),
            Some(l) if now < l.expires_at => (l.rate.min(entitlement), QuotaMode::Lease),
            Some(l) => {
                let target = 0.5 * entitlement / f64::from(l.gateways.max(1));
                let t = (now - l.expires_at).as_secs_f64() / self.decay.as_secs_f64();
                let rate = l.rate + (target - l.rate) * t.min(1.0);
                (rate.clamp(0.0, entitlement), QuotaMode::Fallback)
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyReport {
    pub version: u64,
    pub reservations: usize,
    pub deployments: usize,
    /// Reservations left out, with the reason.
    pub skipped: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApplyError {
    #[error("snapshot is for region {got}, this gateway serves {expected}")]
    WrongRegion { expected: String, got: String },
    #[error("snapshot version {got} is not newer than {current}")]
    Stale { current: u64, got: u64 },
    #[error("this gateway uses static configuration, not snapshots")]
    StaticConfig,
}

pub struct Inner {
    current: RwLock<Arc<Entitlements>>,
    quota: Option<QuotaShares>,
    profiles: HashMap<String, PerformanceProfile>,
    wu_per_cu: f64,
    pub http: reqwest::Client,
    pub engine_url: String,
    pub payg_engine_url: String,
    pub sink: Arc<dyn UsageSink>,
    pub tokens: Arc<dyn TokenCounter>,
}

/// Shared gateway state. Cheap to clone.
#[derive(Clone)]
pub struct AppState(Arc<Inner>);

impl std::ops::Deref for AppState {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.0
    }
}

impl AppState {
    /// With `[entitlements]` configured, the gateway starts empty and serves nothing until
    /// a snapshot (cached or fetched) is applied. Otherwise it serves the static config.
    pub fn new(config: &GatewayConfig, sink: Arc<dyn UsageSink>) -> Result<Self, ConfigError> {
        config.validate()?;
        let engine_url = config.server.engine_url.trim_end_matches('/').to_string();
        let payg_engine_url = config.server.payg_engine_url.as_deref().map_or_else(
            || engine_url.clone(),
            |u| u.trim_end_matches('/').to_string(),
        );
        let source = match &config.entitlements {
            Some(e) => EntitlementSource::Pending {
                region: e.region.clone(),
            },
            None => EntitlementSource::Static,
        };
        let state = Self(Arc::new(Inner {
            current: RwLock::new(Arc::new(Entitlements::empty(source))),
            quota: config.quota.as_ref().map(|q| QuotaShares {
                assumed_gateways: q.assumed_gateways,
                decay: Duration::from_secs_f64(q.fallback_decay_secs),
                leases: Mutex::new(HashMap::new()),
                demand: Mutex::new(HashMap::new()),
            }),
            profiles: config
                .profiles
                .iter()
                .map(|p| (p.name.clone(), p.clone()))
                .collect(),
            wu_per_cu: config.server.wu_per_cu,
            http: reqwest::Client::new(),
            engine_url,
            payg_engine_url,
            sink,
            tokens: Arc::new(ApproxTokenCounter),
        }));

        if config.entitlements.is_none() {
            let reservations: Vec<_> = config
                .reservations
                .iter()
                .map(|r| ReservationEntitlement {
                    id: r.id.clone(),
                    tenant: r.tenant.clone(),
                    model: r.model.clone(),
                    cus: r.cus,
                    tier: r.tier,
                    profile: r.profile.clone(),
                    shape: r.shape,
                })
                .collect();
            let deployments: Vec<_> = config
                .deployments
                .iter()
                .map(|d| DeploymentEntitlement {
                    id: d.id.clone(),
                    reservation: d.reservation.clone(),
                    api_key_sha256: sha256_hex(d.api_key.as_bytes()),
                    previous_keys: vec![],
                    max_share: d.max_share,
                    boundary_policy: d.boundary_policy.clone(),
                })
                .collect();
            for d in &config.deployments {
                let first = config
                    .deployments
                    .iter()
                    .find(|o| o.reservation == d.reservation)
                    .expect("d itself");
                if first.boundary_policy != d.boundary_policy {
                    return Err(ConfigError::Invalid(format!(
                        "deployment {} has a different boundary policy from other deployments of reservation {}",
                        d.id, d.reservation
                    )));
                }
            }
            let (view, _) = state.build(0, EntitlementSource::Static, &reservations, &deployments);
            *state.current.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(view);
        }
        Ok(state)
    }

    /// The current entitlements. Cheap: clones an `Arc`.
    pub fn entitlements(&self) -> Arc<Entitlements> {
        Arc::clone(&self.current.read().unwrap_or_else(|e| e.into_inner()))
    }

    /// The deployment for an API key, if the key is current or still in its rotation grace
    /// period. Expiry is checked here, to the millisecond, not only when the next snapshot
    /// drops the key.
    pub fn deployment_for_key(&self, api_key: &str) -> Option<Arc<Deployment>> {
        let view = self.entitlements();
        let (dep, expires_at_ms) = view.by_key_hash.get(&sha256_hex(api_key.as_bytes()))?;
        if let Some(expires) = expires_at_ms {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64);
            if now_ms >= *expires {
                return None;
            }
        }
        Some(Arc::clone(dep))
    }

    /// The rate this gateway may admit for a reservation, and why.
    pub fn local_rate(&self, id: &str, entitlement: f64, now: Instant) -> (f64, QuotaMode) {
        match &self.quota {
            None => (entitlement, QuotaMode::Disabled),
            Some(q) => q.rate(id, entitlement, now),
        }
    }

    /// Record leases from the coordinator, then resize limiters to match.
    pub fn apply_leases(&self, resp: &RenewResponse, received_at: Instant) {
        let Some(q) = &self.quota else { return };
        let ttl = Duration::from_millis(resp.ttl_ms);
        {
            let mut leases = q.leases.lock().unwrap_or_else(|e| e.into_inner());
            for l in &resp.leases {
                leases.insert(
                    l.id.clone(),
                    LeaseState {
                        rate: l.rate_wu_s.max(0.0),
                        expires_at: received_at + ttl,
                        gateways: l.active_gateways,
                    },
                );
            }
        }
        self.refresh_local_rates(received_at);
    }

    /// Resize each limiter to its current local rate: the lease, the fallback, or the full
    /// entitlement. Changes under 1% are skipped to avoid churn.
    pub fn refresh_local_rates(&self, now: Instant) {
        if self.quota.is_none() {
            return;
        }
        for r in self.entitlements().reservations() {
            let (target, _) = self.local_rate(&r.id, r.entitlement_wu_s, now);
            let current = r.limiter.config().entitlement_wu_s;
            let diff = (target - current).abs();
            if diff > 1e-9 && diff >= 0.01 * current.max(target) {
                r.limiter
                    .reconfigure(LimiterConfig::new(target), r.limiter.policy(), now);
            }
        }
        self.refresh_caps(now);
    }

    /// Keep each deployment cap at `max_share` × its reservation's current local rate.
    fn refresh_caps(&self, now: Instant) {
        for d in self.entitlements().deployments() {
            let (Some(cap), Some(target)) = (&d.cap, d.cap_rate()) else {
                continue;
            };
            let current = cap.config().entitlement_wu_s;
            let diff = (target - current).abs();
            if diff > 1e-9 && diff >= 0.01 * current.max(target) {
                cap.reconfigure(LimiterConfig::new(target), BoundaryPolicy::default(), now);
            }
        }
    }

    /// Demand for each reservation since the last report, smoothed. Drains each limiter's
    /// attempted-WU counter.
    pub fn demand_report(&self, elapsed: Duration) -> Vec<ReservationDemand> {
        let Some(q) = &self.quota else { return vec![] };
        let view = self.entitlements();
        let secs = elapsed.as_secs_f64().max(1e-3);
        let mut demand = q.demand.lock().unwrap_or_else(|e| e.into_inner());
        demand.retain(|id, _| view.reservations.contains_key(id));
        view.reservations()
            .map(|r| {
                let now_rate = r.limiter.take_attempted_wu() / secs;
                let d = demand.entry(r.id.clone()).or_insert(now_rate);
                *d = 0.5 * *d + 0.5 * now_rate;
                ReservationDemand {
                    id: r.id.clone(),
                    entitlement_wu_s: r.entitlement_wu_s,
                    snapshot_version: view.version,
                    demand_wu_s: *d,
                }
            })
            .collect()
    }

    /// Replace the entitlements with `snapshot`, if it's for this region and newer.
    pub fn apply_snapshot(&self, snapshot: &Snapshot) -> Result<ApplyReport, ApplyError> {
        let current = self.entitlements();
        let region = match &current.source {
            EntitlementSource::Static => return Err(ApplyError::StaticConfig),
            EntitlementSource::Snapshot { region } | EntitlementSource::Pending { region } => {
                region.clone()
            }
        };
        if snapshot.region != region {
            return Err(ApplyError::WrongRegion {
                expected: region,
                got: snapshot.region.clone(),
            });
        }
        if snapshot.version <= current.version {
            return Err(ApplyError::Stale {
                current: current.version,
                got: snapshot.version,
            });
        }
        let (mut view, report) = self.build(
            snapshot.version,
            EntitlementSource::Snapshot { region },
            &snapshot.reservations,
            &snapshot.deployments,
        );
        view.generated_at = Some(snapshot.generated_at.clone());
        let mut slot = self.current.write().unwrap_or_else(|e| e.into_inner());
        // Another apply may have won while this one was building.
        if slot.version >= snapshot.version {
            return Err(ApplyError::Stale {
                current: slot.version,
                got: snapshot.version,
            });
        }
        *slot = Arc::new(view);
        Ok(report)
    }

    fn build(
        &self,
        version: u64,
        source: EntitlementSource,
        reservations: &[ReservationEntitlement],
        deployments: &[DeploymentEntitlement],
    ) -> (Entitlements, ApplyReport) {
        let now = Instant::now();
        let prev = self.entitlements();
        let mut view = Entitlements::empty(source);
        view.version = version;
        let mut report = ApplyReport {
            version,
            ..Default::default()
        };

        for r in reservations {
            let Some(profile) = self.profiles.get(&r.profile) else {
                report
                    .skipped
                    .push(format!("{}: unknown profile {}", r.id, r.profile));
                continue;
            };
            if r.cus < 1 {
                report.skipped.push(format!("{}: no CUs", r.id));
                continue;
            }
            // One limiter per reservation. The first deployment's policy governs it.
            let Some(first) = deployments.iter().find(|d| d.reservation == r.id) else {
                report.skipped.push(format!("{}: no deployments", r.id));
                continue;
            };
            let entitlement = f64::from(r.cus) * self.wu_per_cu;
            let (rate, _) = self.local_rate(&r.id, entitlement, now);
            let config = LimiterConfig::new(rate);
            let limiter = match prev.reservations.get(&r.id) {
                Some(p) => {
                    p.limiter
                        .reconfigure(config, first.boundary_policy.clone(), now);
                    p.limiter.clone()
                }
                None => ReservationLimiter::new(config, first.boundary_policy.clone(), now),
            };
            view.reservations.insert(
                r.id.clone(),
                Arc::new(Reservation {
                    id: r.id.clone(),
                    tenant: r.tenant.clone(),
                    model: r.model.clone(),
                    cus: r.cus,
                    entitlement_wu_s: entitlement,
                    tier: r.tier,
                    profile: profile.clone(),
                    shape: r.shape,
                    limiter,
                }),
            );
        }

        for d in deployments {
            let Some(reservation) = view.reservations.get(&d.reservation) else {
                continue; // its reservation was skipped
            };
            let estimator = prev
                .by_deployment
                .get(&d.id)
                .filter(|p| p.reservation.id == d.reservation)
                .map(|p| Arc::clone(&p.estimator))
                .unwrap_or_else(|| {
                    Arc::new(Mutex::new(OutputEstimator::new(
                        reservation.shape.output_p95,
                    )))
                });
            let previous = prev.by_deployment.get(&d.id);
            let max_share = d.max_share.filter(|s| *s > 0.0 && *s <= 1.0);
            let cap = max_share.map(|share| {
                let config =
                    LimiterConfig::new(share * reservation.limiter.config().entitlement_wu_s);
                match previous.and_then(|p| p.cap.clone()) {
                    // Keep the cap's bucket level across snapshots, as for reservations.
                    Some(existing) => {
                        existing.reconfigure(config, BoundaryPolicy::default(), now);
                        existing
                    }
                    None => ReservationLimiter::new(config, BoundaryPolicy::default(), now),
                }
            });
            let dep = Arc::new(Deployment {
                id: d.id.clone(),
                reservation: Arc::clone(reservation),
                estimator,
                max_share,
                cap,
            });
            view.by_key_hash
                .insert(d.api_key_sha256.clone(), (Arc::clone(&dep), None));
            for old in &d.previous_keys {
                view.by_key_hash.insert(
                    old.api_key_sha256.clone(),
                    (Arc::clone(&dep), Some(old.expires_at_ms)),
                );
            }
            view.by_deployment.insert(d.id.clone(), dep);
        }
        report.reservations = view.reservations.len();
        report.deployments = view.by_deployment.len();
        for s in &report.skipped {
            tracing::warn!(reason = %s, version, "entitlement skipped");
        }
        (view, report)
    }
}
