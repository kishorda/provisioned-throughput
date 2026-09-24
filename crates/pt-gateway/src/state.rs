//! Runtime state: the current entitlements, swapped atomically when a snapshot arrives.
//!
//! Requests look up deployments by API-key hash in the current [`Entitlements`]. Applying
//! a new snapshot builds a new view, reusing each reservation's limiter (reconfigured in
//! place) and each deployment's output estimator. Bucket levels, debt, burst credit, and
//! in-flight settlements survive resizes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Instant;

use pt_admission::{LimiterConfig, OutputEstimator, ReservationLimiter};
use pt_core::{ApproxTokenCounter, PerformanceProfile, Shape, Tier, TokenCounter};
use pt_entitlement::{sha256_hex, DeploymentEntitlement, ReservationEntitlement, Snapshot};

use crate::config::{ConfigError, GatewayConfig};
use crate::usage::UsageSink;

pub struct Reservation {
    pub id: String,
    pub tenant: String,
    pub model: String,
    pub cus: u32,
    pub tier: Tier,
    pub profile: PerformanceProfile,
    pub shape: Shape,
    pub limiter: ReservationLimiter,
}

pub struct Deployment {
    pub id: String,
    pub reservation: Arc<Reservation>,
    estimator: Arc<Mutex<OutputEstimator>>,
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
    by_key_hash: HashMap<String, Arc<Deployment>>,
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

    pub fn deployment_for_key(&self, api_key: &str) -> Option<Arc<Deployment>> {
        self.entitlements()
            .by_key_hash
            .get(&sha256_hex(api_key.as_bytes()))
            .cloned()
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
            let config = LimiterConfig::new(f64::from(r.cus) * self.wu_per_cu);
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
            let dep = Arc::new(Deployment {
                id: d.id.clone(),
                reservation: Arc::clone(reservation),
                estimator,
            });
            view.by_key_hash
                .insert(d.api_key_sha256.clone(), Arc::clone(&dep));
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
