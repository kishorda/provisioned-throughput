//! Runtime state built from the configuration.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use pt_admission::{LimiterConfig, OutputEstimator, ReservationLimiter};
use pt_core::{ApproxTokenCounter, PerformanceProfile, Shape, Tier, TokenCounter};

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
    estimator: Mutex<OutputEstimator>,
}

impl Deployment {
    pub fn estimator(&self) -> MutexGuard<'_, OutputEstimator> {
        self.estimator.lock().unwrap_or_else(|e| e.into_inner())
    }
}

pub struct Inner {
    by_api_key: HashMap<String, Arc<Deployment>>,
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
    pub fn new(config: &GatewayConfig, sink: Arc<dyn UsageSink>) -> Result<Self, ConfigError> {
        config.validate()?;
        let now = Instant::now();

        // Each reservation gets one limiter, shared by all of its deployments.
        let mut reservations = HashMap::new();
        for r in &config.reservations {
            let profile = config
                .profiles
                .iter()
                .find(|p| p.name == r.profile)
                .cloned()
                .expect("validated");
            reservations.insert(r.id.clone(), (r, profile));
        }

        let mut limiters: HashMap<String, Arc<Reservation>> = HashMap::new();
        let mut by_api_key = HashMap::new();
        for d in &config.deployments {
            let (r, profile) = &reservations[&d.reservation];
            let reservation = limiters
                .entry(r.id.clone())
                .or_insert_with(|| {
                    let entitlement = f64::from(r.cus) * config.server.wu_per_cu;
                    Arc::new(Reservation {
                        id: r.id.clone(),
                        tenant: r.tenant.clone(),
                        model: r.model.clone(),
                        cus: r.cus,
                        tier: r.tier,
                        profile: profile.clone(),
                        shape: r.shape,
                        // The first deployment's boundary policy applies to the reservation.
                        limiter: ReservationLimiter::new(
                            LimiterConfig::new(entitlement),
                            d.boundary_policy.clone(),
                            now,
                        ),
                    })
                })
                .clone();
            if reservation.limiter.policy() != &d.boundary_policy {
                return Err(ConfigError::Invalid(format!(
                    "deployment {} has a different boundary policy from other deployments of reservation {}",
                    d.id, r.id
                )));
            }
            by_api_key.insert(
                d.api_key.clone(),
                Arc::new(Deployment {
                    id: d.id.clone(),
                    estimator: Mutex::new(OutputEstimator::new(r.shape.output_p95)),
                    reservation,
                }),
            );
        }

        let engine_url = config.server.engine_url.trim_end_matches('/').to_string();
        let payg_engine_url = config.server.payg_engine_url.as_deref().map_or_else(
            || engine_url.clone(),
            |u| u.trim_end_matches('/').to_string(),
        );

        Ok(Self(Arc::new(Inner {
            by_api_key,
            http: reqwest::Client::new(),
            engine_url,
            payg_engine_url,
            sink,
            tokens: Arc::new(ApproxTokenCounter),
        })))
    }

    pub fn deployment_for_key(&self, api_key: &str) -> Option<Arc<Deployment>> {
        self.by_api_key.get(api_key).cloned()
    }
}
