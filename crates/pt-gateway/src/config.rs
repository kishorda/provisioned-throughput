//! Gateway configuration.
//!
//! In production, reservations and deployments come from the entitlement snapshot pushed by
//! the global control plane (docs/03 §2). For P0 they're read from a TOML file with the same
//! shape.

use std::collections::HashSet;
use std::path::Path;

use pt_admission::BoundaryPolicy;
use pt_core::{PerformanceProfile, Shape, Tier};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    pub server: ServerConfig,
    pub profiles: Vec<PerformanceProfile>,
    pub reservations: Vec<ReservationConfig>,
    pub deployments: Vec<DeploymentConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub listen: String,
    /// Dynamo frontend (or mock engine) serving provisioned traffic.
    pub engine_url: String,
    /// PAYG pool for spillover. Falls back to `engine_url` when unset.
    #[serde(default)]
    pub payg_engine_url: Option<String>,
    /// JSONL file for usage records. Unset means stdout.
    #[serde(default)]
    pub usage_log: Option<String>,
    /// WU/s delivered by one Capacity Unit.
    pub wu_per_cu: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservationConfig {
    pub id: String,
    pub tenant: String,
    pub model: String,
    pub cus: u32,
    pub tier: Tier,
    pub profile: String,
    pub shape: Shape,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentConfig {
    pub id: String,
    pub reservation: String,
    /// Plaintext for local development only. The entitlement snapshot carries key hashes.
    pub api_key: String,
    #[serde(default)]
    pub boundary_policy: BoundaryPolicy,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("parsing {path}: {source}")]
    Parse {
        path: String,
        source: toml::de::Error,
    },
    #[error("{0}")]
    Invalid(String),
}

impl GatewayConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_str = path.as_ref().display().to_string();
        let text = std::fs::read_to_string(&path).map_err(|source| ConfigError::Io {
            path: path_str.clone(),
            source,
        })?;
        let config: Self = toml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path_str,
            source,
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Check cross-references, uniqueness, and positive sizes.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        if self.server.wu_per_cu <= 0.0 {
            return invalid("server.wu_per_cu must be positive".into());
        }
        let profiles: HashSet<_> = self.profiles.iter().map(|p| p.name.as_str()).collect();
        let mut reservations = HashSet::new();
        for r in &self.reservations {
            if !reservations.insert(r.id.as_str()) {
                return invalid(format!("duplicate reservation id {}", r.id));
            }
            if !profiles.contains(r.profile.as_str()) {
                return invalid(format!(
                    "reservation {} uses unknown profile {}",
                    r.id, r.profile
                ));
            }
            // Minimum reservation size is 1 CU.
            if r.cus < 1 {
                return invalid(format!("reservation {} must have at least 1 CU", r.id));
            }
        }
        let mut deployments = HashSet::new();
        let mut keys = HashSet::new();
        for d in &self.deployments {
            if !deployments.insert(d.id.as_str()) {
                return invalid(format!("duplicate deployment id {}", d.id));
            }
            if !keys.insert(d.api_key.as_str()) {
                return invalid(format!(
                    "deployment {} reuses another deployment's API key",
                    d.id
                ));
            }
            if !reservations.contains(d.reservation.as_str()) {
                return invalid(format!(
                    "deployment {} uses unknown reservation {}",
                    d.id, d.reservation
                ));
            }
        }
        Ok(())
    }
}
