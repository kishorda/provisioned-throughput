//! Gateway configuration.
//!
//! Reservations and deployments come from one of two sources:
//! - `[entitlements]`: signed snapshots pulled from the control plane (docs/03 §2.1, ADR-007);
//! - static `[[reservations]]` and `[[deployments]]` in this file, for local development.
//!
//! Performance profiles are always local: they describe this region's pools.

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
    #[serde(default)]
    pub entitlements: Option<EntitlementSourceConfig>,
    /// Share entitlements with other gateway replicas through the Quota Coordinator.
    /// Without it, this gateway enforces each reservation's full regional entitlement, so
    /// run only one replica per region.
    #[serde(default)]
    pub quota: Option<QuotaClientConfig>,
    #[serde(default)]
    pub reservations: Vec<ReservationConfig>,
    #[serde(default)]
    pub deployments: Vec<DeploymentConfig>,
}

/// Where to pull entitlement snapshots from.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntitlementSourceConfig {
    /// Control-plane base URL, for example `http://127.0.0.1:8090`.
    pub control_plane_url: String,
    /// This gateway's region. Snapshots for any other region are rejected.
    pub region: String,
    /// Region pull token.
    pub token: String,
    /// Control plane's Ed25519 public key (hex). Unsigned or mis-signed snapshots are rejected.
    pub public_key: String,
    /// Last-known-good snapshot, so the gateway serves through control-plane outages and
    /// restarts. Strongly recommended.
    #[serde(default)]
    pub cache_path: Option<String>,
    /// Long-poll duration.
    #[serde(default = "default_wait_secs")]
    pub wait_secs: u64,
}

fn default_wait_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaClientConfig {
    /// For example `http://127.0.0.1:8095`.
    pub coordinator_url: String,
    pub token: String,
    /// Defaults to a random id per process.
    #[serde(default)]
    pub gateway_id: Option<String>,
    #[serde(default = "default_renew_interval_ms")]
    pub renew_interval_ms: u64,
    /// Before the first lease, each gateway admits entitlement ÷ this.
    #[serde(default = "default_assumed_gateways")]
    pub assumed_gateways: u32,
    /// After a lease expires, the rate moves linearly from the last lease to
    /// 50% × entitlement ÷ active gateways over this long (ADR-003).
    #[serde(default = "default_fallback_decay_secs")]
    pub fallback_decay_secs: f64,
}

fn default_renew_interval_ms() -> u64 {
    250
}
fn default_assumed_gateways() -> u32 {
    3
}
fn default_fallback_decay_secs() -> f64 {
    30.0
}

impl EntitlementSourceConfig {
    pub fn snapshot_url(&self) -> String {
        format!(
            "{}/internal/v1/entitlements/{}",
            self.control_plane_url.trim_end_matches('/'),
            self.region
        )
    }
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
        if let Some(e) = &self.entitlements {
            if !self.reservations.is_empty() || !self.deployments.is_empty() {
                return invalid(
                    "use either [entitlements] or static [[reservations]]/[[deployments]], not both"
                        .into(),
                );
            }
            if pt_entitlement::SnapshotVerifier::from_hex(&e.public_key).is_err() {
                return invalid("entitlements.public_key must be a 32-byte hex Ed25519 key".into());
            }
            if e.wait_secs > 60 {
                return invalid("entitlements.wait_secs can be at most 60".into());
            }
        }
        if let Some(q) = &self.quota {
            if q.assumed_gateways < 1 {
                return invalid("quota.assumed_gateways must be at least 1".into());
            }
            if q.renew_interval_ms < 20 {
                return invalid("quota.renew_interval_ms must be at least 20".into());
            }
            if q.fallback_decay_secs.is_nan() || q.fallback_decay_secs <= 0.0 {
                return invalid("quota.fallback_decay_secs must be positive".into());
            }
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
