//! Control-plane configuration.
//!
//! Tenants, the model catalog, and regional capacity come from a TOML file for now. In
//! production they come from the tenant directory, the Profile Registry, and the Capacity
//! Planner (docs/03 §2.1).

use std::collections::HashSet;
use std::path::Path;

use pt_core::Tier;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPlaneConfig {
    pub server: ServerConfig,
    pub pricing: PricingConfig,
    pub tenants: Vec<TenantConfig>,
    pub models: Vec<ModelConfig>,
    pub capacity: Vec<CapacityConfig>,
    pub entitlements: EntitlementsConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    /// Regions whose gateways pull entitlement snapshots and push usage records.
    pub regions: Vec<RegionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntitlementsConfig {
    /// Ed25519 seed (32 bytes, hex) that signs snapshots. Generate one with
    /// `pt-control-plane keygen`. From a secret store in production.
    pub signing_key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// WU/s per CU, for utilisation. Must match the gateways' `server.wu_per_cu`.
    #[serde(default = "default_wu_per_cu")]
    pub wu_per_cu: f64,
    /// Usage records older than this are dropped.
    #[serde(default = "default_retention_days")]
    pub retention_days: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            wu_per_cu: default_wu_per_cu(),
            retention_days: default_retention_days(),
        }
    }
}

fn default_wu_per_cu() -> f64 {
    1_000.0
}
fn default_retention_days() -> u64 {
    35
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionConfig {
    pub name: String,
    /// Bearer token the region's gateways use to pull snapshots.
    pub token: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub listen: String,
    /// Customer endpoint per region. `{region}` is replaced.
    #[serde(default = "default_endpoint_template")]
    pub endpoint_template: String,
    /// How often to activate, renew, and end reservations.
    #[serde(default = "default_lifecycle_interval_secs")]
    pub lifecycle_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricingConfig {
    #[serde(default = "default_currency")]
    pub currency: String,
    /// Price of one Standard-tier CU for one month, in minor units (cents).
    pub base_cu_price_per_month_cents: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantConfig {
    pub id: String,
    /// Management API key. Plaintext for local development only.
    pub admin_api_key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub id: String,
    pub display_name: String,
    /// Largest context window the model supports, in tokens.
    pub max_context: u64,
    /// Tiers that can be bought for this model.
    pub tiers: Vec<Tier>,
}

/// Sellable CUs for a model in a region.
///
/// Simplification: capacity is counted in CUs regardless of tier. The real Capacity Planner
/// converts CUs to replicas per tier and pool (docs/06 §2).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityConfig {
    pub region: String,
    pub model: String,
    pub cus: u32,
    /// Longest context the region's pools for this model can serve. Pools without
    /// disaggregation typically serve less than the model's maximum.
    pub max_context: u64,
    /// `PerformanceProfile` of the region's pool, passed to gateways in snapshots.
    pub profile: String,
}

fn default_endpoint_template() -> String {
    "https://{region}.pt.example.com/v1".into()
}
fn default_lifecycle_interval_secs() -> u64 {
    60
}
fn default_currency() -> String {
    "USD".into()
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {0}: {1}")]
    Io(String, std::io::Error),
    #[error("parsing {0}: {1}")]
    Parse(String, toml::de::Error),
    #[error("{0}")]
    Invalid(String),
}

impl ControlPlaneConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let p = path.as_ref().display().to_string();
        let text = std::fs::read_to_string(&path).map_err(|e| ConfigError::Io(p.clone(), e))?;
        let config: Self = toml::from_str(&text).map_err(|e| ConfigError::Parse(p, e))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |m: String| Err(ConfigError::Invalid(m));
        let mut ids = HashSet::new();
        let mut keys = HashSet::new();
        for t in &self.tenants {
            if !ids.insert(&t.id) {
                return invalid(format!("duplicate tenant {}", t.id));
            }
            if !keys.insert(&t.admin_api_key) {
                return invalid(format!("tenant {} reuses another tenant's API key", t.id));
            }
        }
        let mut models = HashSet::new();
        for m in &self.models {
            if !models.insert(m.id.as_str()) {
                return invalid(format!("duplicate model {}", m.id));
            }
            if m.tiers.is_empty() {
                return invalid(format!("model {} offers no tiers", m.id));
            }
        }
        let mut pools = HashSet::new();
        for c in &self.capacity {
            let Some(m) = self.model(&c.model) else {
                return invalid(format!("capacity for unknown model {}", c.model));
            };
            if !pools.insert((c.region.as_str(), c.model.as_str())) {
                return invalid(format!(
                    "duplicate capacity for {} in {}",
                    c.model, c.region
                ));
            }
            if c.max_context > m.max_context {
                return invalid(format!(
                    "capacity for {} in {} claims more context than the model supports",
                    c.model, c.region
                ));
            }
        }
        let mut region_names = HashSet::new();
        let mut tokens = HashSet::new();
        for r in &self.regions {
            if !region_names.insert(r.name.as_str()) {
                return invalid(format!("duplicate region {}", r.name));
            }
            if !tokens.insert(r.token.as_str()) {
                return invalid(format!("region {} reuses another region's token", r.name));
            }
        }
        for c in &self.capacity {
            if !region_names.contains(c.region.as_str()) {
                return invalid(format!(
                    "capacity in {} but no [[regions]] entry for it",
                    c.region
                ));
            }
        }
        if pt_entitlement::SnapshotSigner::from_hex(&self.entitlements.signing_key).is_err() {
            return invalid("entitlements.signing_key must be 32 bytes of hex".into());
        }
        if self.telemetry.wu_per_cu <= 0.0 || self.telemetry.retention_days == 0 {
            return invalid(
                "telemetry.wu_per_cu and telemetry.retention_days must be positive".into(),
            );
        }
        if !self.server.endpoint_template.contains("{region}") {
            return invalid("server.endpoint_template must contain {region}".into());
        }
        Ok(())
    }

    pub fn model(&self, id: &str) -> Option<&ModelConfig> {
        self.models.iter().find(|m| m.id == id)
    }

    pub fn tenant_for_key(&self, key: &str) -> Option<&str> {
        self.tenants
            .iter()
            .find(|t| t.admin_api_key == key)
            .map(|t| t.id.as_str())
    }

    /// The region a snapshot token belongs to.
    pub fn region_for_token(&self, token: &str) -> Option<&str> {
        self.regions
            .iter()
            .find(|r| r.token == token)
            .map(|r| r.name.as_str())
    }

    pub fn capacity_for(&self, region: &str, model: &str) -> Option<&CapacityConfig> {
        self.capacity
            .iter()
            .find(|c| c.region == region && c.model == model)
    }

    pub fn regions_for(&self, model: &str) -> Vec<&str> {
        self.capacity
            .iter()
            .filter(|c| c.model == model)
            .map(|c| c.region.as_str())
            .collect()
    }
}
