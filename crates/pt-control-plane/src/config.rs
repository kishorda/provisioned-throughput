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
    /// Calibrated cost profiles, as published by the Profile Registry (docs/03 §2.1). Every
    /// capacity entry names one. Quotes price requests with them.
    pub profiles: Vec<pt_core::PerformanceProfile>,
    pub entitlements: EntitlementsConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    /// Regions whose gateways pull entitlement snapshots and push usage records.
    pub regions: Vec<RegionConfig>,
    /// Operator access, for declaring region incidents. Without it, the incident API is off.
    #[serde(default)]
    pub operators: Option<OperatorsConfig>,
    #[serde(default)]
    pub failover: FailoverConfig,
    /// Durable store (ADR-017). Without it, state is in memory and lost on restart.
    #[serde(default)]
    pub store: Option<StoreConfig>,
    /// PAYG list prices. Spillover is billed at these (docs/11 §4). Every model needs one.
    #[serde(default)]
    pub payg_prices: Vec<PaygPrice>,
    #[serde(default)]
    pub billing: BillingConfig,
    #[serde(default)]
    pub rebalance: RebalanceConfig,
}

/// Moving multi-region splits toward demand (docs/07 §3, ADR-024). Values are placeholders
/// until tuned on real traffic.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebalanceConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// How often the leader looks at demand.
    #[serde(default = "default_rebalance_interval_secs")]
    pub interval_secs: u64,
    /// Demand is attempted WU per region over this window, throttled requests included.
    #[serde(default = "default_rebalance_window_minutes")]
    pub window_minutes: u64,
    /// At least this long between two moves of one reservation's split.
    #[serde(default = "default_rebalance_cooldown_minutes")]
    pub cooldown_minutes: u64,
    /// The effective split may differ from the contract by at most this share of the CUs.
    #[serde(default = "default_rebalance_max_shift")]
    pub max_shift_fraction: f64,
    /// Too little traffic in the window leaves the split alone.
    #[serde(default = "default_rebalance_min_requests")]
    pub min_requests: usize,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: default_rebalance_interval_secs(),
            window_minutes: default_rebalance_window_minutes(),
            cooldown_minutes: default_rebalance_cooldown_minutes(),
            max_shift_fraction: default_rebalance_max_shift(),
            min_requests: default_rebalance_min_requests(),
        }
    }
}

fn default_rebalance_interval_secs() -> u64 {
    300
}
fn default_rebalance_window_minutes() -> u64 {
    15
}
fn default_rebalance_cooldown_minutes() -> u64 {
    15
}
fn default_rebalance_max_shift() -> f64 {
    0.2
}
fn default_rebalance_min_requests() -> usize {
    100
}

/// A model's PAYG list price, in minor currency units per million tokens.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaygPrice {
    pub model: String,
    pub input_per_mtok: u64,
    pub cached_input_per_mtok: u64,
    pub output_per_mtok: u64,
}

/// Monthly invoicing (docs/12 §7, ADR-018).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BillingConfig {
    /// A month's invoice is finalised this long after the month ends, so late usage
    /// records (gateway buffers, retries) are included.
    #[serde(default = "default_finalize_grace_hours")]
    pub finalize_grace_hours: u64,
    /// How often to look for invoices to finalise.
    #[serde(default = "default_finalize_interval_secs")]
    pub finalize_interval_secs: u64,
}

impl Default for BillingConfig {
    fn default() -> Self {
        Self {
            finalize_grace_hours: default_finalize_grace_hours(),
            finalize_interval_secs: default_finalize_interval_secs(),
        }
    }
}

fn default_finalize_grace_hours() -> u64 {
    48
}
fn default_finalize_interval_secs() -> u64 {
    3_600
}

/// A CockroachDB or PostgreSQL database.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreConfig {
    /// `postgres://user@host:port/db`. `PT_DATABASE_URL` overrides it, so passwords can stay
    /// out of the file.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Apply pending migrations at startup.
    #[serde(default = "default_true")]
    pub migrate: bool,
    /// Allow a connection to a non-loopback host without verified TLS
    /// (`sslmode=verify-ca` or `verify-full`). Off by default (ADR-021).
    #[serde(default)]
    pub allow_insecure_transport: bool,
}

impl StoreConfig {
    /// The URL to connect to: `PT_DATABASE_URL`, else `url`.
    pub fn resolved_url(&self) -> Option<String> {
        std::env::var("PT_DATABASE_URL")
            .ok()
            .filter(|u| !u.is_empty())
            .or_else(|| self.url.clone())
    }
}

fn default_max_connections() -> u32 {
    10
}

/// Automatic region-failure handling (docs/07 §4, ADR-014).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailoverConfig {
    /// Declare and resolve region incidents from gateway heartbeats. Operators can always
    /// declare them by hand.
    #[serde(default = "default_true")]
    pub auto_declare: bool,
    /// A region is down when no gateway has reported serving for this long.
    #[serde(default = "default_heartbeat_timeout_seconds")]
    pub heartbeat_timeout_seconds: u64,
    /// An automatic incident is resolved after the region has served continuously for
    /// this long.
    #[serde(default = "default_recovery_seconds")]
    pub recovery_seconds: u64,
    /// After recovery, failover entitlements ramp down and DNS weight ramps back up over
    /// this long (10% per minute by default), so KV caches warm up.
    #[serde(default = "default_return_ramp_minutes")]
    pub return_ramp_minutes: u64,
    /// How often the control plane checks region health.
    #[serde(default = "default_check_interval_ms")]
    pub check_interval_ms: u64,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            auto_declare: true,
            heartbeat_timeout_seconds: default_heartbeat_timeout_seconds(),
            recovery_seconds: default_recovery_seconds(),
            return_ramp_minutes: default_return_ramp_minutes(),
            check_interval_ms: default_check_interval_ms(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_heartbeat_timeout_seconds() -> u64 {
    30
}
fn default_recovery_seconds() -> u64 {
    60
}
fn default_return_ramp_minutes() -> u64 {
    10
}
fn default_check_interval_ms() -> u64 {
    1_000
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
    /// After a customer-initiated change (activation, CU increase, shape change, change
    /// applied at renewal), requests are excluded from the SLA for this long (docs/09 §4).
    #[serde(default = "default_change_grace_minutes")]
    pub change_grace_minutes: u64,
    /// Multi-region SKU: requests are excluded for this long after a region incident
    /// starts, while traffic fails over (docs/07 §4).
    #[serde(default = "default_failover_window_minutes")]
    pub failover_window_minutes: u64,
    /// Durable usage records (ADR-019). Without it, usage is in memory and lost on restart,
    /// along with the month's spillover charges and SLA data. `PT_CLICKHOUSE_URL`
    /// overrides `url`.
    #[serde(default)]
    pub clickhouse: Option<pt_telemetry::clickhouse::ClickHouseConfig>,
}

impl TelemetryConfig {
    /// ClickHouse settings with `PT_CLICKHOUSE_URL` applied, or `None` for in-memory usage.
    pub fn resolved_clickhouse(&self) -> Option<pt_telemetry::clickhouse::ClickHouseConfig> {
        let env = std::env::var("PT_CLICKHOUSE_URL")
            .ok()
            .filter(|u| !u.is_empty());
        match (&self.clickhouse, env) {
            (Some(c), Some(url)) => {
                Some(pt_telemetry::clickhouse::ClickHouseConfig { url, ..c.clone() })
            }
            (Some(c), None) => Some(c.clone()),
            (None, Some(url)) => Some(pt_telemetry::clickhouse::ClickHouseConfig {
                url,
                database: "pt".into(),
                user: "default".into(),
                password: None,
                ..Default::default()
            }),
            (None, None) => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorsConfig {
    /// Bearer key for the operator API. From a secret store in production.
    pub api_key: String,
}

fn default_change_grace_minutes() -> u64 {
    10
}
fn default_failover_window_minutes() -> u64 {
    5
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            wu_per_cu: default_wu_per_cu(),
            retention_days: default_retention_days(),
            change_grace_minutes: default_change_grace_minutes(),
            failover_window_minutes: default_failover_window_minutes(),
            clickhouse: None,
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
    /// Preferred failover region for Multi-region reservations that have a share in both.
    #[serde(default)]
    pub pair: Option<String>,
    /// Data-residency zone, for example `eu`. A Multi-region reservation's regions must all
    /// be in one zone, so failover never moves prompts out of it.
    #[serde(default)]
    pub residency: Option<String>,
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
    /// Serve HTTPS (ADR-022). Without it, the API is plain HTTP, for local development or
    /// behind a TLS-terminating proxy.
    #[serde(default)]
    pub tls: Option<crate::tls::ServerTlsConfig>,
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
        for r in &self.regions {
            if let Some(p) = &r.pair {
                if p == &r.name || !region_names.contains(p.as_str()) {
                    return invalid(format!(
                        "region {} pairs with {p}, which isn't another configured region",
                        r.name
                    ));
                }
            }
        }
        let mut priced = HashSet::new();
        for p in &self.payg_prices {
            if self.model(&p.model).is_none() {
                return invalid(format!("PAYG price for unknown model {}", p.model));
            }
            if !priced.insert(p.model.as_str()) {
                return invalid(format!("duplicate PAYG price for {}", p.model));
            }
        }
        if let Some(m) = self.models.iter().find(|m| !priced.contains(m.id.as_str())) {
            return invalid(format!(
                "model {} has no [[payg_prices]] entry, so its spillover can't be billed",
                m.id
            ));
        }
        if self.telemetry.retention_days * 24 < self.billing.finalize_grace_hours + 24 * 31 {
            return invalid(
                "telemetry.retention_days must keep usage until invoices are finalised (a month plus billing.finalize_grace_hours)"
                    .into(),
            );
        }
        let r = &self.rebalance;
        if !(0.0..=0.5).contains(&r.max_shift_fraction) || r.window_minutes == 0 {
            return invalid(
                "rebalance.max_shift_fraction must be in [0, 0.5] and window_minutes positive"
                    .into(),
            );
        }
        let f = &self.failover;
        if f.heartbeat_timeout_seconds == 0 || f.recovery_seconds == 0 || f.check_interval_ms == 0 {
            return invalid(
                "failover.heartbeat_timeout_seconds, recovery_seconds, and check_interval_ms must be positive"
                    .into(),
            );
        }
        for c in &self.capacity {
            if self.profile(&c.profile).is_none() {
                return invalid(format!(
                    "capacity for {} in {} uses unknown profile {}",
                    c.model, c.region, c.profile
                ));
            }
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

    pub fn profile(&self, name: &str) -> Option<&pt_core::PerformanceProfile> {
        self.profiles.iter().find(|p| p.name == name)
    }

    pub fn payg_price(&self, model: &str) -> Option<&PaygPrice> {
        self.payg_prices.iter().find(|p| p.model == model)
    }

    pub fn region(&self, name: &str) -> Option<&RegionConfig> {
        self.regions.iter().find(|r| r.name == name)
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
