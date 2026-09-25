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
use pt_entitlement::client_tls::ControlPlaneTls;
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
    /// How input tokens are counted before admission (ADR-028).
    #[serde(default)]
    pub tokenization: TokenizationConfig,
    /// Expect cache hits for prefixes this gateway sent recently (ADR-030).
    #[serde(default)]
    pub prefix_cache: PrefixCacheSettings,
    /// Push usage records to the control plane's telemetry API (docs/09).
    #[serde(default)]
    pub usage_export: Option<UsageExportConfig>,
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
    /// More trusted keys, during a signing-key rotation (docs/12 §6, ADR-020). Add the new
    /// key here before the control plane switches to it, and remove the old one once every
    /// gateway reports the new key id.
    #[serde(default)]
    pub extra_public_keys: Vec<String>,
    /// Last-known-good snapshot, so the gateway serves through control-plane outages and
    /// restarts. Strongly recommended.
    #[serde(default)]
    pub cache_path: Option<String>,
    /// Long-poll duration.
    #[serde(default = "default_wait_secs")]
    pub wait_secs: u64,
    /// Heartbeat to the control plane this often, so it can detect region failures
    /// (docs/07 §4). 0 turns heartbeats off.
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    /// Engine path probed before each heartbeat. The gateway reports serving only if it
    /// has entitlements and the engine answers 2xx here.
    #[serde(default = "default_engine_health_path")]
    pub engine_health_path: String,
    /// TLS to the control plane, for snapshots and heartbeats (ADR-022).
    #[serde(default)]
    pub tls: ControlPlaneTls,
}

fn default_wait_secs() -> u64 {
    30
}
fn default_heartbeat_interval_ms() -> u64 {
    5_000
}
fn default_engine_health_path() -> String {
    "/healthz".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageExportConfig {
    /// Control-plane base URL, for example `http://127.0.0.1:8090`.
    pub control_plane_url: String,
    /// The region's token; it also identifies the region on ingest.
    pub token: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    /// Records held while the control plane is unreachable. New records are dropped (and
    /// counted) beyond this.
    #[serde(default = "default_buffer")]
    pub buffer: usize,
    /// TLS to the control plane (ADR-022).
    #[serde(default)]
    pub tls: ControlPlaneTls,
}

fn default_batch_size() -> usize {
    500
}
fn default_flush_interval_ms() -> u64 {
    1_000
}
fn default_buffer() -> usize {
    100_000
}

/// The gateway's prefix-cache index (docs/04 §3, ADR-030).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrefixCacheSettings {
    /// Off: every input token is estimated as uncached prefill.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Prefixes remembered, across all reservations.
    #[serde(default = "default_prefix_capacity")]
    pub capacity: usize,
    /// A prefix not sent for this long is assumed evicted.
    #[serde(default = "default_prefix_ttl_secs")]
    pub ttl_secs: u64,
    /// Hit rate assumed before the engine reports cached tokens for a model. Placeholder.
    #[serde(default = "default_initial_hit_rate")]
    pub initial_hit_rate: f64,
}

impl Default for PrefixCacheSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: default_prefix_capacity(),
            ttl_secs: default_prefix_ttl_secs(),
            initial_hit_rate: default_initial_hit_rate(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_prefix_capacity() -> usize {
    pt_admission::PrefixCacheConfig::default().capacity
}
fn default_prefix_ttl_secs() -> u64 {
    pt_admission::PrefixCacheConfig::default().ttl.as_secs()
}
fn default_initial_hit_rate() -> f64 {
    pt_admission::PrefixCacheConfig::default().initial_hit_rate
}

impl PrefixCacheSettings {
    pub fn config(&self) -> pt_admission::PrefixCacheConfig {
        pt_admission::PrefixCacheConfig {
            capacity: self.capacity,
            ttl: std::time::Duration::from_secs(self.ttl_secs),
            initial_hit_rate: self.initial_hit_rate,
        }
    }
}

/// Input token counting (docs/04 §3, ADR-028).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenizationConfig {
    /// Each model's `tokenizer.json`. Models without one are counted by a bytes-per-token
    /// ratio learned from the engine's counts.
    #[serde(default)]
    pub tokenizers: Vec<pt_tokenize::TokenizerSpec>,
    /// Messages whose counts are cached, across all models.
    #[serde(default = "default_cache_entries")]
    pub cache_entries: usize,
    /// Most uncached bytes tokenized before admission. Longer new messages are estimated
    /// by ratio and tokenized in the background. Placeholder: about 2.5 ms of tokenizing.
    #[serde(default = "default_inline_bytes")]
    pub inline_bytes: usize,
}

impl Default for TokenizationConfig {
    fn default() -> Self {
        Self {
            tokenizers: vec![],
            cache_entries: default_cache_entries(),
            inline_bytes: default_inline_bytes(),
        }
    }
}

fn default_cache_entries() -> usize {
    100_000
}
fn default_inline_bytes() -> usize {
    4 * 1024
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaClientConfig {
    /// For example `http://127.0.0.1:8095`.
    pub coordinator_url: String,
    /// Other coordinator replicas (ADR-027). Only the leader answers; a standby returns
    /// 503 `not_leader`, and the gateway tries the next URL. It sticks with whichever
    /// answered last.
    #[serde(default)]
    pub standby_urls: Vec<String>,
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
    /// Every trusted public key: `public_key` then `extra_public_keys`.
    pub fn trusted_keys(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.public_key.as_str())
            .chain(self.extra_public_keys.iter().map(String::as_str))
    }

    pub fn heartbeat_url(&self) -> String {
        format!(
            "{}/internal/v1/heartbeats",
            self.control_plane_url.trim_end_matches('/')
        )
    }

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
    /// Cap on this deployment's share of the reservation's entitlement, in (0, 1].
    #[serde(default)]
    pub max_share: Option<f64>,
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
            if pt_entitlement::SnapshotVerifier::from_hex_list(e.trusted_keys()).is_err() {
                return invalid(
                    "entitlements.public_key and extra_public_keys must be 32-byte hex Ed25519 keys"
                        .into(),
                );
            }
            if e.wait_secs > 60 {
                return invalid("entitlements.wait_secs can be at most 60".into());
            }
            e.tls
                .check(&e.control_plane_url)
                .map_err(ConfigError::Invalid)?;
        }
        if let Some(u) = &self.usage_export {
            if u.batch_size == 0 || u.batch_size > 5_000 {
                return invalid("usage_export.batch_size must be between 1 and 5000".into());
            }
            if u.buffer < u.batch_size {
                return invalid("usage_export.buffer must be at least batch_size".into());
            }
            u.tls
                .check(&u.control_plane_url)
                .map_err(ConfigError::Invalid)?;
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
        if !(0.0..=1.0).contains(&self.prefix_cache.initial_hit_rate) {
            return invalid("prefix_cache.initial_hit_rate must be in [0, 1]".into());
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
