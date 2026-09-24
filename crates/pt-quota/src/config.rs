//! Coordinator configuration.

use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::coordinator::CoordinatorConfig;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaConfig {
    pub listen: String,
    /// Bearer token gateways present. Shared within the region.
    pub token: String,
    #[serde(default = "default_lease_ttl_ms")]
    pub lease_ttl_ms: u64,
    #[serde(default = "default_floor_fraction")]
    pub floor_fraction: f64,
}

fn default_lease_ttl_ms() -> u64 {
    1_000
}
fn default_floor_fraction() -> f64 {
    0.1
}

impl QuotaConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(&path)?;
        let c: Self = toml::from_str(&text)?;
        anyhow::ensure!(c.lease_ttl_ms >= 100, "lease_ttl_ms must be at least 100");
        anyhow::ensure!(
            (0.0..1.0).contains(&c.floor_fraction),
            "floor_fraction must be in [0, 1)"
        );
        Ok(c)
    }

    pub fn coordinator(&self) -> CoordinatorConfig {
        CoordinatorConfig {
            lease_ttl: Duration::from_millis(self.lease_ttl_ms),
            floor_fraction: self.floor_fraction,
        }
    }
}
