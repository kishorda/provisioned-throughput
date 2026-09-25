//! Coordinator configuration.

use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::coordinator::CoordinatorConfig;
use crate::election::ElectionConfig;

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
    /// Active/standby replicas (ADR-027). Without it, this is the region's only coordinator.
    #[serde(default)]
    pub election: Option<ElectionSettings>,
}

/// Leader election through a Kubernetes Lease.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElectionSettings {
    pub namespace: String,
    pub lease_name: String,
    /// This replica's identity. Defaults to `$POD_NAME`, then `$HOSTNAME`.
    #[serde(default)]
    pub identity: Option<String>,
    #[serde(default = "default_lease_duration_ms")]
    pub lease_duration_ms: u64,
    #[serde(default = "default_renew_deadline_ms")]
    pub renew_deadline_ms: u64,
    #[serde(default = "default_retry_period_ms")]
    pub retry_period_ms: u64,
}

fn default_lease_duration_ms() -> u64 {
    5_000
}
fn default_renew_deadline_ms() -> u64 {
    3_000
}
fn default_retry_period_ms() -> u64 {
    1_000
}

impl ElectionSettings {
    pub fn config(&self) -> anyhow::Result<ElectionConfig> {
        let identity = match &self.identity {
            Some(i) => i.clone(),
            None => std::env::var("POD_NAME")
                .or_else(|_| std::env::var("HOSTNAME"))
                .map_err(|_| anyhow::anyhow!("set election.identity, POD_NAME, or HOSTNAME"))?,
        };
        Ok(ElectionConfig {
            identity,
            lease_duration: Duration::from_millis(self.lease_duration_ms),
            renew_deadline: Duration::from_millis(self.renew_deadline_ms),
            retry_period: Duration::from_millis(self.retry_period_ms),
        })
    }
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
        if let Some(e) = &c.election {
            e.config()?
                .validate(c.coordinator().grant_hold())
                .map_err(anyhow::Error::msg)?;
        }
        Ok(c)
    }

    pub fn coordinator(&self) -> CoordinatorConfig {
        CoordinatorConfig {
            lease_ttl: Duration::from_millis(self.lease_ttl_ms),
            floor_fraction: self.floor_fraction,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(text: &str) -> anyhow::Result<QuotaConfig> {
        let dir = std::env::temp_dir().join(format!("pt-quota-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.toml", text.len()));
        std::fs::write(&path, text)?;
        QuotaConfig::load(&path)
    }

    #[test]
    fn election_timing_must_outlast_grants() {
        let base = r#"
            listen = "127.0.0.1:0"
            token = "t"
            lease_ttl_ms = 1000
            [election]
            namespace = "pt-system"
            lease_name = "pt-quota"
            identity = "q-0"
        "#;
        let c = load(base).unwrap();
        let e = c.election.unwrap().config().unwrap();
        assert_eq!(e.lease_duration - e.renew_deadline, Duration::from_secs(2));
        // 1.5 × 2 s grants outlive a 2 s gap between the leader stopping and a takeover.
        let long = base.replace("lease_ttl_ms = 1000", "lease_ttl_ms = 2000");
        let err = load(&long).unwrap_err().to_string();
        assert!(err.contains("grant hold"), "{err}");
    }
}
