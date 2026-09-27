//! Send the router's status to the control plane for the system dashboard (ADR-043).
//!
//! Every `interval_secs` it POSTs what `/v1/router/status` serves to
//! `/internal/v1/reports/routers` with the region's token. Best effort: a failure is logged
//! and the next interval tries again. Nothing on the request path waits for it.

use std::sync::Arc;
use std::time::Duration;

use pt_entitlement::report::RouterReport;

use crate::config::ReportConfig;
use crate::http::{status_json, Shared};

pub struct Reporter {
    http: reqwest::Client,
    url: String,
    token: String,
    router_id: String,
    interval: Duration,
}

/// `${VAR}` is read from the environment; anything else is used as is.
fn expand(id: &str) -> anyhow::Result<String> {
    match id.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        Some(var) => std::env::var(var).map_err(|_| anyhow::anyhow!("{var} is not set")),
        None => Ok(id.to_string()),
    }
}

impl Reporter {
    pub fn new(c: &ReportConfig) -> anyhow::Result<Self> {
        c.tls
            .check(&c.control_plane_url)
            .map_err(anyhow::Error::msg)?;
        let http = c
            .tls
            .client_builder()
            .map_err(anyhow::Error::msg)?
            .timeout(Duration::from_secs(10))
            .build()?;
        let router_id = expand(&c.router_id)?;
        anyhow::ensure!(
            (1..=128).contains(&router_id.len()),
            "router_id must be 1 to 128 characters"
        );
        Ok(Self {
            http,
            url: format!(
                "{}/internal/v1/reports/routers",
                c.control_plane_url.trim_end_matches('/')
            ),
            token: c.token.clone(),
            router_id,
            interval: Duration::from_secs(c.interval_secs),
        })
    }

    /// Send one report now.
    pub async fn send(&self, shared: &Shared) -> Result<(), String> {
        let report = RouterReport {
            router_id: self.router_id.clone(),
            status: status_json(shared),
        };
        let resp = self
            .http
            .post(&self.url)
            .bearer_auth(&self.token)
            .json(&report)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if resp.status().is_success() {
            return Ok(());
        }
        let status = resp.status();
        Err(format!(
            "{status}: {}",
            resp.text().await.unwrap_or_default()
        ))
    }

    /// Report every interval, forever.
    pub async fn run(self, shared: Arc<Shared>) {
        let mut tick = tokio::time::interval(self.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(e) = self.send(&shared).await {
                tracing::warn!(error = %e, "couldn't report router status");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_ids_can_come_from_the_environment() {
        assert_eq!(expand("router-0").unwrap(), "router-0");
        std::env::set_var("PT_TEST_ROUTER_ID", "router-7");
        assert_eq!(expand("${PT_TEST_ROUTER_ID}").unwrap(), "router-7");
        assert!(expand("${PT_TEST_ROUTER_ID_UNSET}").is_err());
    }
}
