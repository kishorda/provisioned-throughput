//! Region liveness (docs/07 §4, ADR-014).
//!
//! Every `heartbeat_interval_ms` the gateway tells the control plane it's alive and whether
//! it's serving: it has entitlements, and its engine answers health checks. When no gateway
//! in a region reports serving, the control plane declares the region down, and the paired
//! regions' failover entitlements activate.
//!
//! [`run_rate_refresh`] keeps limiters in step with failover entitlements, which grow when
//! a region fails and ramp down after it recovers, without a new snapshot.

use std::time::{Duration, Instant};

use serde_json::json;

use crate::config::EntitlementSourceConfig;
use crate::state::{AppState, EntitlementSource};

pub struct HeartbeatClient {
    config: EntitlementSourceConfig,
    gateway_id: String,
    http: reqwest::Client,
}

impl HeartbeatClient {
    pub fn new(config: EntitlementSourceConfig, gateway_id: String) -> reqwest::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()?;
        Ok(Self {
            config,
            gateway_id,
            http,
        })
    }

    /// Whether this gateway can serve: it has applied entitlements and the engine is up.
    pub async fn serving(&self, app: &AppState) -> bool {
        if !matches!(
            app.entitlements().source,
            EntitlementSource::Snapshot { .. }
        ) {
            return false;
        }
        let url = format!("{}{}", app.engine_url, self.config.engine_health_path);
        matches!(self.http.get(url).send().await, Ok(r) if r.status().is_success())
    }

    /// Send one heartbeat. Returns whether it reported serving.
    pub async fn beat(&self, app: &AppState) -> Result<bool, reqwest::Error> {
        let serving = self.serving(app).await;
        self.http
            .post(self.config.heartbeat_url())
            .bearer_auth(&self.config.token)
            .json(&json!({
                "gateway_id": self.gateway_id,
                "serving": serving,
                "snapshot_version": app.entitlements().version,
                "key_id": app.entitlements().key_id.clone(),
            }))
            .send()
            .await?
            .error_for_status()?;
        Ok(serving)
    }

    pub async fn run(self, app: AppState) {
        let mut tick =
            tokio::time::interval(Duration::from_millis(self.config.heartbeat_interval_ms));
        let mut last = None;
        loop {
            tick.tick().await;
            match self.beat(&app).await {
                Ok(serving) if last != Some(serving) => {
                    tracing::info!(serving, gateway_id = %self.gateway_id, "heartbeat");
                    last = Some(serving);
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "heartbeat failed"),
            }
        }
    }
}

/// Re-apply local rates every `every`, so failover entitlements take effect and ramp down
/// between snapshots.
pub async fn run_rate_refresh(app: AppState, every: Duration) {
    let mut tick = tokio::time::interval(every);
    loop {
        tick.tick().await;
        app.refresh_local_rates(Instant::now());
    }
}
