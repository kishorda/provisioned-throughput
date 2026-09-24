//! Renew quota leases with the regional Quota Coordinator (docs/04 §6, ADR-003, ADR-012).
//!
//! Every `renew_interval_ms` the gateway reports each reservation's demand and receives a
//! lease: the WU/s it may admit locally. Admission itself never waits on the coordinator.
//! If renewals fail, leases expire and each limiter decays towards its fallback share.

use std::time::{Duration, Instant};

use pt_quota::wire::RenewRequest;

use crate::config::QuotaClientConfig;
use crate::state::AppState;

#[derive(Debug, thiserror::Error)]
pub enum QuotaError {
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("coordinator returned {0}")]
    Status(reqwest::StatusCode),
}

pub struct QuotaClient {
    config: QuotaClientConfig,
    gateway_id: String,
    http: reqwest::Client,
}

impl QuotaClient {
    pub fn new(config: QuotaClientConfig) -> Result<Self, QuotaError> {
        let gateway_id = config
            .gateway_id
            .clone()
            .unwrap_or_else(|| format!("gw-{}", uuid::Uuid::new_v4().simple()));
        let timeout =
            Duration::from_millis(config.renew_interval_ms * 4).max(Duration::from_millis(500));
        let http = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self {
            config,
            gateway_id,
            http,
        })
    }

    pub fn gateway_id(&self) -> &str {
        &self.gateway_id
    }

    /// One renewal. `elapsed` is the time since the previous demand report.
    pub async fn renew_once(&self, app: &AppState, elapsed: Duration) -> Result<(), QuotaError> {
        let req = RenewRequest {
            gateway_id: self.gateway_id.clone(),
            reservations: app.demand_report(elapsed),
        };
        let resp = self
            .http
            .post(format!(
                "{}/v1/leases/renew",
                self.config.coordinator_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.config.token)
            .json(&req)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(QuotaError::Status(resp.status()));
        }
        let leases = resp.json().await?;
        app.apply_leases(&leases, Instant::now());
        Ok(())
    }

    /// Renew forever. Failures are logged; limiters keep adjusting to their fallback rates.
    pub async fn run(self, app: AppState) {
        let mut tick = tokio::time::interval(Duration::from_millis(self.config.renew_interval_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = Instant::now();
        let mut failures: u64 = 0;
        loop {
            tick.tick().await;
            let now = Instant::now();
            let elapsed = now - last;
            last = now;
            match self.renew_once(&app, elapsed).await {
                Ok(()) => {
                    if failures > 0 {
                        tracing::info!(after_failures = failures, "quota leases renewed again");
                    }
                    failures = 0;
                }
                Err(e) => {
                    failures += 1;
                    // Log the first failure and then every ~5 s at the default interval.
                    if failures == 1 || failures % 20 == 0 {
                        tracing::warn!(error = %e, failures, "quota renewal failed; using fallback shares");
                    }
                }
            }
            // Moves expired leases along their fallback decay.
            app.refresh_local_rates(Instant::now());
        }
    }
}
