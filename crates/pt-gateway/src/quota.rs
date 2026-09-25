//! Renew quota leases with the regional Quota Coordinator (docs/04 §6, ADR-003, ADR-012).
//!
//! Every `renew_interval_ms` the gateway reports each reservation's demand and receives a
//! lease: the WU/s it may admit locally. Admission itself never waits on the coordinator.
//! If renewals fail, leases expire and each limiter decays towards its fallback share.
//!
//! With coordinator replicas (ADR-027), the gateway tries each URL in turn, starting with
//! the one that answered last, until the leader grants. It reports the lease it holds, so
//! a new leader warming up never grants it more.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use pt_quota::wire::{RenewRequest, RenewResponse};

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
    urls: Vec<String>,
    /// Index into `urls` of the replica that answered last.
    current: AtomicUsize,
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
        let urls = std::iter::once(&config.coordinator_url)
            .chain(&config.standby_urls)
            .map(|u| u.trim_end_matches('/').to_string())
            .collect();
        Ok(Self {
            config,
            gateway_id,
            http,
            urls,
            current: AtomicUsize::new(0),
        })
    }

    /// The coordinator URL that answered last.
    pub fn current_url(&self) -> &str {
        &self.urls[self.current.load(Ordering::Relaxed) % self.urls.len()]
    }

    pub fn gateway_id(&self) -> &str {
        &self.gateway_id
    }

    /// One renewal. `elapsed` is the time since the previous demand report.
    pub async fn renew_once(&self, app: &AppState, elapsed: Duration) -> Result<(), QuotaError> {
        let req = RenewRequest {
            gateway_id: self.gateway_id.clone(),
            reservations: app.demand_report(elapsed, Instant::now()),
        };
        let start = self.current.load(Ordering::Relaxed);
        let mut last = None;
        for i in 0..self.urls.len() {
            let n = (start + i) % self.urls.len();
            match self.renew_at(&self.urls[n], &req).await {
                Ok(leases) => {
                    self.current.store(n, Ordering::Relaxed);
                    app.apply_leases(&leases, Instant::now());
                    return Ok(());
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.expect("at least one coordinator URL"))
    }

    async fn renew_at(&self, url: &str, req: &RenewRequest) -> Result<RenewResponse, QuotaError> {
        let resp = self
            .http
            .post(format!("{url}/v1/leases/renew"))
            .bearer_auth(&self.config.token)
            .json(req)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(QuotaError::Status(resp.status()));
        }
        Ok(resp.json().await?)
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
