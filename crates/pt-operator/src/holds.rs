//! Tell the control plane to pause new sales on a pool (ADR-041).
//!
//! While a node drains expedited, a pool is working through its maintenance slots and hot
//! spares, so it shouldn't take on more reservations. The controller places a hold with the
//! region's token (`PUT /internal/v1/holds/{model}`), renews it while the drain lasts, and
//! lifts it when it's over. Holds expire, so a controller that dies can't pause sales for
//! good.
//!
//! The same client reports each pool's state for the system dashboard (ADR-043).

use std::time::Duration;

use pt_entitlement::report::PoolReport;
use serde_json::json;

use crate::failover::SnapshotSource;

/// How long a hold lasts unless renewed. Pools reconcile every 15 s while draining.
pub const HOLD_TTL: Duration = Duration::from_secs(120);

pub struct HoldClient {
    http: reqwest::Client,
    base: String,
    token: String,
}

impl HoldClient {
    /// From the snapshot source's control plane, region token, and TLS settings.
    pub fn new(source: &SnapshotSource) -> Result<Self, String> {
        source.tls.check(&source.control_plane_url)?;
        let http = source
            .tls
            .client_builder()?
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            http,
            base: source.control_plane_url.trim_end_matches('/').to_string(),
            token: source.token.clone(),
        })
    }

    pub async fn place(&self, model: &str, source: &str, reason: &str) -> Result<(), String> {
        let resp = self
            .http
            .put(format!("{}/internal/v1/holds/{model}", self.base))
            .bearer_auth(&self.token)
            .json(&json!({
                "source": source,
                "reason": reason,
                "ttl_seconds": HOLD_TTL.as_secs(),
            }))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        check(resp).await
    }

    pub async fn lift(&self, model: &str, source: &str) -> Result<(), String> {
        let resp = self
            .http
            .delete(format!("{}/internal/v1/holds/{model}", self.base))
            .query(&[("source", source)])
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        check(resp).await
    }
}

impl HoldClient {
    /// Report a pool's state for the system dashboard (ADR-043).
    pub async fn report_pool(&self, report: &PoolReport) -> Result<(), String> {
        let resp = self
            .http
            .post(format!("{}/internal/v1/reports/pools", self.base))
            .bearer_auth(&self.token)
            .json(report)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        check(resp).await
    }
}

async fn check(resp: reqwest::Response) -> Result<(), String> {
    if resp.status().is_success() {
        return Ok(());
    }
    let status = resp.status();
    Err(format!(
        "{status}: {}",
        resp.text().await.unwrap_or_default()
    ))
}
