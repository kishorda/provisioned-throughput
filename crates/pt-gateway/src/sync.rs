//! Pull entitlement snapshots from the control plane (docs/03 §2.1, ADR-007).
//!
//! - Long-polls `GET /internal/v1/entitlements/{region}` with `If-None-Match`, so changes
//!   arrive within one round trip.
//! - Verifies the Ed25519 signature before parsing, and applies only newer versions for
//!   this region.
//! - Writes each applied snapshot to `cache_path`. On start, the cache is loaded first, so
//!   the gateway serves its last-known-good entitlements even if the control plane is down.
//! - On errors it backs off (1 s to 30 s) and keeps serving what it has.

use std::path::Path;
use std::time::Duration;

use pt_entitlement::{SnapshotVerifier, KEY_ID_HEADER, SIGNATURE_HEADER};
use serde::{Deserialize, Serialize};

use crate::config::EntitlementSourceConfig;
use crate::state::{AppState, ApplyError};

/// On-disk cache: the exact signed bytes plus the signature, so the cache is verified the
/// same way as a fresh fetch.
#[derive(Serialize, Deserialize)]
struct CachedSnapshot {
    signature: String,
    body: String,
    /// Absent in caches written before key ids.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("control plane returned {0}")]
    Status(reqwest::StatusCode),
    #[error("missing {SIGNATURE_HEADER} header")]
    MissingSignature,
    #[error("snapshot rejected: {0}")]
    Verify(#[from] pt_entitlement::Error),
    #[error("snapshot not applied: {0}")]
    Apply(#[from] ApplyError),
    #[error("cache: {0}")]
    Cache(String),
}

pub struct SnapshotClient {
    config: EntitlementSourceConfig,
    verifier: SnapshotVerifier,
    http: reqwest::Client,
}

impl SnapshotClient {
    pub fn new(config: EntitlementSourceConfig) -> Result<Self, SyncError> {
        let verifier = SnapshotVerifier::from_hex_list(config.trusted_keys())?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.wait_secs + 10))
            .build()?;
        Ok(Self {
            config,
            verifier,
            http,
        })
    }

    /// Apply the cached snapshot, if there is a valid one. Returns its version.
    pub fn load_cache(&self, app: &AppState) -> Result<Option<u64>, SyncError> {
        let Some(path) = &self.config.cache_path else {
            return Ok(None);
        };
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(SyncError::Cache(format!("reading {path}: {e}"))),
        };
        let cached: CachedSnapshot = serde_json::from_str(&text)
            .map_err(|e| SyncError::Cache(format!("parsing {path}: {e}")))?;
        let (snapshot, key_id) = self.verifier.verify_with(
            cached.body.as_bytes(),
            &cached.signature,
            cached.key_id.as_deref(),
        )?;
        let report = app.apply_signed_snapshot(&snapshot, Some(&key_id))?;
        tracing::info!(version = report.version, reservations = report.reservations, %key_id, %path, "loaded cached entitlements");
        Ok(Some(report.version))
    }

    /// Fetch once. Returns the applied version, or `None` if nothing changed.
    pub async fn poll_once(&self, app: &AppState, wait: bool) -> Result<Option<u64>, SyncError> {
        let current = app.entitlements().version;
        let mut req = self
            .http
            .get(self.config.snapshot_url())
            .bearer_auth(&self.config.token);
        if current > 0 {
            req = req.header(reqwest::header::IF_NONE_MATCH, format!("\"{current}\""));
            if wait {
                req = req.query(&[("wait", self.config.wait_secs)]);
            }
        }
        let resp = req.send().await?;
        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(SyncError::Status(resp.status()));
        }
        let signature = resp
            .headers()
            .get(SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or(SyncError::MissingSignature)?
            .to_string();
        let key_id = resp
            .headers()
            .get(KEY_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body = resp.bytes().await?;
        let (snapshot, key_id) = self
            .verifier
            .verify_with(&body, &signature, key_id.as_deref())?;
        let report = match app.apply_signed_snapshot(&snapshot, Some(&key_id)) {
            Ok(r) => r,
            // Same content under an older or equal version: nothing to do.
            Err(ApplyError::Stale { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        tracing::info!(
            version = report.version,
            reservations = report.reservations,
            deployments = report.deployments,
            skipped = report.skipped.len(),
            %key_id,
            "applied entitlement snapshot"
        );
        if let Some(path) = &self.config.cache_path {
            let cached = CachedSnapshot {
                signature,
                body: String::from_utf8(body.to_vec())
                    .map_err(|e| SyncError::Cache(e.to_string()))?,
                key_id: Some(key_id),
            };
            if let Err(e) = write_atomic(
                Path::new(path),
                &serde_json::to_vec(&cached).expect("serialises"),
            ) {
                tracing::warn!(error = %e, %path, "failed to write entitlement cache");
            }
        }
        Ok(Some(report.version))
    }

    /// Poll forever. Errors never clear the current entitlements.
    pub async fn run(self, app: AppState) {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.poll_once(&app, true).await {
                Ok(_) => backoff = Duration::from_secs(1),
                Err(e) => {
                    let e = e.to_string();
                    let current = app.entitlements();
                    tracing::warn!(
                        error = %e,
                        retry_in = ?backoff,
                        version = current.version,
                        generated_at = current.generated_at.as_deref().unwrap_or("never"),
                        "entitlement sync failed; still serving the current entitlements"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)
}
