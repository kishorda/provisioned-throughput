//! Failover demand from the regional entitlement snapshot (docs/07 §4, ADR-016).
//!
//! The controller follows the same signed snapshot the gateways use. A reservation's
//! active failover CUs (its dormant failover shares × the failed region's activation) are
//! spread over its `PoolAllocation`s in proportion to their `wuPerSec`:
//!
//! ```text
//! extra(alloc) = alloc.wuPerSec × active_failover_cus(reservation) ÷ cus(reservation)
//! ```
//!
//! So no WU-per-CU constant is needed. The follower long-polls with `If-None-Match`,
//! verifies the Ed25519 signature, accepts only newer versions for its region, and keeps
//! the last good snapshot through control-plane outages.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pt_crds::PoolAllocationSpec;
use pt_entitlement::{failover_cus, Snapshot, SnapshotVerifier, KEY_ID_HEADER, SIGNATURE_HEADER};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// Extra failover WU/s for each of `allocations`. Each allocation grows by the same
/// fraction as its reservation, so a reservation spread over several pools is split
/// between them in proportion.
pub fn failover_extra(
    snapshot: Option<&Snapshot>,
    allocations: &[PoolAllocationSpec],
    now_ms: u64,
) -> Vec<f64> {
    let Some(snap) = snapshot else {
        return vec![0.0; allocations.len()];
    };
    // Fraction of each reservation's CUs currently added by failover.
    let ratio: HashMap<&str, f64> = snap
        .reservations
        .iter()
        .filter(|r| r.cus > 0 && !r.failover.is_empty())
        .map(|r| {
            let active = failover_cus(&r.failover, &snap.failovers, now_ms);
            (r.id.as_str(), active / f64::from(r.cus))
        })
        .filter(|(_, x)| *x > 0.0)
        .collect();
    allocations
        .iter()
        .map(|a| {
            ratio
                .get(a.reservation.as_str())
                .map_or(0.0, |x| (a.wu_per_sec.max(0.0)) * x)
        })
        .collect()
}

/// The latest verified snapshot, or `None` before the first one.
pub type SnapshotRx = watch::Receiver<Option<Arc<Snapshot>>>;

/// Where to follow snapshots from. Read from the environment by `pt-operator`.
#[derive(Debug, Clone)]
pub struct SnapshotSource {
    /// Control-plane base URL, for example `http://control-plane:8090`.
    pub control_plane_url: String,
    pub region: String,
    pub token: String,
    /// Trusted Ed25519 public keys (hex), comma-separated during a signing-key rotation
    /// (ADR-020).
    pub public_key: String,
    /// Last-known-good cache, so a restart during a control-plane outage still knows about
    /// an active failover.
    pub cache_path: Option<PathBuf>,
    pub wait_secs: u64,
    /// TLS to the control plane (ADR-022): `PT_CONTROL_PLANE_CA`,
    /// `PT_CONTROL_PLANE_CLIENT_CERT`, `PT_CONTROL_PLANE_CLIENT_KEY`, and
    /// `PT_ALLOW_INSECURE_TRANSPORT=true`.
    pub tls: pt_entitlement::client_tls::ControlPlaneTls,
}

impl SnapshotSource {
    /// From `PT_CONTROL_PLANE_URL`, `PT_REGION`, `PT_REGION_TOKEN`, `PT_SNAPSHOT_PUBLIC_KEY`,
    /// and optional `PT_SNAPSHOT_CACHE`. `None` if any required one is unset.
    pub fn from_env() -> Option<Self> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        Some(Self {
            control_plane_url: var("PT_CONTROL_PLANE_URL")?,
            region: var("PT_REGION")?,
            token: var("PT_REGION_TOKEN")?,
            public_key: var("PT_SNAPSHOT_PUBLIC_KEY")?,
            cache_path: var("PT_SNAPSHOT_CACHE").map(PathBuf::from),
            wait_secs: 30,
            tls: pt_entitlement::client_tls::ControlPlaneTls {
                ca_cert: var("PT_CONTROL_PLANE_CA"),
                client_cert: var("PT_CONTROL_PLANE_CLIENT_CERT"),
                client_key: var("PT_CONTROL_PLANE_CLIENT_KEY"),
                allow_insecure_transport: var("PT_ALLOW_INSECURE_TRANSPORT")
                    .is_some_and(|v| v == "true"),
            },
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FollowError {
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("control plane returned {0}")]
    Status(reqwest::StatusCode),
    #[error("missing {SIGNATURE_HEADER} header")]
    MissingSignature,
    #[error("snapshot rejected: {0}")]
    Verify(#[from] pt_entitlement::Error),
    #[error("snapshot is for region {0}")]
    WrongRegion(String),
    #[error("transport: {0}")]
    Transport(String),
}

#[derive(Serialize, Deserialize)]
struct Cached {
    signature: String,
    body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_id: Option<String>,
}

/// Follows the region's snapshot and publishes each newer one.
pub struct SnapshotFollower {
    source: SnapshotSource,
    verifier: SnapshotVerifier,
    http: reqwest::Client,
    tx: watch::Sender<Option<Arc<Snapshot>>>,
}

impl SnapshotFollower {
    pub fn new(source: SnapshotSource) -> Result<(Self, SnapshotRx), FollowError> {
        let verifier = SnapshotVerifier::from_hex_list(
            source
                .public_key
                .split(',')
                .map(str::trim)
                .filter(|k| !k.is_empty()),
        )?;
        source
            .tls
            .check(&source.control_plane_url)
            .map_err(FollowError::Transport)?;
        let http = source
            .tls
            .client_builder()
            .map_err(FollowError::Transport)?
            .timeout(Duration::from_secs(source.wait_secs + 10))
            .build()?;
        let (tx, rx) = watch::channel(None);
        Ok((
            Self {
                source,
                verifier,
                http,
                tx,
            },
            rx,
        ))
    }

    fn version(&self) -> u64 {
        self.tx.borrow().as_ref().map_or(0, |s| s.version)
    }

    /// Verify, check the region and version, publish, and cache.
    fn accept(
        &self,
        body: &[u8],
        signature: &str,
        key_id: Option<&str>,
    ) -> Result<bool, FollowError> {
        let (snap, key_id) = self.verifier.verify_with(body, signature, key_id)?;
        if snap.region != self.source.region {
            return Err(FollowError::WrongRegion(snap.region));
        }
        if snap.version <= self.version() {
            return Ok(false);
        }
        tracing::info!(
            version = snap.version,
            failovers = snap.failovers.len(),
            %key_id,
            "entitlement snapshot"
        );
        self.tx.send_replace(Some(Arc::new(snap)));
        if let Some(path) = &self.source.cache_path {
            let cached = Cached {
                signature: signature.into(),
                body: String::from_utf8_lossy(body).into_owned(),
                key_id: Some(key_id),
            };
            let tmp = path.with_extension("tmp");
            let written = std::fs::write(&tmp, serde_json::to_vec(&cached).unwrap_or_default())
                .and_then(|_| std::fs::rename(&tmp, path));
            if let Err(e) = written {
                tracing::warn!(error = %e, "failed to write snapshot cache");
            }
        }
        Ok(true)
    }

    /// Load the cached snapshot, if there's a valid one.
    pub fn load_cache(&self) -> bool {
        let Some(path) = &self.source.cache_path else {
            return false;
        };
        let Ok(text) = std::fs::read_to_string(path) else {
            return false;
        };
        match serde_json::from_str::<Cached>(&text) {
            Ok(c) => self
                .accept(c.body.as_bytes(), &c.signature, c.key_id.as_deref())
                .unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Fetch once, waiting up to `wait_secs` for a change. Returns whether a newer snapshot
    /// was published.
    pub async fn poll_once(&self, wait: bool) -> Result<bool, FollowError> {
        let url = format!(
            "{}/internal/v1/entitlements/{}",
            self.source.control_plane_url.trim_end_matches('/'),
            self.source.region
        );
        let current = self.version();
        let mut req = self.http.get(url).bearer_auth(&self.source.token);
        if current > 0 {
            req = req.header(reqwest::header::IF_NONE_MATCH, format!("\"{current}\""));
            if wait {
                req = req.query(&[("wait", self.source.wait_secs)]);
            }
        }
        let resp = req.send().await?;
        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(false);
        }
        if !resp.status().is_success() {
            return Err(FollowError::Status(resp.status()));
        }
        let signature = resp
            .headers()
            .get(SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or(FollowError::MissingSignature)?
            .to_string();
        let key_id = resp
            .headers()
            .get(KEY_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body = resp.bytes().await?;
        self.accept(&body, &signature, key_id.as_deref())
    }

    /// Follow forever. Errors keep the last snapshot.
    pub async fn run(self) {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.poll_once(true).await {
                Ok(_) => backoff = Duration::from_secs(1),
                Err(e) => {
                    tracing::warn!(error = %e, retry_in = ?backoff, "snapshot follow failed; keeping the last snapshot");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pt_core::{Shape, Tier};
    use pt_entitlement::{FailoverShare, RegionFailover, ReservationEntitlement};

    fn res(id: &str, cus: u32, failover_cus: u32) -> ReservationEntitlement {
        ReservationEntitlement {
            id: id.into(),
            tenant: "t".into(),
            model: "m".into(),
            cus,
            tier: Tier::Agentic,
            profile: "p".into(),
            shape: Shape {
                input_p95: 1,
                input_max: 1,
                output_p95: 1,
                context_ceiling: 1,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
            failover: if failover_cus > 0 {
                vec![FailoverShare {
                    from_region: "eu-west".into(),
                    cus: failover_cus,
                }]
            } else {
                vec![]
            },
        }
    }

    fn alloc(reservation: &str, pool: &str, wu: f64) -> PoolAllocationSpec {
        PoolAllocationSpec {
            reservation: reservation.into(),
            tenant: "t".into(),
            pool: pool.into(),
            wu_per_sec: wu,
            tier: Tier::Agentic,
            kv_share: 0.0,
            burst_factor: 1.0,
            dedicated_workers: vec![],
        }
    }

    #[test]
    fn plain_http_to_a_remote_control_plane_is_refused() {
        let source = |url: &str, allow| SnapshotSource {
            control_plane_url: url.into(),
            region: "eu-west".into(),
            token: "t".into(),
            public_key: "b51da0aa7183df2b9a9ac31f26e8f856fd8b818d1118cdb87c9253afe13bb164".into(),
            cache_path: None,
            wait_secs: 1,
            tls: pt_entitlement::client_tls::ControlPlaneTls {
                allow_insecure_transport: allow,
                ..Default::default()
            },
        };
        assert!(matches!(
            SnapshotFollower::new(source("http://cp.pt.example.com", false)),
            Err(FollowError::Transport(_))
        ));
        assert!(SnapshotFollower::new(source("http://cp.pt.example.com", true)).is_ok());
        assert!(SnapshotFollower::new(source("https://cp.pt.example.com", false)).is_ok());
        assert!(SnapshotFollower::new(source("http://127.0.0.1:8090", false)).is_ok());
    }

    #[test]
    fn failover_demand_is_spread_over_allocations() {
        let mut snap = Snapshot {
            region: "eu-central".into(),
            version: 1,
            generated_at: String::new(),
            reservations: vec![res("multi", 2, 6), res("regional", 4, 0)],
            deployments: vec![],
            failovers: vec![],
        };
        let all = [
            alloc("multi", "a", 1_500.0),
            alloc("multi", "b", 500.0),
            alloc("regional", "a", 4_000.0),
        ];
        let pool_a: Vec<_> = all.iter().filter(|a| a.pool == "a").cloned().collect();
        // Dormant: nothing extra.
        assert_eq!(failover_extra(Some(&snap), &pool_a, 5_000), [0.0, 0.0]);
        assert_eq!(failover_extra(None, &pool_a, 5_000), [0.0, 0.0]);

        // eu-west fails: "multi" triples (2 own + 6 failover CUs), split by allocation.
        snap.failovers.push(RegionFailover {
            region: "eu-west".into(),
            incident: "inc".into(),
            started_at_ms: 1_000,
            ended_at_ms: None,
            return_ramp_ms: 10_000,
        });
        assert_eq!(failover_extra(Some(&snap), &pool_a, 5_000), [4_500.0, 0.0]);
        let pool_b: Vec<_> = all.iter().filter(|a| a.pool == "b").cloned().collect();
        assert_eq!(failover_extra(Some(&snap), &pool_b, 5_000), [1_500.0]);

        // Half way down the return ramp.
        snap.failovers[0].ended_at_ms = Some(5_000);
        assert_eq!(failover_extra(Some(&snap), &pool_a, 10_000), [2_250.0, 0.0]);
        assert_eq!(failover_extra(Some(&snap), &pool_a, 15_000), [0.0, 0.0]);
    }
}
