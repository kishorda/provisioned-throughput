//! Router configuration. Workers and allocations are static here; in production they come
//! from Dynamo's discovery (etcd) and the `PoolAllocation` CRDs (docs/08 §2).

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Deserialize;

use crate::workers::{Allocation, Weights, Worker};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterConfig {
    pub listen: String,
    /// Tokens per KV block, for estimating a request's KV footprint.
    #[serde(default = "default_block_size")]
    pub block_size: u64,
    /// Output length assumed when a request doesn't set `max_tokens`.
    #[serde(default = "default_max_tokens")]
    pub default_max_tokens: u64,
    /// Longest a request waits in the router queue before 503.
    #[serde(default = "default_queue_timeout_ms")]
    pub queue_timeout_ms: u64,
    /// PAYG goes first after this many dispatches without one, if any is queued.
    #[serde(default = "default_payg_guard_every")]
    pub payg_guard_every: u64,
    /// A request marked `x-pt-failover` keeps the failover fence up for this long.
    #[serde(default = "default_failover_hold_ms")]
    pub failover_hold_ms: u64,
    /// During a failover, provisioned work that has waited this long with every eligible
    /// worker busy preempts running PAYG.
    #[serde(default = "default_preempt_grace_ms")]
    pub preempt_grace_ms: u64,
    /// Share of each floor worker's slots and KV that PAYG and spillover may hold (docs/05
    /// §6). Hot spares aren't capped. Use 1.0 for a PAYG-only pool.
    #[serde(default = "default_backfill_ratio")]
    pub backfill_ratio: f64,
    /// Size the floor's room for provisioned work from expected load instead (ADR-039).
    /// `backfill_ratio` applies until anything is learned.
    #[serde(default)]
    pub adaptive_backfill: Option<AdaptiveBackfillConfig>,
    #[serde(default)]
    pub weights: Option<WeightsConfig>,
    pub workers: Vec<WorkerConfig>,
    #[serde(default)]
    pub allocations: Vec<AllocationConfig>,
    /// Send `/v1/router/status` to the control plane for the system dashboard (ADR-043).
    #[serde(default)]
    pub report: Option<ReportConfig>,
}

/// `[report]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportConfig {
    pub control_plane_url: String,
    /// The region's token, as the gateways use.
    pub token: String,
    /// Names this router on the dashboard, for example its pod name. `${VAR}` is read
    /// from the environment.
    pub router_id: String,
    #[serde(default = "default_report_interval_secs")]
    pub interval_secs: u64,
    #[serde(default)]
    pub tls: pt_entitlement::client_tls::ControlPlaneTls,
}

fn default_report_interval_secs() -> u64 {
    15
}

/// `[adaptive_backfill]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveBackfillConfig {
    #[serde(default = "default_headroom")]
    pub headroom: f64,
    #[serde(default = "default_min_ratio")]
    pub min_ratio: f64,
    #[serde(default = "default_max_ratio")]
    pub max_ratio: f64,
    #[serde(default = "default_half_life_secs")]
    pub half_life_secs: u64,
}

fn default_headroom() -> f64 {
    crate::dispatch::AdaptiveBackfill::default().headroom
}
fn default_min_ratio() -> f64 {
    crate::dispatch::AdaptiveBackfill::default().min_ratio
}
fn default_max_ratio() -> f64 {
    crate::dispatch::AdaptiveBackfill::default().max_ratio
}
fn default_half_life_secs() -> u64 {
    crate::dispatch::AdaptiveBackfill::default()
        .half_life
        .as_secs()
}

impl AdaptiveBackfillConfig {
    pub fn policy(&self) -> crate::dispatch::AdaptiveBackfill {
        crate::dispatch::AdaptiveBackfill {
            headroom: self.headroom,
            min_ratio: self.min_ratio,
            max_ratio: self.max_ratio,
            half_life: std::time::Duration::from_secs(self.half_life_secs.max(1)),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeightsConfig {
    pub overlap: f64,
    pub load: f64,
    pub session: f64,
    pub kv_overcommit: f64,
    #[serde(default = "default_spare_weight")]
    pub spare: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub id: String,
    pub url: String,
    /// Concurrent requests the worker takes (its batch slots).
    pub slots: u32,
    /// KV cache capacity in blocks.
    pub kv_blocks: u32,
    /// Hot spare (docs/06 §3): serves PAYG until provisioned traffic needs it. In
    /// production these are the pool's `headroom.hotSpares` replicas.
    #[serde(default)]
    pub hot_spare: bool,
}

/// Mirrors a `PoolAllocation` (docs/08 §2).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationConfig {
    pub reservation: String,
    pub wu_per_sec: f64,
    #[serde(default)]
    pub kv_share: Option<f64>,
    #[serde(default)]
    pub dedicated_workers: Vec<String>,
}

fn default_block_size() -> u64 {
    16
}
fn default_max_tokens() -> u64 {
    256
}
fn default_queue_timeout_ms() -> u64 {
    30_000
}
fn default_payg_guard_every() -> u64 {
    50
}
fn default_failover_hold_ms() -> u64 {
    30_000
}
fn default_preempt_grace_ms() -> u64 {
    250
}
fn default_backfill_ratio() -> f64 {
    crate::workers::DEFAULT_BACKFILL_RATIO
}
fn default_spare_weight() -> f64 {
    0.25
}

impl RouterConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let c: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        c.validate()?;
        Ok(c)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.workers.is_empty(), "configure at least one worker");
        if let Some(r) = &self.report {
            anyhow::ensure!(r.interval_secs > 0, "report.interval_secs must be positive");
            r.tls
                .check(&r.control_plane_url)
                .map_err(|e| anyhow::anyhow!("report: {e}"))?;
        }
        anyhow::ensure!(self.block_size > 0, "block_size must be positive");
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.backfill_ratio),
            "backfill_ratio must be in [0, 1]"
        );
        if let Some(a) = &self.adaptive_backfill {
            anyhow::ensure!(
                0.0 <= a.min_ratio && a.min_ratio <= a.max_ratio && a.max_ratio <= 1.0,
                "adaptive_backfill needs 0 <= min_ratio <= max_ratio <= 1"
            );
            anyhow::ensure!(
                a.headroom >= 1.0,
                "adaptive_backfill.headroom must be at least 1"
            );
        }
        let mut ids = HashSet::new();
        for w in &self.workers {
            anyhow::ensure!(ids.insert(w.id.as_str()), "duplicate worker {}", w.id);
            anyhow::ensure!(
                w.slots > 0 && w.kv_blocks > 0,
                "worker {} needs slots and kv_blocks",
                w.id
            );
        }
        let mut owners = HashMap::new();
        for a in &self.allocations {
            anyhow::ensure!(
                a.wu_per_sec > 0.0,
                "allocation {} needs a positive wu_per_sec",
                a.reservation
            );
            if let Some(s) = a.kv_share {
                anyhow::ensure!(
                    s > 0.0 && s <= 1.0,
                    "allocation {} kv_share must be in (0, 1]",
                    a.reservation
                );
            }
            for w in &a.dedicated_workers {
                anyhow::ensure!(
                    ids.contains(w.as_str()),
                    "allocation {} names unknown worker {w}",
                    a.reservation
                );
                if let Some(other) = owners.insert(w.clone(), a.reservation.clone()) {
                    anyhow::bail!(
                        "worker {w} is dedicated to both {other} and {}",
                        a.reservation
                    );
                }
            }
        }
        Ok(())
    }

    pub fn workers(&self) -> Vec<Worker> {
        self.workers
            .iter()
            .map(|w| {
                Worker::new(&w.id, w.url.trim_end_matches('/'), w.slots, w.kv_blocks)
                    .with_hot_spare(w.hot_spare)
            })
            .collect()
    }

    pub fn allocations(&self) -> HashMap<String, Allocation> {
        self.allocations
            .iter()
            .map(|a| {
                (
                    a.reservation.clone(),
                    Allocation {
                        wu_per_sec: a.wu_per_sec,
                        kv_share: a.kv_share,
                        dedicated_workers: a.dedicated_workers.clone(),
                    },
                )
            })
            .collect()
    }

    pub fn weights(&self) -> Weights {
        let base = match &self.weights {
            Some(w) => Weights {
                overlap: w.overlap,
                load: w.load,
                session: w.session,
                kv_overcommit: w.kv_overcommit,
                spare: w.spare,
                ..Weights::default()
            },
            None => Weights::default(),
        };
        Weights {
            backfill_ratio: self.backfill_ratio,
            ..base
        }
    }
}
