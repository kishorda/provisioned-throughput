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
    #[serde(default)]
    pub weights: Option<WeightsConfig>,
    pub workers: Vec<WorkerConfig>,
    #[serde(default)]
    pub allocations: Vec<AllocationConfig>,
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
        anyhow::ensure!(self.block_size > 0, "block_size must be positive");
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
        match &self.weights {
            Some(w) => Weights {
                overlap: w.overlap,
                load: w.load,
                session: w.session,
                kv_overcommit: w.kv_overcommit,
                spare: w.spare,
            },
            None => Weights::default(),
        }
    }
}
