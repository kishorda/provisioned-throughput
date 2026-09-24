//! `ModelPool`: a set of Dynamo serving replicas for one model on one GPU class in one
//! cluster (docs/08 §2). The capacity controller sizes it from its `PoolAllocation`s and
//! renders a `DynamoGraphDeployment`.

use kube::CustomResource;
use pt_core::PoolIsolation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::condition::Condition;
use crate::profile::Backend;

#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pt.example.com",
    version = "v1",
    kind = "ModelPool",
    plural = "modelpools",
    shortname = "ptpool",
    namespaced,
    status = "ModelPoolStatus",
    printcolumn = r#"{"name":"Model","type":"string","jsonPath":".spec.model"}"#,
    printcolumn = r#"{"name":"Isolation","type":"string","jsonPath":".spec.isolation"}"#,
    printcolumn = r#"{"name":"Allocated WU/s","type":"number","jsonPath":".status.allocatedWuPerSec"}"#,
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".status.desiredReplicas.total"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type==\"Ready\")].status"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ModelPoolSpec {
    pub model: String,
    /// Name of the cluster-scoped `PerformanceProfile` that calibrates this pool.
    pub profile_ref: String,
    pub engine: EngineSpec,
    #[serde(default)]
    pub isolation: PoolIsolation,
    #[serde(default)]
    pub disaggregation: Disaggregation,
    #[serde(default)]
    pub headroom: Headroom,
    /// Planning target for utilisation of the provisioned floor (docs/06 §2).
    #[serde(default = "default_target_utilization")]
    pub target_utilization: f64,
    /// z-score for the correlated-burst allowance. 2.33 covers 99% of bursts.
    #[serde(default = "default_burst_z")]
    pub burst_z: f64,
    #[serde(default)]
    pub payg: Payg,
    /// Hard cap on total replicas (GPU budget). Exceeding it raises `CapacityShortfall`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_replicas: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EngineSpec {
    pub backend: Backend,
    /// Must match the profile's engine version, or the pool isn't reconciled.
    pub version: String,
    /// Worker image, for example `nvcr.io/nvidia/ai-dynamo/tensorrtllm-runtime:<tag>`.
    pub image: String,
    /// Extra worker arguments, appended after the ones the controller sets.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Disaggregation {
    #[serde(default)]
    pub enabled: bool,
    /// Let Dynamo prefill short prompts locally on decode workers.
    #[serde(default = "default_true")]
    pub conditional: bool,
    /// Fraction of allocated WU that is prefill work, for sizing the two roles.
    #[serde(default = "default_prefill_share")]
    pub prefill_share: f64,
    #[serde(default)]
    pub prefill_replicas_min: u32,
    #[serde(default)]
    pub decode_replicas_min: u32,
}

impl Default for Disaggregation {
    fn default() -> Self {
        Self {
            enabled: false,
            conditional: true,
            prefill_share: default_prefill_share(),
            prefill_replicas_min: 0,
            decode_replicas_min: 0,
        }
    }
}

/// Headroom per role, on top of the provisioned floor (docs/06 §2–3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Headroom {
    /// Replicas lost to the largest single failure domain the pool spans. Kept hot.
    #[serde(default = "default_failure_k")]
    pub failure_domain_k: u32,
    /// Replicas that may be drained for maintenance at once.
    #[serde(default = "default_one")]
    pub maintenance_slots: u32,
    /// Loaded replicas serving PAYG until provisioned traffic needs them.
    #[serde(default)]
    pub hot_spares: u32,
    /// Replicas with weights staged on node-local NVMe but no GPU claim. During a region
    /// failover, the controller loads as many as the failover demand needs beyond the hot
    /// spares (docs/07 §4).
    #[serde(default)]
    pub warm_spares: u32,
}

impl Default for Headroom {
    fn default() -> Self {
        Self {
            failure_domain_k: default_failure_k(),
            maintenance_slots: 1,
            hot_spares: 0,
            warm_spares: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Payg {
    /// Serve preemptible PAYG on idle capacity. Must be false for strict-dedicated pools.
    #[serde(default = "default_true")]
    pub backfill: bool,
    /// Ceiling for the Dynamo Planner when it scales above the floor for PAYG demand.
    #[serde(default)]
    pub max_replicas: u32,
}

impl Default for Payg {
    fn default() -> Self {
        Self {
            backfill: true,
            max_replicas: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RoleReplicas {
    /// Aggregated workers. Zero for disaggregated pools.
    #[serde(default)]
    pub aggregated: u32,
    #[serde(default)]
    pub prefill: u32,
    #[serde(default)]
    pub decode: u32,
    #[serde(default)]
    pub total: u32,
}

impl RoleReplicas {
    pub fn aggregated(n: u32) -> Self {
        Self {
            aggregated: n,
            prefill: 0,
            decode: 0,
            total: n,
        }
    }

    pub fn disaggregated(prefill: u32, decode: u32) -> Self {
        Self {
            aggregated: 0,
            prefill,
            decode,
            total: prefill + decode,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelPoolStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Replicas needed to serve all allocations at target utilisation.
    #[serde(default)]
    pub provisioned_floor: RoleReplicas,
    /// Floor plus failure-domain headroom, maintenance slots, and hot spares.
    #[serde(default)]
    pub desired_replicas: RoleReplicas,
    /// Minimum replicas that must stay available during voluntary disruption.
    #[serde(default)]
    pub min_available: RoleReplicas,
    #[serde(default)]
    pub allocated_wu_per_sec: f64,
    /// Number of `PoolAllocation`s on this pool.
    #[serde(default)]
    pub allocations: u32,
    /// Extra demand from active failover entitlements (docs/07 §4).
    #[serde(default)]
    pub failover_wu_per_sec: f64,
    /// Warm spares loaded (serving) for the failover, per role. Included in
    /// `desiredReplicas`. Held until the failover ends.
    #[serde(default)]
    pub warm_spares_loaded: RoleReplicas,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

fn default_target_utilization() -> f64 {
    0.85
}
fn default_burst_z() -> f64 {
    2.33
}
fn default_prefill_share() -> f64 {
    0.3
}
fn default_failure_k() -> u32 {
    1
}
fn default_one() -> u32 {
    1
}
fn default_true() -> bool {
    true
}
