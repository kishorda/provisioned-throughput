//! `PoolAllocation`: one reservation's share of one pool, written by the global Capacity
//! Planner (docs/06 §2, docs/08 §2). The router reads the same objects for WFQ weights and
//! KV budgets.

use kube::CustomResource;
use pt_core::Tier;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pt.example.com",
    version = "v1",
    kind = "PoolAllocation",
    plural = "poolallocations",
    shortname = "ptalloc",
    namespaced,
    printcolumn = r#"{"name":"Pool","type":"string","jsonPath":".spec.pool"}"#,
    printcolumn = r#"{"name":"Tenant","type":"string","jsonPath":".spec.tenant"}"#,
    printcolumn = r#"{"name":"Tier","type":"string","jsonPath":".spec.tier"}"#,
    printcolumn = r#"{"name":"WU/s","type":"number","jsonPath":".spec.wuPerSec"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PoolAllocationSpec {
    pub reservation: String,
    pub tenant: String,
    /// Name of a `ModelPool` in the same namespace.
    pub pool: String,
    /// This reservation's WU/s entitlement on this pool.
    pub wu_per_sec: f64,
    pub tier: Tier,
    /// Share of the pool's KV blocks this tenant may hold before preemption (docs/05 §4).
    #[serde(default)]
    pub kv_share: f64,
    /// Declared peak-to-mean ratio from the reservation's shape. Sizes the burst allowance.
    #[serde(default = "default_burst_factor")]
    pub burst_factor: f64,
    /// Workers reserved for this tenant on dedicated pools.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dedicated_workers: Vec<String>,
}

fn default_burst_factor() -> f64 {
    1.0
}
