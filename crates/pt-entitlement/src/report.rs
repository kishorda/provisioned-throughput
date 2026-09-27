//! Status that regional components report to the control plane for the dashboards
//! (ADR-043). Shared by the capacity controller, the router, and the control plane.
//!
//! Reports are soft state: each replaces the reporter's previous one, and the control plane
//! shows how old it is. Nothing on the request path depends on them.

use serde::{Deserialize, Serialize};

/// Replicas per worker role.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Roles {
    #[serde(default)]
    pub aggregated: u32,
    #[serde(default)]
    pub prefill: u32,
    #[serde(default)]
    pub decode: u32,
    #[serde(default)]
    pub total: u32,
}

/// One status condition of a pool (`Ready`, `CapacityShortfall`, `FailoverActive`,
/// `Draining`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportCondition {
    #[serde(rename = "type")]
    pub type_: String,
    /// `True`, `False`, or `Unknown`.
    pub status: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub message: String,
}

/// A `ModelPool`'s state, from the region's capacity controller after each reconcile
/// (`POST /internal/v1/reports/pools`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolReport {
    pub namespace: String,
    pub name: String,
    /// The served model (the pool's `spec.model`).
    pub model: String,
    /// The control plane's id for it, if the pool names one.
    #[serde(default)]
    pub catalog_model: Option<String>,
    pub profile: String,
    /// Backend and version, for example `trtllm 1.2`.
    pub engine: String,
    pub desired: Roles,
    /// Worker pods that are Ready.
    pub ready: Roles,
    pub floor: Roles,
    pub min_available: Roles,
    pub hot_spares: u32,
    pub warm_spares_loaded: Roles,
    pub drain_surge: Roles,
    #[serde(default)]
    pub draining_nodes: Vec<String>,
    pub allocations: u32,
    pub allocated_wu_per_sec: f64,
    pub failover_wu_per_sec: f64,
    #[serde(default)]
    pub conditions: Vec<ReportCondition>,
    /// Ready pods labelled as hot spares (ADR-044).
    #[serde(default)]
    pub spare_pods: Vec<String>,
}

impl PoolReport {
    /// The condition of `type_`, if reported.
    pub fn condition(&self, type_: &str) -> Option<&ReportCondition> {
        self.conditions.iter().find(|c| c.type_ == type_)
    }

    /// Whether condition `type_` is `True`.
    pub fn is(&self, type_: &str) -> bool {
        self.condition(type_).is_some_and(|c| c.status == "True")
    }

    /// Where the report is kept: one per pool.
    pub fn id(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// A router's state, sent periodically (`POST /internal/v1/reports/routers`). `status` is
/// what the router serves at `/v1/router/status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouterReport {
    pub router_id: String,
    pub status: serde_json::Value,
}
