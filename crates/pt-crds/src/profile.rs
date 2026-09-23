//! `PerformanceProfile`: calibrated cost coefficients and per-replica capacity for one
//! (model, GPU class, engine version, parallelism). Produced by the Calibration Service
//! (docs/02 §6). A pool isn't sellable without one.

use std::collections::BTreeMap;

use kube::CustomResource;
use pt_core::cost::TierCapacity;
use pt_core::Coefficients;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pt.example.com",
    version = "v1",
    kind = "PerformanceProfile",
    plural = "performanceprofiles",
    shortname = "ptprofile",
    status = "PerformanceProfileStatus",
    printcolumn = r#"{"name":"Model","type":"string","jsonPath":".spec.model"}"#,
    printcolumn = r#"{"name":"GPU","type":"string","jsonPath":".spec.gpuClass"}"#,
    printcolumn = r#"{"name":"Engine","type":"string","jsonPath":".spec.engine.backend"}"#,
    printcolumn = r#"{"name":"Drift%","type":"number","jsonPath":".status.driftPct"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PerformanceProfileSpec {
    pub model: String,
    pub gpu_class: String,
    pub engine: EngineVersion,
    pub parallelism: Parallelism,
    /// WU = a·uncached_prefill + b·cached_prefill + c·decode·m + d·KV_token_seconds.
    pub coefficients: Coefficients,
    /// Decode-cost multipliers, for example `specDecode: 0.62`, `jsonSchema: 1.07`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub decode_modifiers: BTreeMap<String, f64>,
    /// WU/s one aggregated replica sustains at each tier's SLO.
    pub capacity: TierCapacity,
    /// WU/s per replica for disaggregated prefill and decode workers. Falls back to
    /// `capacity` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_capacity: Option<RoleCapacity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EngineVersion {
    /// `trtllm`, `vllm`, or `sglang`.
    pub backend: Backend,
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Trtllm,
    Vllm,
    Sglang,
}

impl Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            Backend::Trtllm => "trtllm",
            Backend::Vllm => "vllm",
            Backend::Sglang => "sglang",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Parallelism {
    #[serde(default = "one")]
    pub tp: u32,
    #[serde(default = "one")]
    pub pp: u32,
    #[serde(default = "one")]
    pub ep: u32,
}

impl Parallelism {
    /// GPUs per replica. Expert parallelism reuses the tensor-parallel GPUs.
    pub fn gpus(&self) -> u32 {
        self.tp * self.pp
    }
}

fn one() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RoleCapacity {
    pub prefill: TierCapacity,
    pub decode: TierCapacity,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PerformanceProfileStatus {
    /// RFC 3339 time of the last calibration validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validated_at: Option<String>,
    /// Production actual-WU vs GPU-seconds drift. New sales stop above 5%.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drift_pct: Option<f64>,
}
