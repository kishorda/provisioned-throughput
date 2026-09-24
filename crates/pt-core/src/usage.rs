//! Per-request usage record (docs/09 §1).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Strict priority order: `Provisioned > Burst > Spillover > Payg` (docs/05 §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrafficClass {
    Provisioned,
    Burst,
    Spillover,
    Payg,
}

impl TrafficClass {
    pub fn as_str(self) -> &'static str {
        match self {
            TrafficClass::Provisioned => "provisioned",
            TrafficClass::Burst => "burst",
            TrafficClass::Spillover => "spillover",
            TrafficClass::Payg => "payg",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    EntitlementExhausted,
    QueueDeadline,
    QueueFull,
    /// The deployment reached its `max_share` of the reservation's entitlement.
    DeploymentCapExhausted,
}

impl RejectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            RejectReason::EntitlementExhausted => "entitlement_exhausted",
            RejectReason::QueueDeadline => "queue_deadline",
            RejectReason::QueueFull => "queue_full",
            RejectReason::DeploymentCapExhausted => "deployment_cap_exhausted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub enum Outcome {
    Ok,
    Rejected(RejectReason),
    ClientCancelled,
    Error(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct TokenBreakdown {
    pub uncached_prefill: u64,
    pub cached_prefill: u64,
    pub decode: u64,
}

/// Gateway-measured timings in milliseconds since the request was fully received.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Timings {
    pub queue_ms: f64,
    pub ttft_ms: Option<f64>,
    pub total_ms: f64,
    /// (last byte − first byte) / (output tokens − 1).
    pub tpot_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageRecord {
    /// Idempotency key for exactly-once billing.
    pub request_id: Uuid,
    /// When the gateway fully received the request, in Unix milliseconds.
    #[serde(default)]
    pub received_at_ms: u64,
    pub tenant: String,
    pub reservation: String,
    pub deployment: String,
    pub class: Option<TrafficClass>,
    pub session_id: Option<String>,
    pub tokens: TokenBreakdown,
    pub kv_token_seconds: f64,
    pub wu_estimated: f64,
    pub wu_actual: f64,
    pub timings: Timings,
    pub in_shape: bool,
    pub outcome: Outcome,
    pub profile: String,
}
