//! Boundary policy configuration (docs/04 §4).
//!
//! When the provisioned bucket can't admit a request, the enabled policies are tried in
//! order: burst → queue → spillover → reject.

use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundaryPolicy {
    #[serde(default)]
    pub burst: Option<BurstPolicy>,
    #[serde(default)]
    pub queue: Option<QueuePolicy>,
    /// Send over-entitlement traffic to the PAYG pool, billed at PAYG list price.
    #[serde(default)]
    pub spillover: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BurstPolicy {
    /// Unused entitlement banked as burst credit, in seconds of entitlement.
    #[serde(default = "default_max_credit_seconds")]
    pub max_credit_seconds: f64,
    /// Instantaneous ceiling as a multiple of the entitlement rate.
    #[serde(default = "default_max_rate_multiple")]
    pub max_rate_multiple: f64,
    /// Fraction of the credit cap reserved for `continuation` calls, so a new agent session
    /// can't use up the credit a half-finished one needs.
    #[serde(default = "default_continuation_reserve")]
    pub continuation_reserve: f64,
}

impl Default for BurstPolicy {
    fn default() -> Self {
        Self {
            max_credit_seconds: default_max_credit_seconds(),
            max_rate_multiple: default_max_rate_multiple(),
            continuation_reserve: default_continuation_reserve(),
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuePolicy {
    /// Longest a request may wait at the gateway, measured from receipt.
    #[serde(default = "default_deadline_ms")]
    pub deadline_ms: u64,
    /// Queue depth limit, in seconds of entitlement.
    #[serde(default = "default_max_depth_wu_seconds")]
    pub max_depth_wu_seconds: f64,
}

impl Default for QueuePolicy {
    fn default() -> Self {
        Self {
            deadline_ms: default_deadline_ms(),
            max_depth_wu_seconds: default_max_depth_wu_seconds(),
        }
    }
}

fn default_max_credit_seconds() -> f64 {
    60.0
}
fn default_max_rate_multiple() -> f64 {
    2.0
}
fn default_continuation_reserve() -> f64 {
    0.25
}
fn default_deadline_ms() -> u64 {
    2_000
}
fn default_max_depth_wu_seconds() -> f64 {
    5.0
}
