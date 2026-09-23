//! The Provisioned Throughput resource and its API request types (docs/12).

use jiff::Timestamp;
use pt_admission::BoundaryPolicy;
use pt_core::{PoolIsolation, Shape, TermMonths, Tier};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionShare {
    pub region: String,
    pub cus: u32,
}

pub fn total_cus(regions: &[RegionShare]) -> u32 {
    regions.iter().map(|r| r.cus).sum()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sku {
    /// Best-effort failover to other regions.
    #[default]
    Regional,
    /// Pre-reserved failover headroom in a paired region (docs/07 §2). Needs 2+ regions.
    MultiRegion,
}

/// ```text
/// Scheduled ──start──▶ Active ──term end, auto_renew──▶ Active (renewed)
///     │                  │  └──term end, no renew──▶ Ended
///  DELETE             DELETE
///     ▼                  ▼
/// Cancelled     PendingCancellation ──term end──▶ Ended
///                        └──PATCH auto_renew=true──▶ Active
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// The term starts in the future. Changes apply immediately and DELETE cancels.
    Scheduled,
    Active,
    /// Deleted mid-term. Serving and billing continue until `term_end`.
    PendingCancellation,
    /// The term ended without renewal. Capacity is released.
    Ended,
    /// Deleted before the term started. Capacity is released.
    Cancelled,
}

impl State {
    pub fn is_live(self) -> bool {
        matches!(
            self,
            State::Scheduled | State::Active | State::PendingCancellation
        )
    }
}

/// Changes that take effect at the next renewal: CU decreases, region changes, and tier
/// changes (docs/11 §4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingChanges {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<Tier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regions: Option<Vec<RegionShare>>,
    pub effective_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Price {
    pub currency: String,
    pub per_cu_monthly: u64,
    pub monthly: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    pub region: String,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub at: Timestamp,
    #[serde(flatten)]
    pub kind: EventKind,
}

/// Audit and billing trail. Amounts are in minor currency units.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    Created {
        cus: u32,
        monthly: u64,
    },
    Activated,
    CapacityIncreased {
        region: String,
        from: u32,
        to: u32,
        prorated_charge: u64,
    },
    ChangeScheduled {
        tier: Option<Tier>,
        regions: Option<Vec<RegionShare>>,
    },
    ChangeApplied {
        tier: Tier,
        regions: Vec<RegionShare>,
    },
    ScheduledChangeFailed {
        reason: String,
    },
    ShapeUpdated,
    BoundaryPolicyUpdated,
    Renamed {
        name: String,
    },
    AutoRenewChanged {
        auto_renew: bool,
    },
    CancellationRequested {
        effective_at: Timestamp,
    },
    CancellationWithdrawn,
    Renewed {
        term_start: Timestamp,
        term_end: Timestamp,
        monthly: u64,
    },
    Ended,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvisionedThroughput {
    pub id: String,
    pub tenant: String,
    pub name: String,
    pub model: String,
    pub tier: Tier,
    pub regions: Vec<RegionShare>,
    pub cus: u32,
    pub sku: Sku,
    pub isolation: PoolIsolation,
    pub shape: Shape,
    pub boundary_policy: BoundaryPolicy,
    pub term_months: TermMonths,
    pub term_start: Timestamp,
    pub term_end: Timestamp,
    pub auto_renew: bool,
    pub state: State,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_changes: Option<PendingChanges>,
    /// Data-plane deployment id; the gateway's `x-pt-deployment`.
    pub deployment_id: String,
    pub endpoints: Vec<Endpoint>,
    pub price: Price,
    /// SHA-256 of the inference API key. The key itself is only returned at creation.
    #[serde(skip_serializing, default)]
    pub api_key_sha256: String,
    pub version: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub events: Vec<Event>,
}

/// `POST /v1/provisioned-throughput`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRequest {
    pub name: String,
    pub model: String,
    pub tier: Tier,
    pub regions: Vec<RegionShare>,
    #[serde(default)]
    pub sku: Sku,
    #[serde(default)]
    pub isolation: PoolIsolation,
    pub term_months: TermMonths,
    /// Defaults to now. At most 90 days ahead.
    #[serde(default)]
    pub start_at: Option<Timestamp>,
    #[serde(default = "default_true")]
    pub auto_renew: bool,
    pub shape: Shape,
    #[serde(default)]
    pub boundary_policy: BoundaryPolicy,
}

/// `PATCH /v1/provisioned-throughput/{id}`. Omitted fields are unchanged.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateRequest {
    #[serde(default)]
    pub name: Option<String>,
    /// Scheduled for the next term, unless the term hasn't started.
    #[serde(default)]
    pub tier: Option<Tier>,
    /// Increases in existing regions apply now. Anything else is scheduled for the next term.
    #[serde(default)]
    pub regions: Option<Vec<RegionShare>>,
    #[serde(default)]
    pub shape: Option<Shape>,
    #[serde(default)]
    pub boundary_policy: Option<BoundaryPolicy>,
    #[serde(default)]
    pub auto_renew: Option<bool>,
}

fn default_true() -> bool {
    true
}
