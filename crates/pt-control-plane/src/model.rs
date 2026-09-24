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
    KeyRotated {
        new_key: String,
        previous_key: String,
        previous_expires_at: Option<Timestamp>,
    },
    KeyRevoked {
        key: String,
    },
    KeyExpired {
        key: String,
    },
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
    /// Inference API keys: exactly one current key (no expiry), plus up to two rotated-out
    /// keys in their grace period. Secrets are only returned when a key is issued.
    pub api_keys: Vec<ApiKey>,
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

/// An operator-declared region incident (docs/07 §4, docs/09 §4).
///
/// Multi-region reservations with a share in the region have requests excluded from the
/// SLA for the failover window after `started_at`. Regional reservations have requests
/// served in the region excluded until `ended_at`, because they have no SLO during a
/// region failure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegionIncident {
    pub id: String,
    pub region: String,
    pub started_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<Timestamp>,
    pub description: String,
    pub declared_at: Timestamp,
}

/// `POST /internal/v1/incidents`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclareIncident {
    pub region: String,
    /// Defaults to now. At most 24 hours in the past.
    #[serde(default)]
    pub started_at: Option<Timestamp>,
    pub description: String,
}

/// `POST /internal/v1/incidents/{id}/resolve`
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveIncident {
    /// Defaults to now.
    #[serde(default)]
    pub ended_at: Option<Timestamp>,
}

/// Metadata for one inference API key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    /// The key's first characters, to recognise it by (for example `ptk_3f9a2c1b`).
    pub prefix: String,
    /// Never returned by the API.
    #[serde(skip_serializing, default)]
    pub sha256: String,
    pub created_at: Timestamp,
    /// `None` for the current key. Set when a rotation starts the key's grace period.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
}

impl ApiKey {
    pub fn is_current(&self) -> bool {
        self.expires_at.is_none()
    }
}

/// `POST /v1/provisioned-throughput/{id}/keys/rotate`
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RotateKeyRequest {
    /// How long the current key keeps working. Default 60, at most 10,080 (7 days).
    /// 0 revokes it immediately.
    #[serde(default)]
    pub grace_minutes: Option<u64>,
}
