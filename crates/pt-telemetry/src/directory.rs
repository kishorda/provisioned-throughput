//! What telemetry needs to know about tenants, regions, and reservations. The control plane
//! implements this, which keeps this crate independent of its storage.

use std::future::Future;

use pt_core::{Shape, Tier};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq)]
pub struct ReservationInfo {
    pub id: String,
    pub tenant: String,
    pub tier: Tier,
    /// Total CUs across regions.
    pub cus: u32,
    /// Total entitlement across regions, WU/s.
    pub entitlement_wu_s: f64,
    /// Monthly price in minor currency units, the basis for SLA credits.
    pub monthly_price: u64,
    pub currency: String,
    pub shape: Shape,
    /// Periods excluded from the SLA (docs/09 §4): customer-initiated changes and declared
    /// region incidents.
    pub exclusions: Vec<ExclusionWindow>,
}

/// A period whose requests don't count towards SLA attainment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExclusionWindow {
    /// Unix milliseconds, inclusive.
    pub start_ms: u64,
    /// Unix milliseconds, exclusive.
    pub end_ms: u64,
    /// For example `resize`, `activation`, `failover`, `region_outage`.
    pub reason: String,
    /// Only requests served in this region are excluded. `None` means every region.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

impl ExclusionWindow {
    pub fn covers(&self, region: &str, at_ms: u64) -> bool {
        self.start_ms <= at_ms
            && at_ms < self.end_ms
            && self.region.as_deref().is_none_or(|r| r == region)
    }
}

/// The directory couldn't answer (for example, its store is down). Retryable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("directory unavailable: {0}")]
pub struct DirectoryError(pub String);

pub trait Directory: Send + Sync + 'static {
    /// Tenant owning a management API key.
    fn tenant_for_key(&self, key: &str) -> Option<String>;

    /// Region owning a pull/push token.
    fn region_for_token(&self, token: &str) -> Option<String>;

    /// The tenant's reservation, or `None` if it doesn't exist or belongs to someone else.
    fn reservation(
        &self,
        tenant: &str,
        id: &str,
    ) -> impl Future<Output = Result<Option<ReservationInfo>, DirectoryError>> + Send;

    /// Current time, Unix milliseconds.
    fn now_ms(&self) -> u64;
}
