//! What telemetry needs to know about tenants, regions, and reservations. The control plane
//! implements this, which keeps this crate independent of its storage.

use std::future::Future;

use pt_core::{Shape, Tier};

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
}

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
    ) -> impl Future<Output = Option<ReservationInfo>> + Send;

    /// Current time, Unix milliseconds.
    fn now_ms(&self) -> u64;
}
