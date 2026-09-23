//! `CapacityReservation`: a read-only regional mirror of a global contract (docs/08 §2).
//! The global Reservation Service remains the source of truth.

use kube::CustomResource;
use pt_admission::BoundaryPolicy;
use pt_core::{PoolIsolation, Shape, TermMonths, Tier};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pt.example.com",
    version = "v1",
    kind = "CapacityReservation",
    plural = "capacityreservations",
    shortname = "ptres",
    namespaced,
    printcolumn = r#"{"name":"Tenant","type":"string","jsonPath":".spec.tenant"}"#,
    printcolumn = r#"{"name":"Model","type":"string","jsonPath":".spec.model"}"#,
    printcolumn = r#"{"name":"CUs","type":"integer","jsonPath":".spec.cus"}"#,
    printcolumn = r#"{"name":"Tier","type":"string","jsonPath":".spec.tier"}"#,
    printcolumn = r#"{"name":"Term ends","type":"string","jsonPath":".spec.termEnd"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct CapacityReservationSpec {
    pub tenant: String,
    pub model: String,
    /// Minimum 1.
    #[schemars(range(min = 1))]
    pub cus: u32,
    pub tier: Tier,
    pub shape: Shape,
    #[serde(default)]
    pub sku: Sku,
    #[serde(default)]
    pub isolation: PoolIsolation,
    /// 1, 3, or 6 months.
    pub term_months: TermMonths,
    /// RFC 3339 date the term ends.
    pub term_end: String,
    #[serde(default)]
    pub boundary_policy: BoundaryPolicy,
    /// GPU class for hardware-pinned reservations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_gpu_class: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Sku {
    #[default]
    Regional,
    MultiRegion,
    HardwarePinned,
}
