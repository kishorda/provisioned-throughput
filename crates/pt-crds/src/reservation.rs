//! `CapacityReservation`: a read-only regional mirror of a global contract (docs/08 §2).
//! The global Reservation Service remains the source of truth.

use kube::CustomResource;
use pt_admission::BoundaryPolicy;
use pt_core::{PoolIsolation, Shape, Tier};
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

/// Allowed term lengths. Serialised as the integer 1, 3, or 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum TermMonths {
    One,
    Three,
    Six,
}

impl TryFrom<u8> for TermMonths {
    type Error = String;
    fn try_from(v: u8) -> Result<Self, String> {
        match v {
            1 => Ok(Self::One),
            3 => Ok(Self::Three),
            6 => Ok(Self::Six),
            other => Err(format!("termMonths must be 1, 3, or 6, not {other}")),
        }
    }
}

impl From<TermMonths> for u8 {
    fn from(t: TermMonths) -> u8 {
        match t {
            TermMonths::One => 1,
            TermMonths::Three => 3,
            TermMonths::Six => 6,
        }
    }
}

impl JsonSchema for TermMonths {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "TermMonths".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "integer", "enum": [1, 3, 6] })
    }
}
