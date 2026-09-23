//! Reservation term lengths (docs/02 §8): 1, 3, or 6 months.

use serde::{Deserialize, Serialize};

/// Allowed term lengths. Serialised as the integer 1, 3, or 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum TermMonths {
    One,
    Three,
    Six,
}

impl TermMonths {
    pub fn months(self) -> u8 {
        self.into()
    }
}

impl TryFrom<u8> for TermMonths {
    type Error = String;
    fn try_from(v: u8) -> Result<Self, String> {
        match v {
            1 => Ok(Self::One),
            3 => Ok(Self::Three),
            6 => Ok(Self::Six),
            other => Err(format!("term must be 1, 3, or 6 months, not {other}")),
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

#[cfg(feature = "schema")]
impl schemars::JsonSchema for TermMonths {
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
