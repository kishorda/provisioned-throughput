//! Status conditions, following the Kubernetes `metav1.Condition` shape.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// For example `Ready`, `ProfileMismatch`, `CapacityShortfall`.
    #[serde(rename = "type")]
    pub type_: String,
    /// `True`, `False`, or `Unknown`.
    pub status: String,
    pub reason: String,
    #[serde(default)]
    pub message: String,
    /// RFC 3339 timestamp.
    pub last_transition_time: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

impl Condition {
    pub fn new(type_: &str, status: bool, reason: &str, message: impl Into<String>) -> Self {
        Self {
            type_: type_.into(),
            status: if status { "True" } else { "False" }.into(),
            reason: reason.into(),
            message: message.into(),
            last_transition_time: String::new(),
            observed_generation: None,
        }
    }

    pub fn is_true(&self) -> bool {
        self.status == "True"
    }
}

/// Replace or add `new` in `conditions`. Keeps the previous transition time when the status
/// hasn't changed, and stamps `now` when it has.
pub fn set_condition(conditions: &mut Vec<Condition>, mut new: Condition, now: &str) {
    match conditions.iter_mut().find(|c| c.type_ == new.type_) {
        Some(existing) => {
            new.last_transition_time = if existing.status == new.status {
                existing.last_transition_time.clone()
            } else {
                now.into()
            };
            *existing = new;
        }
        None => {
            new.last_transition_time = now.into();
            conditions.push(new);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_time_only_changes_with_status() {
        let mut cs = Vec::new();
        set_condition(&mut cs, Condition::new("Ready", true, "Sized", ""), "t1");
        set_condition(
            &mut cs,
            Condition::new("Ready", true, "Sized", "again"),
            "t2",
        );
        assert_eq!(cs.len(), 1);
        assert_eq!(cs[0].last_transition_time, "t1");
        assert_eq!(cs[0].message, "again");
        set_condition(&mut cs, Condition::new("Ready", false, "Broken", ""), "t3");
        assert_eq!(cs[0].last_transition_time, "t3");
    }
}
