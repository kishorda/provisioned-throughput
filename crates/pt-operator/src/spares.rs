//! Which worker pods are hot spares (ADR-044). Pure.
//!
//! A pool keeps `headroom.hotSpares` loaded replicas per role that serve PAYG until a
//! region failover needs them. The controller labels every Ready worker pod
//! `pt.example.com/serving=floor` or `=spare`, and the pool's two headless Services select
//! on that label, so routers find floor workers and spares through DNS.
//!
//! Spares come only from Ready pods beyond `min_available` (floor plus failure-domain
//! headroom) for their role, so a pool that is short never hides its floor behind spares.
//!
//! The choice is stable: pods that are spares stay spares while they're Ready, so a
//! reconcile doesn't move PAYG around. A spare that stops being Ready goes back to floor,
//! so it returns as a floor worker and another Ready pod takes its place.

use std::collections::BTreeMap;

use pt_crds::labels;

use crate::render::Role;

/// A worker pod as spare assignment sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodServing {
    pub name: String,
    pub role: Role,
    pub ready: bool,
    pub terminating: bool,
    /// Its current `pt.example.com/serving` label, if any.
    pub serving: Option<String>,
}

impl PodServing {
    fn is_spare(&self) -> bool {
        self.serving.as_deref() == Some(labels::SPARE)
    }
}

/// The label changes to make: pod name → `floor` or `spare`, sorted by name.
/// `min_available(role)` is the role's floor plus failure-domain headroom.
pub fn assign(
    pods: &[PodServing],
    hot_spares: u32,
    min_available: impl Fn(Role) -> u32,
) -> Vec<(String, &'static str)> {
    let mut by_role: BTreeMap<&str, Vec<&PodServing>> = BTreeMap::new();
    for p in pods {
        by_role.entry(p.role.as_str()).or_default().push(p);
    }
    let mut changes = Vec::new();
    for pods in by_role.values() {
        let role = pods[0].role;
        let mut serving: Vec<&PodServing> = pods
            .iter()
            .copied()
            .filter(|p| p.ready && !p.terminating)
            .collect();
        serving.sort_by(|a, b| a.name.cmp(&b.name));
        let beyond = serving.len().saturating_sub(min_available(role) as usize);
        let want = (hot_spares as usize).min(beyond);
        // Keep current spares first, then promote the newest-named floor pods.
        let mut spares: Vec<&str> = serving
            .iter()
            .filter(|p| p.is_spare())
            .map(|p| p.name.as_str())
            .take(want)
            .collect();
        for p in serving.iter().rev() {
            if spares.len() >= want {
                break;
            }
            if !p.is_spare() {
                spares.push(&p.name);
            }
        }
        for p in &serving {
            let target = if spares.contains(&p.name.as_str()) {
                labels::SPARE
            } else {
                labels::FLOOR
            };
            if p.serving.as_deref() != Some(target) {
                changes.push((p.name.clone(), target));
            }
        }
        // A spare that isn't serving any more returns as floor.
        for p in pods
            .iter()
            .filter(|p| !(p.ready && !p.terminating) && p.is_spare())
        {
            changes.push((p.name.clone(), labels::FLOOR));
        }
    }
    changes.sort();
    changes
}

/// The Ready spares after applying `changes`, sorted: for the pool report.
pub fn spares_after(pods: &[PodServing], changes: &[(String, &'static str)]) -> Vec<String> {
    let mut out: Vec<String> = pods
        .iter()
        .filter(|p| p.ready && !p.terminating)
        .filter(|p| match changes.iter().find(|(n, _)| *n == p.name) {
            Some((_, v)) => *v == labels::SPARE,
            None => p.is_spare(),
        })
        .map(|p| p.name.clone())
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(name: &str, ready: bool, serving: Option<&str>) -> PodServing {
        PodServing {
            name: name.into(),
            role: Role::Decode,
            ready,
            terminating: false,
            serving: serving.map(str::to_string),
        }
    }

    fn none(_: Role) -> u32 {
        0
    }

    #[test]
    fn new_pods_get_floor_and_the_newest_names_become_spares() {
        let pods = [
            pod("d-0", true, None),
            pod("d-1", true, None),
            pod("d-2", true, None),
        ];
        assert_eq!(
            assign(&pods, 1, none),
            [
                ("d-0".into(), "floor"),
                ("d-1".into(), "floor"),
                ("d-2".into(), "spare")
            ]
        );
    }

    #[test]
    fn spares_stay_put_and_nothing_changes_when_labels_are_right() {
        let pods = [
            pod("d-0", true, Some("spare")),
            pod("d-1", true, Some("floor")),
            pod("d-2", true, Some("floor")),
        ];
        assert!(assign(&pods, 1, none).is_empty());
        // Fewer spares wanted: the extra one goes back to floor.
        let two = [
            pod("d-0", true, Some("spare")),
            pod("d-1", true, Some("spare")),
            pod("d-2", true, Some("floor")),
        ];
        assert_eq!(assign(&two, 1, none), [("d-1".into(), "floor")]);
    }

    #[test]
    fn a_spare_that_stops_being_ready_is_replaced() {
        let pods = [
            pod("d-0", true, Some("floor")),
            pod("d-1", true, Some("floor")),
            pod("d-2", false, Some("spare")),
        ];
        assert_eq!(
            assign(&pods, 1, none),
            [("d-1".into(), "spare"), ("d-2".into(), "floor")]
        );
        let changes = assign(&pods, 1, none);
        assert_eq!(spares_after(&pods, &changes), ["d-1"]);
        // Pods that aren't Ready and aren't spares are left alone.
        let pending = [pod("d-0", true, Some("floor")), pod("d-1", false, None)];
        assert!(assign(&pending, 0, none).is_empty());
    }

    #[test]
    fn roles_are_counted_separately() {
        let mut prefill = pod("p-0", true, None);
        prefill.role = Role::Prefill;
        let pods = [prefill, pod("d-0", true, None), pod("d-1", true, None)];
        assert_eq!(
            assign(&pods, 2, none),
            [
                ("d-0".into(), "spare"),
                ("d-1".into(), "spare"),
                ("p-0".into(), "spare")
            ]
        );
    }

    #[test]
    fn spares_come_only_from_pods_beyond_min_available() {
        let pods = [
            pod("d-0", true, None),
            pod("d-1", true, None),
            pod("d-2", true, Some("spare")),
        ];
        // Two must stay available: one spare fits.
        assert_eq!(
            assign(&pods, 2, |_| 2),
            [("d-0".into(), "floor"), ("d-1".into(), "floor")]
        );
        // Short of pods: the spare goes back to floor.
        assert_eq!(
            assign(&pods, 2, |_| 3),
            [
                ("d-0".into(), "floor"),
                ("d-1".into(), "floor"),
                ("d-2".into(), "floor")
            ]
        );
    }
}
