//! Surge before a drain (docs/06 §6, ADR-033).
//!
//! Drains come from anywhere: `kubectl drain`, a managed node-pool upgrade, the cluster
//! autoscaler. All of them cordon the node first, then evict its pods through the
//! Eviction API, which honours PodDisruptionBudgets and is retried while a budget says no.
//!
//! The controller watches for that. For every worker pod on a cordoned node, it adds one
//! replica of the same role (the *surge*). The pool's budget keeps `floor + failure_k`
//! available, so evictions wait until the surge replicas are ready, and then go through.
//! The pool never drops below its floor plus failure-domain headroom, and doesn't eat into
//! its maintenance slots or hot spares either. When the node has no more pool pods, or is
//! uncordoned, the surge goes away.
//!
//! **Expedite.** A node annotated `pt.example.com/drain=expedite` (an urgent security
//! patch) gets no surge. Its pods leave through the maintenance slots and hot spares right
//! away, which the budget already allows.
//!
//! Pure: the controller lists the pool's worker pods and the nodes, and passes them in.

use std::collections::{BTreeSet, HashMap};

use pt_crds::pool::RoleReplicas;

use crate::render::Role;

/// Node annotation that asks for an expedited drain.
pub const DRAIN_ANNOTATION: &str = "pt.example.com/drain";
pub const EXPEDITE: &str = "expedite";

/// One of the pool's worker pods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPod {
    pub node: String,
    pub role: Role,
    /// Being deleted already: its replacement is the owner's job, not a surge's.
    pub terminating: bool,
}

/// What the drain logic needs to know about a node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NodeState {
    /// Cordoned (`spec.unschedulable`).
    pub unschedulable: bool,
    pub expedite: bool,
}

/// Drains affecting one pool.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Drain {
    /// Extra replicas per role while the drains last.
    pub surge: RoleReplicas,
    /// Cordoned nodes that still host the pool's worker pods, sorted.
    pub nodes: Vec<String>,
    /// Of those, the ones draining through the maintenance slots without a surge.
    pub expedited: Vec<String>,
}

impl Drain {
    pub fn active(&self) -> bool {
        !self.nodes.is_empty()
    }
}

pub fn assess(pods: &[WorkerPod], nodes: &HashMap<String, NodeState>) -> Drain {
    let mut surge = RoleReplicas::default();
    let mut draining = BTreeSet::new();
    let mut expedited = BTreeSet::new();
    for p in pods.iter().filter(|p| !p.terminating) {
        let Some(node) = nodes.get(&p.node).filter(|n| n.unschedulable) else {
            continue;
        };
        draining.insert(p.node.clone());
        if node.expedite {
            expedited.insert(p.node.clone());
            continue;
        }
        match p.role {
            Role::Aggregated => surge.aggregated += 1,
            Role::Prefill => surge.prefill += 1,
            Role::Decode => surge.decode += 1,
        }
        surge.total += 1;
    }
    Drain {
        surge,
        nodes: draining.into_iter().collect(),
        expedited: expedited.into_iter().collect(),
    }
}

/// `desired` plus as much of `surge` as `max` allows. Returns the surge actually added.
pub fn add_surge(
    desired: RoleReplicas,
    surge: RoleReplicas,
    max: Option<u32>,
) -> (RoleReplicas, RoleReplicas) {
    let mut room = max.map_or(u32::MAX, |m| m.saturating_sub(desired.total));
    let mut take = |n: u32| {
        let t = n.min(room);
        room -= t;
        t
    };
    // Decode first: it holds the KV cache and serves every token of a stream.
    let decode = take(surge.decode);
    let prefill = take(surge.prefill);
    let aggregated = take(surge.aggregated);
    let added = RoleReplicas {
        aggregated,
        prefill,
        decode,
        total: aggregated + prefill + decode,
    };
    let out = RoleReplicas {
        aggregated: desired.aggregated + aggregated,
        prefill: desired.prefill + prefill,
        decode: desired.decode + decode,
        total: desired.total + added.total,
    };
    (out, added)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(node: &str, role: Role) -> WorkerPod {
        WorkerPod {
            node: node.into(),
            role,
            terminating: false,
        }
    }

    fn nodes(v: &[(&str, bool, bool)]) -> HashMap<String, NodeState> {
        v.iter()
            .map(|(n, u, e)| {
                (
                    n.to_string(),
                    NodeState {
                        unschedulable: *u,
                        expedite: *e,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn surges_one_replica_per_pod_on_a_cordoned_node() {
        let pods = [
            pod("a", Role::Decode),
            pod("a", Role::Prefill),
            pod("b", Role::Decode),
            pod("c", Role::Decode),
        ];
        let d = assess(&pods, &nodes(&[("a", true, false), ("b", false, false)]));
        assert_eq!(d.surge, RoleReplicas::disaggregated(1, 1));
        assert_eq!(d.nodes, ["a"]);
        assert!(d.expedited.is_empty());
        // Nothing cordoned: no drain. Unknown nodes don't count.
        assert!(!assess(&pods, &nodes(&[("a", false, false)])).active());
    }

    #[test]
    fn expedited_and_terminating_pods_get_no_surge() {
        let mut gone = pod("a", Role::Aggregated);
        gone.terminating = true;
        let pods = [gone, pod("b", Role::Aggregated), pod("c", Role::Aggregated)];
        let d = assess(
            &pods,
            &nodes(&[("a", true, false), ("b", true, true), ("c", true, false)]),
        );
        assert_eq!(d.surge, RoleReplicas::aggregated(1), "only c's pod");
        assert_eq!(d.nodes, ["b", "c"], "a has nothing left to drain");
        assert_eq!(d.expedited, ["b"]);
    }

    #[test]
    fn surge_is_capped_by_max_replicas_decode_first() {
        let desired = RoleReplicas::disaggregated(4, 10);
        let surge = RoleReplicas::disaggregated(2, 2);
        assert_eq!(
            add_surge(desired, surge, None),
            (RoleReplicas::disaggregated(6, 12), surge)
        );
        let (out, added) = add_surge(desired, surge, Some(17));
        assert_eq!(added, RoleReplicas::disaggregated(1, 2));
        assert_eq!(out, RoleReplicas::disaggregated(5, 12));
        let (out, added) = add_surge(desired, surge, Some(10));
        assert_eq!((out, added.total), (desired, 0), "already over the cap");
    }
}
