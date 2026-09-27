//! The pool report the controller sends the control plane after each reconcile, for the
//! system dashboard (ADR-043). Pure: built from the spec, the planned status, and pod
//! readiness.

use pt_crds::pool::RoleReplicas;
use pt_crds::{ModelPoolSpec, ModelPoolStatus};
use pt_entitlement::report::{PoolReport, ReportCondition, Roles};

use crate::drain::WorkerPod;
use crate::render::Role;

fn roles(r: &RoleReplicas) -> Roles {
    Roles {
        aggregated: r.aggregated,
        prefill: r.prefill,
        decode: r.decode,
        total: r.total,
    }
}

/// Worker pods per role that are Ready and not being deleted.
pub fn ready(pods: &[WorkerPod]) -> Roles {
    let mut r = Roles::default();
    for p in pods.iter().filter(|p| p.ready && !p.terminating) {
        match p.role {
            Role::Aggregated => r.aggregated += 1,
            Role::Prefill => r.prefill += 1,
            Role::Decode => r.decode += 1,
        }
        r.total += 1;
    }
    r
}

pub fn pool_report(
    namespace: &str,
    name: &str,
    spec: &ModelPoolSpec,
    status: &ModelPoolStatus,
    ready: Roles,
) -> PoolReport {
    PoolReport {
        namespace: namespace.into(),
        name: name.into(),
        model: spec.model.clone(),
        catalog_model: spec.catalog_model.clone(),
        profile: spec.profile_ref.clone(),
        engine: format!("{} {}", spec.engine.backend.as_str(), spec.engine.version),
        desired: roles(&status.desired_replicas),
        ready,
        floor: roles(&status.provisioned_floor),
        min_available: roles(&status.min_available),
        hot_spares: spec.headroom.hot_spares,
        warm_spares_loaded: roles(&status.warm_spares_loaded),
        drain_surge: roles(&status.drain_surge),
        draining_nodes: status.draining_nodes.clone(),
        allocations: status.allocations,
        allocated_wu_per_sec: status.allocated_wu_per_sec,
        failover_wu_per_sec: status.failover_wu_per_sec,
        conditions: status
            .conditions
            .iter()
            .map(|c| ReportCondition {
                type_: c.type_.clone(),
                status: c.status.clone(),
                reason: c.reason.clone(),
                message: c.message.clone(),
            })
            .collect(),
        spare_pods: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(role: Role, ready: bool, terminating: bool) -> WorkerPod {
        WorkerPod {
            node: "n".into(),
            role,
            terminating,
            ready,
        }
    }

    #[test]
    fn only_ready_pods_that_are_staying_count() {
        let r = ready(&[
            pod(Role::Prefill, true, false),
            pod(Role::Decode, true, false),
            pod(Role::Decode, true, false),
            pod(Role::Decode, false, false),
            pod(Role::Decode, true, true),
        ]);
        assert_eq!(
            r,
            Roles {
                aggregated: 0,
                prefill: 1,
                decode: 2,
                total: 3
            }
        );
    }
}
