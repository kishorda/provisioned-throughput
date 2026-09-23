//! Decide what a `ModelPool` should look like: status conditions plus the child objects to
//! apply. Pure, so every rule is testable without a cluster.

use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use pt_core::PoolIsolation;
use pt_crds::condition::set_condition;
use pt_crds::pool::{ModelPoolSpec, ModelPoolStatus};
use pt_crds::profile::PerformanceProfileSpec;
use pt_crds::{Condition, PoolAllocationSpec};
use serde_json::Value;

use crate::render::{self, PoolRef};
use crate::sizing::{self, Demand};

pub const READY: &str = "Ready";
pub const CAPACITY_SHORTFALL: &str = "CapacityShortfall";

pub struct PoolInput<'a> {
    pub name: &'a str,
    pub namespace: &'a str,
    pub generation: Option<i64>,
    pub spec: &'a ModelPoolSpec,
    pub previous: Option<&'a ModelPoolStatus>,
    pub owner: OwnerReference,
}

pub struct Children {
    pub dgd: Value,
    pub pdbs: Vec<PodDisruptionBudget>,
}

pub struct Plan {
    pub status: ModelPoolStatus,
    /// `None` when the pool can't be reconciled. Existing children are left as they are,
    /// so a bad profile or spec never scales a serving pool down.
    pub children: Option<Children>,
}

pub fn plan(
    pool: &PoolInput<'_>,
    profile: Option<&PerformanceProfileSpec>,
    allocations: &[PoolAllocationSpec],
    now: &str,
) -> Plan {
    let mut status = pool.previous.cloned().unwrap_or_default();
    status.observed_generation = pool.generation;
    status.allocations = allocations.len() as u32;
    status.allocated_wu_per_sec = allocations.iter().map(|a| a.wu_per_sec).sum();

    let blocked = |mut status: ModelPoolStatus, reason: &str, message: String| {
        set_condition(
            &mut status.conditions,
            Condition::new(READY, false, reason, message),
            now,
        );
        Plan {
            status,
            children: None,
        }
    };

    let Some(profile) = profile else {
        return blocked(
            status,
            "ProfileNotFound",
            format!(
                "PerformanceProfile {} does not exist.",
                pool.spec.profile_ref
            ),
        );
    };
    if let Some(msg) = profile_mismatch(pool.spec, profile) {
        return blocked(status, "ProfileMismatch", msg);
    }
    if let Some(msg) = invalid_spec(pool.spec) {
        return blocked(status, "InvalidSpec", msg);
    }

    let demands: Vec<Demand> = allocations.iter().map(Demand::from).collect();
    let sizing = match sizing::size(pool.spec, profile, &demands) {
        Ok(s) => s,
        Err(e) => return blocked(status, "SizingFailed", e.to_string()),
    };

    status.provisioned_floor = sizing.floor;
    status.desired_replicas = sizing.desired;
    status.min_available = sizing.min_available;
    let shortfall = match sizing.shortfall {
        Some(s) => Condition::new(
            CAPACITY_SHORTFALL,
            true,
            "MaxReplicasExceeded",
            format!(
                "Allocations need {} replicas but maxReplicas is {}. Provisioned SLOs are at risk.",
                s.needed, s.max
            ),
        ),
        None => Condition::new(CAPACITY_SHORTFALL, false, "WithinBudget", ""),
    };
    set_condition(&mut status.conditions, shortfall, now);
    set_condition(
        &mut status.conditions,
        Condition::new(
            READY,
            true,
            "Reconciled",
            format!(
                "{} allocations, {:.0} WU/s, {} replicas.",
                allocations.len(),
                status.allocated_wu_per_sec,
                sizing.desired.total
            ),
        ),
        now,
    );

    let pool_ref = PoolRef {
        name: pool.name,
        namespace: pool.namespace,
        owner: pool.owner.clone(),
        spec: pool.spec,
    };
    let children = Children {
        dgd: render::dynamo_graph_deployment(&pool_ref, profile, &sizing.desired),
        pdbs: render::disruption_budgets(&pool_ref, &sizing.min_available),
    };
    Plan {
        status,
        children: Some(children),
    }
}

/// The pool's engine must be the one the profile was calibrated on (docs/08 §5).
fn profile_mismatch(pool: &ModelPoolSpec, profile: &PerformanceProfileSpec) -> Option<String> {
    if pool.model != profile.model {
        return Some(format!(
            "Pool serves {} but the profile is for {}.",
            pool.model, profile.model
        ));
    }
    if pool.engine.backend != profile.engine.backend
        || pool.engine.version != profile.engine.version
    {
        return Some(format!(
            "Pool runs {} {} but the profile was calibrated on {} {}. Recalibrate before rolling the engine.",
            pool.engine.backend.as_str(),
            pool.engine.version,
            profile.engine.backend.as_str(),
            profile.engine.version
        ));
    }
    None
}

fn invalid_spec(pool: &ModelPoolSpec) -> Option<String> {
    if pool.isolation == PoolIsolation::StrictDedicated && pool.payg.backfill {
        return Some(
            "Strict-dedicated pools can't backfill PAYG. Set payg.backfill to false.".into(),
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use pt_core::cost::TierCapacity;
    use pt_core::{Coefficients, Tier};
    use pt_crds::pool::{Disaggregation, EngineSpec, Headroom, Payg};
    use pt_crds::profile::{Backend, EngineVersion, Parallelism};

    fn spec() -> ModelPoolSpec {
        ModelPoolSpec {
            model: "m".into(),
            profile_ref: "prof".into(),
            engine: EngineSpec {
                backend: Backend::Trtllm,
                version: "1.2".into(),
                image: "img".into(),
                extra_args: vec![],
            },
            isolation: PoolIsolation::Shared,
            disaggregation: Disaggregation::default(),
            headroom: Headroom::default(),
            target_utilization: 1.0,
            burst_z: 0.0,
            payg: Payg::default(),
            max_replicas: None,
        }
    }

    fn profile() -> PerformanceProfileSpec {
        PerformanceProfileSpec {
            model: "m".into(),
            gpu_class: "B200".into(),
            engine: EngineVersion {
                backend: Backend::Trtllm,
                version: "1.2".into(),
            },
            parallelism: Parallelism {
                tp: 8,
                pp: 1,
                ep: 1,
            },
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 3.0,
                d: 0.0,
            },
            decode_modifiers: Default::default(),
            capacity: TierCapacity {
                interactive: 10_000.0,
                agentic: 8_000.0,
                standard: 20_000.0,
            },
            role_capacity: None,
        }
    }

    fn alloc(wu: f64) -> PoolAllocationSpec {
        PoolAllocationSpec {
            reservation: "r".into(),
            tenant: "t".into(),
            pool: "pool".into(),
            wu_per_sec: wu,
            tier: Tier::Interactive,
            kv_share: 0.1,
            burst_factor: 1.0,
            dedicated_workers: vec![],
        }
    }

    fn input(spec: &ModelPoolSpec) -> PoolInput<'_> {
        PoolInput {
            name: "pool",
            namespace: "pt",
            generation: Some(3),
            spec,
            previous: None,
            owner: OwnerReference::default(),
        }
    }

    fn ready(status: &ModelPoolStatus) -> &Condition {
        status.conditions.iter().find(|c| c.type_ == READY).unwrap()
    }

    #[test]
    fn reconciles_and_renders_children() {
        let s = spec();
        let p = plan(
            &input(&s),
            Some(&profile()),
            &[alloc(25_000.0), alloc(5_000.0)],
            "t1",
        );
        assert!(ready(&p.status).is_true());
        assert_eq!(p.status.allocations, 2);
        assert_eq!(p.status.allocated_wu_per_sec, 30_000.0);
        assert_eq!(p.status.provisioned_floor.total, 3);
        assert_eq!(p.status.observed_generation, Some(3));
        let children = p.children.unwrap();
        // floor 3 + failure k 1 + maintenance 1
        assert_eq!(children.dgd["spec"]["services"]["Worker"]["replicas"], 5);
        assert_eq!(children.pdbs.len(), 1);
    }

    #[test]
    fn missing_profile_blocks_without_children() {
        let s = spec();
        let p = plan(&input(&s), None, &[alloc(1.0)], "t1");
        assert!(!ready(&p.status).is_true());
        assert_eq!(ready(&p.status).reason, "ProfileNotFound");
        assert!(p.children.is_none());
    }

    #[test]
    fn engine_version_mismatch_blocks() {
        let mut s = spec();
        s.engine.version = "1.3".into();
        let p = plan(&input(&s), Some(&profile()), &[], "t1");
        assert_eq!(ready(&p.status).reason, "ProfileMismatch");
        assert!(ready(&p.status).message.contains("Recalibrate"));
        assert!(p.children.is_none());
    }

    #[test]
    fn strict_dedicated_with_backfill_is_invalid() {
        let mut s = spec();
        s.isolation = PoolIsolation::StrictDedicated;
        let p = plan(&input(&s), Some(&profile()), &[], "t1");
        assert_eq!(ready(&p.status).reason, "InvalidSpec");
        s.payg.backfill = false;
        let p = plan(&input(&s), Some(&profile()), &[], "t1");
        assert!(ready(&p.status).is_true());
    }

    #[test]
    fn shortfall_condition_reflects_max_replicas() {
        let mut s = spec();
        s.max_replicas = Some(3);
        let p = plan(&input(&s), Some(&profile()), &[alloc(30_000.0)], "t1");
        let c = p
            .status
            .conditions
            .iter()
            .find(|c| c.type_ == CAPACITY_SHORTFALL)
            .unwrap();
        assert!(c.is_true());
        assert_eq!(p.status.desired_replicas.total, 3);
    }

    #[test]
    fn unknown_tier_capacity_fails_sizing() {
        let s = spec();
        let mut prof = profile();
        prof.capacity.interactive = 0.0;
        let p = plan(&input(&s), Some(&prof), &[alloc(1.0)], "t1");
        assert_eq!(ready(&p.status).reason, "SizingFailed");
    }
}
