//! Render child objects for a `ModelPool`: a Dynamo `DynamoGraphDeployment` (DGD) and one
//! `PodDisruptionBudget` per worker role.
//!
//! The DGD layout follows Dynamo's `nvidia.com/v1alpha1` examples: a `Frontend` service
//! running the KV-aware router, and worker services with `componentType: worker` and
//! `subComponentType: prefill|decode` when disaggregated. Dynamo's CRD is still alpha, so
//! check field names and worker flags against the Dynamo release you deploy. Everything
//! version-specific is in this module.

use std::collections::BTreeMap;

use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use pt_crds::labels;
use pt_crds::pool::{ModelPoolSpec, RoleReplicas};
use pt_crds::profile::{Backend, PerformanceProfileSpec};
use serde_json::{json, Value};

pub const DGD_API_VERSION: &str = "nvidia.com/v1alpha1";
pub const DGD_KIND: &str = "DynamoGraphDeployment";
pub const CONDITIONAL_DISAGG_ANNOTATION: &str = "pt.example.com/conditional-disaggregation";
/// Warm spares to keep staged (weights on node-local NVMe, no GPU claim), for the weight
/// prefetcher. Dynamo has no warm-spare concept, so this records intent.
pub const WARM_SPARES_STAGED_ANNOTATION: &str = "pt.example.com/warm-spares-staged";
/// Warm spares loaded into the worker replicas for an active failover.
pub const WARM_SPARES_LOADED_ANNOTATION: &str = "pt.example.com/warm-spares-loaded";
/// Frontend replicas. The frontend is CPU-only and stateless.
pub const FRONTEND_REPLICAS: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Aggregated,
    Prefill,
    Decode,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Aggregated => "aggregated",
            Role::Prefill => "prefill",
            Role::Decode => "decode",
        }
    }

    fn service_name(self) -> &'static str {
        match self {
            Role::Aggregated => "Worker",
            Role::Prefill => "PrefillWorker",
            Role::Decode => "DecodeWorker",
        }
    }

    /// Worker roles present for these replica counts.
    pub fn for_replicas(r: &RoleReplicas) -> Vec<(Role, u32)> {
        if r.prefill > 0 || r.decode > 0 {
            vec![(Role::Prefill, r.prefill), (Role::Decode, r.decode)]
        } else {
            vec![(Role::Aggregated, r.aggregated)]
        }
    }
}

/// Everything the renderers need about the owning pool.
pub struct PoolRef<'a> {
    pub name: &'a str,
    pub namespace: &'a str,
    pub owner: OwnerReference,
    pub spec: &'a ModelPoolSpec,
}

fn pod_labels(pool: &str, role: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (labels::POOL.to_string(), pool.to_string()),
        (labels::ROLE.to_string(), role.to_string()),
    ])
}

fn object_labels(pool: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (labels::POOL.to_string(), pool.to_string()),
        (labels::MANAGED_BY.to_string(), labels::MANAGER.to_string()),
    ])
}

/// Worker module and arguments for a backend and role.
fn worker_command(backend: Backend, model: &str, role: Role) -> (&'static str, Vec<String>) {
    let mut args = Vec::new();
    let module = match backend {
        Backend::Vllm => {
            args.extend(["--model".into(), model.into()]);
            if role == Role::Prefill {
                args.push("--is-prefill-worker".into());
            }
            "dynamo.vllm"
        }
        Backend::Trtllm | Backend::Sglang => {
            args.extend(["--model-path".into(), model.into()]);
            if role != Role::Aggregated {
                args.extend(["--disaggregation-mode".into(), role.as_str().into()]);
            }
            if backend == Backend::Trtllm {
                "dynamo.trtllm"
            } else {
                "dynamo.sglang"
            }
        }
    };
    (module, args)
}

pub fn dynamo_graph_deployment(
    pool: &PoolRef<'_>,
    profile: &PerformanceProfileSpec,
    replicas: &RoleReplicas,
) -> Value {
    let spec = pool.spec;
    let mut services = serde_json::Map::new();
    let frontend_args = ["--router-mode", "kv"];
    services.insert(
        "Frontend".into(),
        json!({
            "componentType": "frontend",
            "replicas": FRONTEND_REPLICAS,
            "extraPodMetadata": { "labels": pod_labels(pool.name, "frontend") },
            "extraPodSpec": {
                "mainContainer": {
                    "image": spec.engine.image,
                    "command": ["python3", "-m", "dynamo.frontend"],
                    "args": frontend_args,
                }
            }
        }),
    );

    let gpus = profile.parallelism.gpus().to_string();
    for (role, count) in Role::for_replicas(replicas) {
        let (module, mut args) = worker_command(spec.engine.backend, &spec.model, role);
        args.extend(spec.engine.extra_args.iter().cloned());
        let mut svc = json!({
            "componentType": "worker",
            "replicas": count,
            "resources": { "limits": { "gpu": gpus } },
            "extraPodMetadata": { "labels": pod_labels(pool.name, role.as_str()) },
            "extraPodSpec": {
                "mainContainer": {
                    "image": spec.engine.image,
                    "command": ["python3", "-m", module],
                    "args": args,
                }
            }
        });
        if role != Role::Aggregated {
            svc["subComponentType"] = json!(role.as_str());
        }
        services.insert(role.service_name().into(), svc);
    }

    json!({
        "apiVersion": DGD_API_VERSION,
        "kind": DGD_KIND,
        "metadata": {
            "name": pool.name,
            "namespace": pool.namespace,
            "labels": object_labels(pool.name),
            // How conditional disaggregation is configured differs across Dynamo releases,
            // so record the intent here for the router extension to read.
            "annotations": {
                CONDITIONAL_DISAGG_ANNOTATION: spec.disaggregation.conditional.to_string(),
            },
            "ownerReferences": [pool.owner],
        },
        "spec": { "services": services },
    })
}

/// Record warm-spare state on a rendered DGD: how many to keep staged, and how many are
/// loaded into its worker replicas for a failover.
pub fn annotate_warm_spares(dgd: &mut Value, warm_spares: u32, loaded: u32) {
    let a = &mut dgd["metadata"]["annotations"];
    a[WARM_SPARES_STAGED_ANNOTATION] = json!(warm_spares.saturating_sub(loaded).to_string());
    a[WARM_SPARES_LOADED_ANNOTATION] = json!(loaded.to_string());
}

/// One PDB per worker role. Voluntary disruptions (drains, upgrades) may take down
/// maintenance slots and hot spares, but never the floor plus failure-domain headroom
/// (docs/06 §6). Counts are in pods, so this assumes one pod per replica; multi-node
/// replicas need Grove-level budgets.
pub fn disruption_budgets(
    pool: &PoolRef<'_>,
    min_available: &RoleReplicas,
) -> Vec<PodDisruptionBudget> {
    Role::for_replicas(min_available)
        .into_iter()
        .map(|(role, min)| PodDisruptionBudget {
            metadata: ObjectMeta {
                name: Some(format!("{}-{}", pool.name, role.as_str())),
                namespace: Some(pool.namespace.to_string()),
                labels: Some(object_labels(pool.name)),
                owner_references: Some(vec![pool.owner.clone()]),
                ..Default::default()
            },
            spec: Some(PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::Int(min as i32)),
                selector: Some(LabelSelector {
                    match_labels: Some(pod_labels(pool.name, role.as_str())),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            status: None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pt_core::cost::TierCapacity;
    use pt_core::Coefficients;
    use pt_crds::pool::{Disaggregation, EngineSpec, Headroom, Payg};
    use pt_crds::profile::{EngineVersion, Parallelism};

    fn spec(backend: Backend, disagg: bool) -> ModelPoolSpec {
        ModelPoolSpec {
            model: "meta-llama/Llama-4-Maverick".into(),
            profile_ref: "p".into(),
            engine: EngineSpec {
                backend,
                version: "1.2".into(),
                image: "nvcr.io/nvidia/ai-dynamo/runtime:1.2".into(),
                extra_args: vec!["--max-num-seqs".into(), "256".into()],
            },
            isolation: Default::default(),
            disaggregation: Disaggregation {
                enabled: disagg,
                ..Default::default()
            },
            headroom: Headroom::default(),
            target_utilization: 0.85,
            burst_z: 2.33,
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
                tp: 4,
                pp: 2,
                ep: 1,
            },
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 3.0,
                d: 0.0,
            },
            decode_modifiers: Default::default(),
            capacity: TierCapacity::default(),
            role_capacity: None,
        }
    }

    fn owner() -> OwnerReference {
        OwnerReference {
            api_version: "pt.example.com/v1".into(),
            kind: "ModelPool".into(),
            name: "maverick".into(),
            uid: "uid-1".into(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    #[test]
    fn aggregated_dgd() {
        let s = spec(Backend::Vllm, false);
        let p = PoolRef {
            name: "maverick",
            namespace: "pt",
            owner: owner(),
            spec: &s,
        };
        let dgd = dynamo_graph_deployment(&p, &profile(), &RoleReplicas::aggregated(7));
        assert_eq!(dgd["kind"], DGD_KIND);
        assert_eq!(dgd["metadata"]["ownerReferences"][0]["uid"], "uid-1");
        let w = &dgd["spec"]["services"]["Worker"];
        assert_eq!(w["replicas"], 7);
        assert_eq!(w["resources"]["limits"]["gpu"], "8");
        assert_eq!(w["extraPodMetadata"]["labels"][labels::ROLE], "aggregated");
        assert_eq!(
            w["extraPodSpec"]["mainContainer"]["args"],
            json!([
                "--model",
                "meta-llama/Llama-4-Maverick",
                "--max-num-seqs",
                "256"
            ])
        );
        assert!(w.get("subComponentType").is_none());
        assert_eq!(
            dgd["spec"]["services"]["Frontend"]["componentType"],
            "frontend"
        );
    }

    #[test]
    fn disaggregated_dgd_has_both_roles() {
        let s = spec(Backend::Trtllm, true);
        let p = PoolRef {
            name: "maverick",
            namespace: "pt",
            owner: owner(),
            spec: &s,
        };
        let dgd = dynamo_graph_deployment(&p, &profile(), &RoleReplicas::disaggregated(3, 9));
        let svcs = &dgd["spec"]["services"];
        assert!(svcs.get("Worker").is_none());
        assert_eq!(
            dgd["metadata"]["annotations"][CONDITIONAL_DISAGG_ANNOTATION],
            "true"
        );
        assert_eq!(svcs["PrefillWorker"]["replicas"], 3);
        assert_eq!(svcs["PrefillWorker"]["subComponentType"], "prefill");
        assert_eq!(svcs["DecodeWorker"]["replicas"], 9);
        let args = &svcs["DecodeWorker"]["extraPodSpec"]["mainContainer"]["args"];
        assert_eq!(args[2], "--disaggregation-mode");
        assert_eq!(args[3], "decode");
    }

    #[test]
    fn pdbs_protect_floor_plus_failure_headroom() {
        let s = spec(Backend::Trtllm, true);
        let p = PoolRef {
            name: "maverick",
            namespace: "pt",
            owner: owner(),
            spec: &s,
        };
        let pdbs = disruption_budgets(&p, &RoleReplicas::disaggregated(4, 11));
        assert_eq!(pdbs.len(), 2);
        let decode = &pdbs[1];
        assert_eq!(decode.metadata.name.as_deref(), Some("maverick-decode"));
        let spec = decode.spec.as_ref().unwrap();
        assert_eq!(spec.min_available, Some(IntOrString::Int(11)));
        let sel = spec
            .selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert_eq!(sel[labels::ROLE], "decode");
        assert_eq!(sel[labels::POOL], "maverick");
    }
}
