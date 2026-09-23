//! Plan the example manifests in `deploy/examples` end to end.

use std::path::PathBuf;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use pt_crds::{ModelPool, PerformanceProfile, PoolAllocation};
use pt_operator::plan::{plan, PoolInput, READY};
use serde::de::DeserializeOwned;

fn load<T: DeserializeOwned>(file: &str) -> T {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/examples")
        .join(file);
    serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn example_pool_sizes_and_renders() {
    let profile: PerformanceProfile = load("performanceprofile.yaml");
    let pool: ModelPool = load("modelpool.yaml");
    let alloc: PoolAllocation = load("poolallocation.yaml");

    let input = PoolInput {
        name: "maverick-b200-a1",
        namespace: "pt-serving",
        generation: Some(1),
        spec: &pool.spec,
        previous: None,
        owner: OwnerReference::default(),
    };
    let p = plan(
        &input,
        Some(&profile.spec),
        &[alloc.spec],
        "2026-09-23T00:00:00Z",
    );
    let ready = p
        .status
        .conditions
        .iter()
        .find(|c| c.type_ == READY)
        .unwrap();
    assert!(ready.is_true(), "{ready:?}");

    // 82,000 agentic WU/s with burst factor 3, split 30/70:
    //   prefill: 24,600 / 120,000 = 0.205 units, burst 2.33·0.41 = 0.955 → 1.37 → 2 (min 2)
    //   decode:  57,400 /  21,000 = 2.733 units, burst 2.33·5.467 = 12.74 → 18.2 → 19
    // Headroom per role: failure 2 + maintenance 1 + hot 1.
    assert_eq!(
        (
            p.status.provisioned_floor.prefill,
            p.status.provisioned_floor.decode
        ),
        (2, 19)
    );
    assert_eq!(
        (
            p.status.desired_replicas.prefill,
            p.status.desired_replicas.decode
        ),
        (6, 23)
    );
    assert_eq!(
        (
            p.status.min_available.prefill,
            p.status.min_available.decode
        ),
        (4, 21)
    );

    let children = p.children.unwrap();
    let svcs = &children.dgd["spec"]["services"];
    assert_eq!(svcs["PrefillWorker"]["replicas"], 6);
    assert_eq!(svcs["DecodeWorker"]["replicas"], 23);
    assert_eq!(svcs["DecodeWorker"]["resources"]["limits"]["gpu"], "8");
    assert_eq!(children.pdbs.len(), 2);
}
