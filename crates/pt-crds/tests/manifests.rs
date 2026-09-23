//! Keep committed manifests in sync with the Rust types.

use std::path::PathBuf;

use pt_crds::{CapacityReservation, ModelPool, PerformanceProfile, PoolAllocation};
use serde::de::DeserializeOwned;

fn deploy(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy")
        .join(path)
}

#[test]
fn committed_crds_match_generated() {
    for crd in pt_crds::all_crds() {
        let file = pt_crds::crd_file_name(&crd);
        let committed = std::fs::read_to_string(deploy("crds").join(&file))
            .unwrap_or_else(|e| panic!("deploy/crds/{file}: {e}"));
        assert_eq!(
            committed,
            pt_crds::crd_yaml(&crd).unwrap(),
            "deploy/crds/{file} is stale. Run `cargo run -p pt-crds --bin crdgen`."
        );
    }
}

#[test]
fn crd_scopes() {
    let scopes: Vec<_> = pt_crds::all_crds()
        .into_iter()
        .map(|c| (c.spec.names.kind, c.spec.scope))
        .collect();
    assert_eq!(
        scopes,
        [
            ("PerformanceProfile".to_string(), "Cluster".to_string()),
            ("ModelPool".to_string(), "Namespaced".to_string()),
            ("PoolAllocation".to_string(), "Namespaced".to_string()),
            ("CapacityReservation".to_string(), "Namespaced".to_string()),
        ]
    );
}

fn load<T: DeserializeOwned>(file: &str) -> T {
    let text = std::fs::read_to_string(deploy("examples").join(file)).unwrap();
    serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("deploy/examples/{file}: {e}"))
}

#[test]
fn examples_parse() {
    let profile: PerformanceProfile = load("performanceprofile.yaml");
    let pool: ModelPool = load("modelpool.yaml");
    let alloc: PoolAllocation = load("poolallocation.yaml");
    let res: CapacityReservation = load("capacityreservation.yaml");

    // The examples reference each other.
    assert_eq!(
        pool.spec.profile_ref,
        profile.metadata.name.clone().unwrap()
    );
    assert_eq!(pool.spec.model, profile.spec.model);
    assert_eq!(alloc.spec.pool, pool.metadata.name.clone().unwrap());
    assert_eq!(alloc.spec.reservation, res.metadata.name.clone().unwrap());
    assert_eq!(u8::from(res.spec.term_months), 3);
}

#[test]
fn term_months_rejects_other_lengths() {
    let text = std::fs::read_to_string(deploy("examples/capacityreservation.yaml"))
        .unwrap()
        .replace("termMonths: 3", "termMonths: 12");
    let err = serde_yaml::from_str::<CapacityReservation>(&text).unwrap_err();
    assert!(err.to_string().contains("1, 3, or 6"), "{err}");
}
