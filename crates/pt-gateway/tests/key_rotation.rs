//! Rotating the snapshot signing key without an outage (docs/12 §6, ADR-020).
//!
//! The runbook: (1) add the new public key to every verifier, (2) restart the control plane
//! with the new signing key, (3) once every gateway reports the new key id, remove the old
//! public key.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pt_control_plane::clock::SystemClock;
use pt_control_plane::store::MemoryStore;
use pt_control_plane::{api as cp_api, with_store, ControlPlaneConfig};
use pt_core::cost::TierCapacity;
use pt_core::{Coefficients, PerformanceProfile};
use pt_entitlement::SnapshotSigner;
use pt_gateway::config::{EntitlementSourceConfig, ServerConfig};
use pt_gateway::health::HeartbeatClient;
use pt_gateway::sync::{SnapshotClient, SyncError};
use pt_gateway::usage::MemorySink;
use pt_gateway::{AppState, GatewayConfig};
use serde_json::json;

const PROFILE: &str = "llama-4-maverick.b200.trtllm-1.2.tp8";

fn cp_config(signing_key: Option<&str>) -> ControlPlaneConfig {
    let mut c = ControlPlaneConfig::load(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/control-plane.toml"),
    )
    .unwrap();
    if let Some(k) = signing_key {
        c.entitlements.signing_key = k.into();
    }
    c
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

fn source(cp: &str, keys: &[&str], cache: &str) -> EntitlementSourceConfig {
    EntitlementSourceConfig {
        control_plane_url: cp.into(),
        region: "eu-west".into(),
        token: "region-token-eu-west-dev".into(),
        public_key: keys[0].into(),
        extra_public_keys: keys[1..].iter().map(|k| k.to_string()).collect(),
        cache_path: Some(cache.into()),
        wait_secs: 1,
        heartbeat_interval_ms: 0,
        engine_health_path: "/healthz".into(),
        tls: Default::default(),
    }
}

fn gateway(source: EntitlementSourceConfig) -> AppState {
    let config = GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: "http://127.0.0.1:1".into(),
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: 1_000.0,
        },
        profiles: vec![PerformanceProfile {
            name: PROFILE.into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 1.0,
                d: 0.0,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }],
        entitlements: Some(source),
        quota: None,
        tokenization: Default::default(),
        usage_export: None,
        reservations: vec![],
        deployments: vec![],
    };
    AppState::new(&config, Arc::new(MemorySink::default())).unwrap()
}

#[tokio::test]
async fn rotate_without_an_outage() {
    let store = Arc::new(MemoryStore::default());
    let cache = std::env::temp_dir()
        .join(format!("pt-rotation-{}.json", uuid::Uuid::new_v4()))
        .display()
        .to_string();

    // The current control plane signs with the development key (A).
    let cp_a = with_store(cp_config(None), store.clone(), SystemClock)
        .await
        .unwrap();
    let key_a = cp_a.signer().public_key_hex();
    let id_a = cp_a.signer().key_id();
    let url_a = serve(cp_api::router(cp_a.clone())).await;
    let created: serde_json::Value = reqwest::Client::new()
        .post(format!("{url_a}/v1/provisioned-throughput"))
        .bearer_auth("sk-admin-acme-dev")
        .json(&json!({
            "name": "agents", "model": "llama-4-maverick", "tier": "agentic",
            "regions": [{ "region": "eu-west", "cus": 2 }], "term_months": 1,
            "shape": { "input_p95": 1000, "input_max": 8000, "output_p95": 100, "context_ceiling": 16000 },
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let api_key = created["api_key"].as_str().unwrap().to_string();

    // Step 1: a new key (B) is generated and every verifier trusts both.
    let (seed_b, key_b) = SnapshotSigner::generate();
    let id_b = SnapshotSigner::from_hex(&seed_b).unwrap().key_id();
    let gw = gateway(source(&url_a, &[&key_a, &key_b], &cache));
    let client = SnapshotClient::new(source(&url_a, &[&key_a, &key_b], &cache)).unwrap();
    client.poll_once(&gw, false).await.unwrap().unwrap();
    assert_eq!(gw.entitlements().key_id.as_deref(), Some(id_a.as_str()));
    assert!(gw.deployment_for_key(&api_key).is_some());

    // A gateway nobody updated (trusts only A), sharing nothing with the first.
    let stale_cache = format!("{cache}.stale");
    let stale = gateway(source(&url_a, &[&key_a], &stale_cache));
    SnapshotClient::new(source(&url_a, &[&key_a], &stale_cache))
        .unwrap()
        .poll_once(&stale, false)
        .await
        .unwrap();

    // Step 2: the control plane restarts with key B over the same store.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let cp_b = with_store(cp_config(Some(&seed_b)), store.clone(), SystemClock)
        .await
        .unwrap();
    assert_eq!(cp_b.signer().key_id(), id_b);
    let url_b = serve(cp_api::router(cp_b.clone())).await;

    let client = SnapshotClient::new(source(&url_b, &[&key_a, &key_b], &cache)).unwrap();
    let applied = client.poll_once(&gw, false).await.unwrap();
    assert!(
        applied.is_some(),
        "a restart always publishes a newer version"
    );
    assert_eq!(gw.entitlements().key_id.as_deref(), Some(id_b.as_str()));
    assert!(
        gw.deployment_for_key(&api_key).is_some(),
        "no gap in service"
    );

    // The heartbeat tells operators which key each gateway is on.
    HeartbeatClient::new(source(&url_b, &[&key_a, &key_b], &cache), "gw-1".into())
        .unwrap()
        .beat(&gw)
        .await
        .unwrap();
    let west = cp_b
        .region_statuses()
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.region == "eu-west")
        .unwrap();
    assert_eq!(west.snapshot_key_ids.get(&id_b), Some(&1));
    assert!(!west.snapshot_key_ids.contains_key(&id_a));

    // The stale gateway refuses B's snapshots but keeps serving A's: static stability.
    let err = SnapshotClient::new(source(&url_b, &[&key_a], &stale_cache))
        .unwrap()
        .poll_once(&stale, false)
        .await
        .unwrap_err();
    assert!(
        matches!(err, SyncError::Verify(pt_entitlement::Error::UnknownKey(ref id)) if *id == id_b),
        "{err}"
    );
    assert_eq!(stale.entitlements().key_id.as_deref(), Some(id_a.as_str()));
    assert!(stale.deployment_for_key(&api_key).is_some());

    // Step 3: key A is removed. A restarted gateway trusting only B loads its cache, which
    // was re-signed with B.
    let restarted = gateway(source(&url_b, &[&key_b], &cache));
    let loaded = SnapshotClient::new(source(&url_b, &[&key_b], &cache))
        .unwrap()
        .load_cache(&restarted)
        .unwrap();
    assert!(loaded.is_some());
    assert_eq!(
        restarted.entitlements().key_id.as_deref(),
        Some(id_b.as_str())
    );
    assert!(restarted.deployment_for_key(&api_key).is_some());

    let _ = std::fs::remove_file(&cache);
    let _ = std::fs::remove_file(&stale_cache);
}
