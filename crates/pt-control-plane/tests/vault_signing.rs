//! Snapshots signed by a key held in Vault's Transit engine (ADR-038), against a mock of
//! the two Transit endpoints the control plane uses.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::Json;
use base64::Engine;
use common::*;
use ed25519_dalek::{Signer as _, SigningKey};
use pt_control_plane::clock::ManualClock;
use pt_control_plane::signing::VaultConfig;
use pt_control_plane::{app, in_memory};
use pt_entitlement::{SnapshotVerifier, KEY_ID_HEADER, SIGNATURE_HEADER};
use serde_json::{json, Value};

const TOKEN_ENV: &str = "PT_TEST_VAULT_TOKEN";
const TOKEN: &str = "s.test-token";

struct Vault {
    /// Version 1 was rotated out; version 2 signs.
    keys: [SigningKey; 2],
    signs: AtomicU64,
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

fn b64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

async fn mock_vault() -> (String, Arc<Vault>) {
    let vault = Arc::new(Vault {
        keys: [
            SigningKey::from_bytes(&[7; 32]),
            SigningKey::from_bytes(&[9; 32]),
        ],
        signs: AtomicU64::new(0),
    });
    let authorized = |h: &HeaderMap| h.get("X-Vault-Token").is_some_and(|v| v == TOKEN);
    let app = axum::Router::new()
        .route(
            "/v1/transit/sign/{key}",
            post(
                move |State(v): State<Arc<Vault>>,
                      Path(key): Path<String>,
                      h: HeaderMap,
                      Json(body): Json<Value>| async move {
                    if !authorized(&h) || key != "snapshots" {
                        return (StatusCode::FORBIDDEN, Json(json!({ "errors": ["denied"] })));
                    }
                    v.signs.fetch_add(1, Ordering::SeqCst);
                    let input = b64().decode(body["input"].as_str().unwrap()).unwrap();
                    let sig = v.keys[1].sign(&input);
                    let signature = format!("vault:v2:{}", b64().encode(sig.to_bytes()));
                    (
                        StatusCode::OK,
                        Json(json!({ "data": { "signature": signature, "key_version": 2 } })),
                    )
                },
            ),
        )
        .route(
            "/v1/transit/keys/{key}",
            get(
                move |State(v): State<Arc<Vault>>, h: HeaderMap| async move {
                    if !authorized(&h) {
                        return (StatusCode::FORBIDDEN, Json(json!({ "errors": ["denied"] })));
                    }
                    let public = |k: &SigningKey| b64().encode(k.verifying_key().as_bytes());
                    (
                        StatusCode::OK,
                        Json(json!({ "data": {
                    "type": "ed25519",
                    "latest_version": 2,
                    "keys": {
                        "1": { "public_key": public(&v.keys[0]) },
                        "2": { "public_key": public(&v.keys[1]) },
                    }
                } })),
                    )
                },
            ),
        )
        .with_state(vault.clone());
    (serve(app).await, vault)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn snapshot(base: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{base}/internal/v1/entitlements/eu-west"))
        .bearer_auth("region-token-eu-west-dev")
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn vault_signs_and_gateways_verify() {
    let (vault_url, vault) = mock_vault().await;
    // SAFETY: only this test reads this variable.
    unsafe { std::env::set_var(TOKEN_ENV, TOKEN) };
    let mut c = config();
    c.entitlements.signing_key = String::new();
    c.entitlements.vault = Some(VaultConfig {
        url: vault_url.clone(),
        key: "snapshots".into(),
        token_env: TOKEN_ENV.into(),
        mount: "transit".into(),
        tls: Default::default(),
    });
    c.validate().unwrap();
    let svc = in_memory(c, ManualClock::new(t0()));
    let (routes, _) = app(svc);
    let base = serve(routes).await;

    let resp = snapshot(&base).await;
    assert_eq!(resp.status(), 200);
    let key_id = resp.headers()[KEY_ID_HEADER].to_str().unwrap().to_string();
    let sig = resp.headers()[SIGNATURE_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    let body = resp.bytes().await.unwrap();
    // A gateway trusting Vault's current public key accepts it, under that key's id.
    let public = hex(vault.keys[1].verifying_key().as_bytes());
    let verifier = SnapshotVerifier::from_hex(&public).unwrap();
    let (snap, used) = verifier.verify_with(&body, &sig, Some(&key_id)).unwrap();
    assert_eq!(snap.region, "eu-west");
    assert_eq!(used, key_id);
    // The key of the rotated-out version doesn't.
    let old = SnapshotVerifier::from_hex(&hex(vault.keys[0].verifying_key().as_bytes())).unwrap();
    assert!(old.verify_with(&body, &sig, Some(&key_id)).is_err());

    // The same snapshot again is signed from the cache.
    assert_eq!(snapshot(&base).await.status(), 200);
    assert_eq!(vault.signs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn vault_down_means_no_snapshot_not_an_unsigned_one() {
    // SAFETY: only the tests in this file read this variable, with the same value.
    unsafe { std::env::set_var(TOKEN_ENV, TOKEN) };
    let mut c = config();
    c.entitlements.signing_key = String::new();
    c.entitlements.vault = Some(VaultConfig {
        url: "http://127.0.0.1:1".into(),
        key: "snapshots".into(),
        token_env: TOKEN_ENV.into(),
        mount: "transit".into(),
        tls: Default::default(),
    });
    let svc = in_memory(c, ManualClock::new(t0()));
    let (routes, _) = app(svc);
    let base = serve(routes).await;
    let resp = snapshot(&base).await;
    assert_eq!(resp.status(), 503);
    assert!(resp.headers().get(SIGNATURE_HEADER).is_none());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "signing_unavailable");
}

#[test]
fn signer_config_rules() {
    let mut c = config();
    c.entitlements.vault = Some(VaultConfig {
        url: "https://vault.internal:8200".into(),
        key: "snapshots".into(),
        token_env: "PT_TEST_VAULT_TOKEN_UNSET".into(),
        mount: "transit".into(),
        tls: Default::default(),
    });
    assert!(c.validate().unwrap_err().to_string().contains("not both"));
    c.entitlements.signing_key = String::new();
    c.validate().unwrap();
    // Without the token in the environment, the signer can't start.
    let err = pt_control_plane::signing::Signer::from_config(&c)
        .err()
        .unwrap();
    assert!(err.contains("PT_TEST_VAULT_TOKEN_UNSET"), "{err}");
    // Plain HTTP to a remote Vault is refused.
    c.entitlements.vault.as_mut().unwrap().url = "http://vault.internal:8200".into();
    assert!(c.validate().unwrap_err().to_string().contains("TLS"));
}

#[test]
fn vault_config_parses_from_toml() {
    let v: VaultConfig = toml::from_str(
        r#"
        url = "https://vault.internal:8200"
        key = "snapshots"
        [tls]
        ca_cert = "/etc/pt/vault-ca.pem"
        "#,
    )
    .unwrap();
    assert_eq!(v.token_env, "VAULT_TOKEN");
    assert_eq!(v.mount, "transit");
    assert_eq!(v.tls.ca_cert.as_deref(), Some("/etc/pt/vault-ca.pem"));
    assert!(toml::from_str::<VaultConfig>("url = \"x\"\nkey = \"k\"\ntoken = \"secret\"").is_err());
}
