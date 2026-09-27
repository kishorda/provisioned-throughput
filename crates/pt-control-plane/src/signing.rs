//! Who signs entitlement snapshots (docs/12 §6, ADR-038).
//!
//! Either the Ed25519 key in configuration (`[entitlements] signing_key`, for development)
//! or a key held by HashiCorp Vault's Transit engine (`[entitlements.vault]`), which never
//! leaves Vault. With Vault, the control plane sends each snapshot body to
//! `POST /v1/transit/sign/<key>` and reads the public key of the version that signed it from
//! `GET /v1/transit/keys/<key>`, so the key id follows Vault's key rotation (ADR-020).
//!
//! Signatures are cached by body hash, so gateways polling for the same snapshot don't each
//! cost a call to Vault. If Vault is unreachable, signing fails and the snapshot endpoint
//! answers 503: a snapshot is never served unsigned.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use base64::Engine;
use pt_entitlement::client_tls::ControlPlaneTls;
use pt_entitlement::{sha256_hex, Snapshot, SnapshotSigner};
use serde::Deserialize;
use serde_json::Value;

/// `[entitlements.vault]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VaultConfig {
    /// For example `https://vault.internal:8200`.
    pub url: String,
    /// Transit key name. Its type must be `ed25519`.
    pub key: String,
    /// Environment variable holding the Vault token. Never put the token in the file.
    #[serde(default = "default_token_env")]
    pub token_env: String,
    /// Transit mount path.
    #[serde(default = "default_mount")]
    pub mount: String,
    /// `[entitlements.vault.tls]`: CA file, client certificate, and the transport policy,
    /// as for gateways.
    #[serde(default)]
    pub tls: ControlPlaneTls,
}

fn default_token_env() -> String {
    "VAULT_TOKEN".into()
}
fn default_mount() -> String {
    "transit".into()
}

#[derive(Debug, thiserror::Error)]
pub enum SignError {
    #[error("vault: {0}")]
    Vault(String),
}

/// Signatures kept, by body hash.
const CACHE: usize = 256;

pub enum Signer {
    Local(SnapshotSigner),
    Vault(VaultSigner),
}

impl Signer {
    /// The signer `config` asks for. Fails if the Vault token isn't in the environment.
    pub fn from_config(config: &crate::config::ControlPlaneConfig) -> Result<Self, String> {
        match &config.entitlements.vault {
            Some(v) => Ok(Signer::Vault(VaultSigner::new(v)?)),
            None => SnapshotSigner::from_hex(&config.entitlements.signing_key)
                .map(Signer::Local)
                .map_err(|e| e.to_string()),
        }
    }

    /// Serialise and sign. Returns (body, hex signature, key id).
    pub async fn sign(&self, snapshot: &Snapshot) -> Result<(Vec<u8>, String, String), SignError> {
        match self {
            Signer::Local(s) => {
                let (body, sig) = s.sign(snapshot);
                Ok((body, sig, s.key_id()))
            }
            Signer::Vault(v) => {
                let body = serde_json::to_vec(snapshot).expect("snapshot serialises");
                let (sig, key_id) = v.sign(&body).await?;
                Ok((body, sig, key_id))
            }
        }
    }

    /// The local key's public key (hex), for tests and `keygen`.
    pub fn public_key_hex(&self) -> String {
        match self {
            Signer::Local(s) => s.public_key_hex(),
            Signer::Vault(_) => String::new(),
        }
    }

    /// The local key's id. With Vault, the id comes with each signature.
    pub fn key_id(&self) -> String {
        match self {
            Signer::Local(s) => s.key_id(),
            Signer::Vault(_) => String::new(),
        }
    }
}

type SignatureCache = (HashMap<String, (String, String)>, VecDeque<String>);

pub struct VaultSigner {
    http: reqwest::Client,
    sign_url: String,
    keys_url: String,
    token: String,
    /// Body hash → (hex signature, key id), and hashes oldest first.
    signed: Mutex<SignatureCache>,
    /// Key version → key id.
    key_ids: Mutex<HashMap<u64, String>>,
}

impl VaultSigner {
    pub fn new(config: &VaultConfig) -> Result<Self, String> {
        config
            .tls
            .check(&config.url)
            .map_err(|e| e.replace("control plane", "Vault"))?;
        let token = std::env::var(&config.token_env)
            .map_err(|_| format!("set {} to a Vault token", config.token_env))?;
        let http = config
            .tls
            .client_builder()?
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| e.to_string())?;
        let base = config.url.trim_end_matches('/');
        Ok(Self {
            http,
            sign_url: format!("{base}/v1/{}/sign/{}", config.mount, config.key),
            keys_url: format!("{base}/v1/{}/keys/{}", config.mount, config.key),
            token,
            signed: Mutex::new((HashMap::new(), VecDeque::new())),
            key_ids: Mutex::new(HashMap::new()),
        })
    }

    async fn sign(&self, body: &[u8]) -> Result<(String, String), SignError> {
        let digest = sha256_hex(body);
        if let Some(hit) = self
            .signed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .0
            .get(&digest)
        {
            return Ok(hit.clone());
        }
        let b64 = base64::engine::general_purpose::STANDARD;
        let resp: Value = self
            .call(self.http.post(&self.sign_url).json(&serde_json::json!({
                "input": b64.encode(body),
            })))
            .await?;
        // "vault:v<version>:<base64 signature>"
        let signature = resp["data"]["signature"]
            .as_str()
            .ok_or_else(|| SignError::Vault("no signature in the response".into()))?;
        let mut parts = signature.splitn(3, ':');
        let (Some("vault"), Some(v), Some(sig)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(SignError::Vault(format!(
                "unexpected signature {signature}"
            )));
        };
        let version: u64 = v
            .trim_start_matches('v')
            .parse()
            .map_err(|_| SignError::Vault(format!("unexpected key version {v}")))?;
        let sig = b64
            .decode(sig)
            .map_err(|e| SignError::Vault(format!("signature: {e}")))?;
        let key_id = self.key_id(version).await?;
        let out = (hex(&sig), key_id);
        let mut signed = self.signed.lock().unwrap_or_else(|e| e.into_inner());
        if signed.0.insert(digest.clone(), out.clone()).is_none() {
            signed.1.push_back(digest);
            if signed.1.len() > CACHE {
                if let Some(old) = signed.1.pop_front() {
                    signed.0.remove(&old);
                }
            }
        }
        Ok(out)
    }

    /// The key id of a Transit key version: the first 16 hex characters of the SHA-256 of
    /// its public key, as for local keys.
    async fn key_id(&self, version: u64) -> Result<String, SignError> {
        if let Some(id) = self
            .key_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&version)
        {
            return Ok(id.clone());
        }
        let resp: Value = self.call(self.http.get(&self.keys_url)).await?;
        if resp["data"]["type"].as_str() != Some("ed25519") {
            return Err(SignError::Vault(format!(
                "the Transit key is {}, not ed25519",
                resp["data"]["type"]
            )));
        }
        let public = resp["data"]["keys"][version.to_string()]["public_key"]
            .as_str()
            .ok_or_else(|| SignError::Vault(format!("no public key for version {version}")))?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(public)
            .map_err(|e| SignError::Vault(format!("public key: {e}")))?;
        let id = sha256_hex(&bytes)[..16].to_string();
        tracing::info!(version, key_id = %id, public_key = %hex(&bytes), "snapshot signing key from Vault");
        self.key_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(version, id.clone());
        Ok(id)
    }

    async fn call(&self, req: reqwest::RequestBuilder) -> Result<Value, SignError> {
        let resp = req
            .header("X-Vault-Token", &self.token)
            .send()
            .await
            .map_err(|e| SignError::Vault(e.to_string()))?;
        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .map_err(|e| SignError::Vault(format!("{status}: {e}")))?;
        if !status.is_success() {
            return Err(SignError::Vault(format!("{status}: {}", body["errors"])));
        }
        Ok(body)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
