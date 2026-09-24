//! Entitlement snapshots (docs/03 §2.1, ADR-007).
//!
//! The control plane builds one snapshot per region: every reservation serving there, its
//! regional CU share, and its deployments' API-key hashes and boundary policies. Snapshots
//! are signed with Ed25519, so a region only needs the public key to trust them, and are
//! versioned so a gateway never goes backwards.
//!
//! The signature covers the exact snapshot bytes. Gateways verify the bytes, then parse them.
//!
//! **Key rotation (ADR-020).** Each key has an id: the first 16 hex characters of the
//! SHA-256 of its public key, so ids can't be mistyped. The control plane signs with one
//! key and sends its id in [`KEY_ID_HEADER`]. Verifiers trust a set of public keys, so a new
//! key can be trusted before it's used and an old one removed after.

#[cfg(feature = "client")]
pub mod client_tls;

use ed25519_dalek::{Signer, Verifier};
use pt_admission::BoundaryPolicy;
use pt_core::{Shape, Tier};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// HTTP header carrying the hex Ed25519 signature of the response body.
pub const SIGNATURE_HEADER: &str = "x-pt-signature";

/// HTTP header naming the key that signed the body (see [`key_id`]).
pub const KEY_ID_HEADER: &str = "x-pt-key-id";

/// A public key's id: the first 16 hex characters of its SHA-256.
pub fn key_id(public_key: &ed25519_dalek::VerifyingKey) -> String {
    sha256_hex(public_key.as_bytes())[..16].to_string()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub region: String,
    /// Increases with every change. Gateways ignore snapshots that aren't newer.
    pub version: u64,
    /// RFC 3339.
    pub generated_at: String,
    pub reservations: Vec<ReservationEntitlement>,
    pub deployments: Vec<DeploymentEntitlement>,
    /// Region failures that activate failover entitlements: open incidents, and resolved
    /// ones whose return ramp hasn't finished (docs/07 §4).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failovers: Vec<RegionFailover>,
}

impl Snapshot {
    /// A reservation's CUs in this region at `now_ms`: its own share plus the active part
    /// of its failover entitlements.
    pub fn effective_cus(&self, r: &ReservationEntitlement, now_ms: u64) -> f64 {
        f64::from(r.cus) + failover_cus(&r.failover, &self.failovers, now_ms)
    }
}

/// The active CUs of `shares`, given the current region failures.
pub fn failover_cus(shares: &[FailoverShare], failovers: &[RegionFailover], now_ms: u64) -> f64 {
    shares
        .iter()
        .map(|s| {
            let active = failovers
                .iter()
                .filter(|f| f.region == s.from_region)
                .map(|f| f.activation(now_ms))
                .fold(0.0, f64::max);
            f64::from(s.cus) * active
        })
        .sum()
}

/// A failed region. Failover entitlements from it are fully active from `started_at_ms`
/// until `ended_at_ms`, then ramp down linearly over `return_ramp_ms` while traffic
/// returns to the recovered region.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionFailover {
    pub region: String,
    /// The incident that caused it.
    pub incident: String,
    pub started_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at_ms: Option<u64>,
    pub return_ramp_ms: u64,
}

impl RegionFailover {
    /// How much of a failover entitlement from this region is active: 0 to 1.
    pub fn activation(&self, now_ms: u64) -> f64 {
        if now_ms < self.started_at_ms {
            return 0.0;
        }
        match self.ended_at_ms {
            None => 1.0,
            Some(end) if now_ms <= end => 1.0,
            Some(_) if self.return_ramp_ms == 0 => 0.0,
            Some(end) => (1.0 - (now_ms - end) as f64 / self.return_ramp_ms as f64).max(0.0),
        }
    }
}

/// Dormant capacity a reservation may use in this region while another region has failed
/// (Multi-region SKU).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailoverShare {
    /// The region whose traffic this region takes over.
    pub from_region: String,
    pub cus: u32,
}

/// A reservation's entitlement in one region.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservationEntitlement {
    pub id: String,
    pub tenant: String,
    pub model: String,
    /// This region's share of the reservation's CUs.
    pub cus: u32,
    pub tier: Tier,
    /// `PerformanceProfile` of the region's pool for this model.
    pub profile: String,
    pub shape: Shape,
    /// Failover entitlements, dormant until their region fails.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failover: Vec<FailoverShare>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentEntitlement {
    pub id: String,
    pub reservation: String,
    /// SHA-256 of the current inference API key, lowercase hex. Keys never leave the
    /// control plane.
    pub api_key_sha256: String,
    /// Keys replaced by a rotation that are still in their grace period.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub previous_keys: Vec<PreviousKey>,
    /// Cap on this deployment's share of the reservation's entitlement, in (0, 1].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_share: Option<f64>,
    pub boundary_policy: BoundaryPolicy,
}

/// A rotated-out key that keeps working until `expires_at_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviousKey {
    pub api_key_sha256: String,
    /// Unix milliseconds. Gateways reject the key from this moment.
    pub expires_at_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("key must be {0} hex bytes")]
    BadKey(usize),
    #[error("signature is not valid hex")]
    BadSignatureEncoding,
    #[error("signature does not match")]
    BadSignature,
    #[error("signed by key {0}, which isn't trusted")]
    UnknownKey(String),
    #[error("no trusted keys configured")]
    NoKeys,
    #[error("snapshot is not valid JSON: {0}")]
    Parse(#[from] serde_json::Error),
}

/// Held by the control plane only.
pub struct SnapshotSigner(ed25519_dalek::SigningKey);

/// Distributed to every region: the public keys a verifier trusts, by id.
#[derive(Debug, Clone)]
pub struct SnapshotVerifier(Vec<(String, ed25519_dalek::VerifyingKey)>);

impl SnapshotSigner {
    /// From a 32-byte hex seed.
    pub fn from_hex(seed: &str) -> Result<Self, Error> {
        let bytes: [u8; 32] = decode_hex(seed.trim())
            .and_then(|b| b.try_into().ok())
            .ok_or(Error::BadKey(32))?;
        Ok(Self(ed25519_dalek::SigningKey::from_bytes(&bytes)))
    }

    /// A new random key, as (seed hex, public key hex).
    pub fn generate() -> (String, String) {
        let mut seed = [0u8; 32];
        seed[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        seed[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        let key = ed25519_dalek::SigningKey::from_bytes(&seed);
        (
            encode_hex(&seed),
            encode_hex(key.verifying_key().as_bytes()),
        )
    }

    pub fn public_key_hex(&self) -> String {
        encode_hex(self.0.verifying_key().as_bytes())
    }

    /// This key's id, sent in [`KEY_ID_HEADER`].
    pub fn key_id(&self) -> String {
        key_id(&self.0.verifying_key())
    }

    /// Serialise and sign. Returns (body, hex signature).
    pub fn sign(&self, snapshot: &Snapshot) -> (Vec<u8>, String) {
        let body = serde_json::to_vec(snapshot).expect("snapshot serialises");
        let sig = self.0.sign(&body);
        (body, encode_hex(&sig.to_bytes()))
    }
}

impl SnapshotVerifier {
    /// Trust one public key (hex).
    pub fn from_hex(public_key: &str) -> Result<Self, Error> {
        Self::from_hex_list([public_key])
    }

    /// Trust several public keys (hex), for example the current key and the next one
    /// during a rotation.
    pub fn from_hex_list<'a>(keys: impl IntoIterator<Item = &'a str>) -> Result<Self, Error> {
        let mut out: Vec<(String, ed25519_dalek::VerifyingKey)> = Vec::new();
        for k in keys {
            let bytes: [u8; 32] = decode_hex(k.trim())
                .and_then(|b| b.try_into().ok())
                .ok_or(Error::BadKey(32))?;
            let key =
                ed25519_dalek::VerifyingKey::from_bytes(&bytes).map_err(|_| Error::BadKey(32))?;
            let id = key_id(&key);
            if !out.iter().any(|(i, _)| *i == id) {
                out.push((id, key));
            }
        }
        if out.is_empty() {
            return Err(Error::NoKeys);
        }
        Ok(Self(out))
    }

    /// Ids of the trusted keys.
    pub fn key_ids(&self) -> Vec<String> {
        self.0.iter().map(|(id, _)| id.clone()).collect()
    }

    /// Check `signature` over `body` with any trusted key, then parse it.
    pub fn verify(&self, body: &[u8], signature: &str) -> Result<Snapshot, Error> {
        self.verify_with(body, signature, None).map(|(s, _)| s)
    }

    /// Check `signature` over `body` with the key named by `key_id`, or with any trusted
    /// key when it's `None` (a control plane from before key ids). Returns the snapshot and
    /// the id of the key that verified it.
    pub fn verify_with(
        &self,
        body: &[u8],
        signature: &str,
        key_id: Option<&str>,
    ) -> Result<(Snapshot, String), Error> {
        let sig: [u8; 64] = decode_hex(signature.trim())
            .and_then(|b| b.try_into().ok())
            .ok_or(Error::BadSignatureEncoding)?;
        let sig = ed25519_dalek::Signature::from_bytes(&sig);
        let candidates: Vec<&(String, ed25519_dalek::VerifyingKey)> = match key_id {
            Some(id) => {
                let found: Vec<_> = self.0.iter().filter(|(i, _)| i == id.trim()).collect();
                if found.is_empty() {
                    return Err(Error::UnknownKey(id.trim().to_string()));
                }
                found
            }
            None => self.0.iter().collect(),
        };
        let (id, _) = candidates
            .into_iter()
            .find(|(_, k)| k.verify(body, &sig).is_ok())
            .ok_or(Error::BadSignature)?;
        Ok((serde_json::from_slice(body)?, id.clone()))
    }
}

/// SHA-256 as lowercase hex. Used for API-key hashes on both sides.
pub fn sha256_hex(bytes: &[u8]) -> String {
    encode_hex(&Sha256::digest(bytes))
}

pub fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> Snapshot {
        Snapshot {
            region: "eu-west".into(),
            version: 7,
            generated_at: "2026-10-01T00:00:00Z".into(),
            reservations: vec![ReservationEntitlement {
                id: "pt-1".into(),
                tenant: "acme".into(),
                model: "m".into(),
                cus: 4,
                tier: Tier::Agentic,
                profile: "p".into(),
                shape: Shape {
                    input_p95: 1,
                    input_max: 2,
                    output_p95: 1,
                    context_ceiling: 4,
                    cache_hit_ratio: 0.0,
                    burst_factor: 1.0,
                },
                failover: vec![FailoverShare {
                    from_region: "eu-central".into(),
                    cus: 2,
                }],
            }],
            deployments: vec![DeploymentEntitlement {
                id: "dep-1".into(),
                reservation: "pt-1".into(),
                api_key_sha256: sha256_hex(b"ptk_x"),
                previous_keys: vec![PreviousKey {
                    api_key_sha256: sha256_hex(b"ptk_old"),
                    expires_at_ms: 1_800_000_000_000,
                }],
                max_share: Some(0.25),
                boundary_policy: BoundaryPolicy::default(),
            }],
            failovers: vec![RegionFailover {
                region: "eu-central".into(),
                incident: "inc-1".into(),
                started_at_ms: 1_000,
                ended_at_ms: Some(2_000),
                return_ramp_ms: 1_000,
            }],
        }
    }

    #[test]
    fn failover_activates_then_ramps_down() {
        let s = snapshot();
        let r = &s.reservations[0];
        assert_eq!(s.effective_cus(r, 999), 4.0, "not yet failed");
        assert_eq!(s.effective_cus(r, 1_000), 6.0);
        assert_eq!(s.effective_cus(r, 2_000), 6.0);
        assert_eq!(s.effective_cus(r, 2_500), 5.0, "half way down the ramp");
        assert_eq!(s.effective_cus(r, 3_000), 4.0);
        assert_eq!(s.effective_cus(r, 9_000), 4.0);

        let mut open = s.failovers[0].clone();
        open.ended_at_ms = None;
        assert_eq!(open.activation(u64::MAX), 1.0);
        // Failures of other regions don't activate it.
        open.region = "us-east".into();
        assert_eq!(failover_cus(&r.failover, &[open], 1_500), 0.0);
    }

    #[test]
    fn sign_and_verify_round_trip() {
        let (seed, public) = SnapshotSigner::generate();
        let signer = SnapshotSigner::from_hex(&seed).unwrap();
        assert_eq!(signer.public_key_hex(), public);
        let (body, sig) = signer.sign(&snapshot());
        let verified = SnapshotVerifier::from_hex(&public)
            .unwrap()
            .verify(&body, &sig)
            .unwrap();
        assert_eq!(verified, snapshot());
    }

    #[test]
    fn tampering_and_wrong_keys_are_rejected() {
        let (seed, public) = SnapshotSigner::generate();
        let (body, sig) = SnapshotSigner::from_hex(&seed).unwrap().sign(&snapshot());
        let verifier = SnapshotVerifier::from_hex(&public).unwrap();

        let tampered = String::from_utf8(body.clone())
            .unwrap()
            .replace("\"cus\":4", "\"cus\":400");
        assert!(matches!(
            verifier.verify(tampered.as_bytes(), &sig),
            Err(Error::BadSignature)
        ));

        let (_, other_public) = SnapshotSigner::generate();
        let other = SnapshotVerifier::from_hex(&other_public).unwrap();
        assert!(matches!(
            other.verify(&body, &sig),
            Err(Error::BadSignature)
        ));

        assert!(matches!(
            verifier.verify(&body, "zz"),
            Err(Error::BadSignatureEncoding)
        ));
        assert!(SnapshotSigner::from_hex("abcd").is_err());
    }

    #[test]
    fn several_trusted_keys_and_key_ids() {
        let (old_seed, old_pub) = SnapshotSigner::generate();
        let (new_seed, new_pub) = SnapshotSigner::generate();
        let old = SnapshotSigner::from_hex(&old_seed).unwrap();
        let new = SnapshotSigner::from_hex(&new_seed).unwrap();
        assert_eq!(old.key_id().len(), 16);
        assert_ne!(old.key_id(), new.key_id());

        // During a rotation, both are trusted.
        let both = SnapshotVerifier::from_hex_list([old_pub.as_str(), new_pub.as_str()]).unwrap();
        assert_eq!(both.key_ids(), [old.key_id(), new.key_id()]);
        for signer in [&old, &new] {
            let (body, sig) = signer.sign(&snapshot());
            let (s, id) = both
                .verify_with(&body, &sig, Some(&signer.key_id()))
                .unwrap();
            assert_eq!((s, id), (snapshot(), signer.key_id()));
            // Without an id (an older control plane), any trusted key is tried.
            assert_eq!(
                both.verify_with(&body, &sig, None).unwrap().1,
                signer.key_id()
            );
        }

        // After it, the old key is no longer trusted.
        let only_new = SnapshotVerifier::from_hex(&new_pub).unwrap();
        let (body, sig) = old.sign(&snapshot());
        assert!(matches!(
            only_new.verify_with(&body, &sig, Some(&old.key_id())),
            Err(Error::UnknownKey(id)) if id == old.key_id()
        ));
        assert!(matches!(
            only_new.verify_with(&body, &sig, None),
            Err(Error::BadSignature)
        ));
        // A key id that doesn't match the signature fails.
        assert!(matches!(
            both.verify_with(&body, &sig, Some(&new.key_id())),
            Err(Error::BadSignature)
        ));
        assert!(matches!(
            SnapshotVerifier::from_hex_list(Vec::<&str>::new()),
            Err(Error::NoKeys)
        ));
    }

    #[test]
    fn hex_round_trip() {
        assert_eq!(
            decode_hex(&encode_hex(&[0, 15, 255])),
            Some(vec![0, 15, 255])
        );
        assert_eq!(decode_hex("0g"), None);
        assert_eq!(decode_hex("abc"), None);
    }
}
