# ADR-038: Sign snapshots with a key held in Vault Transit

- **Status:** Accepted. Extends [ADR-020](ADR-020-signing-key-rotation.md).
- **Date:** 2026-09-26

## Context
Entitlement snapshots are signed with Ed25519 (docs/12 §6), and the private key was read
from `[entitlements] signing_key` in the control plane's configuration. Whoever can read
that file can mint entitlements for every region. Production needs the key held by a key
management service, where the control plane can use it but can't read it.

## Decision
- **Where the key lives.** `[entitlements.vault]` signs with a key in HashiCorp Vault's
  Transit engine (`type = ed25519`), instead of `signing_key`. Exactly one of the two must
  be set. The key never leaves Vault.
- **Signing.** Each snapshot body goes to `POST /v1/<mount>/sign/<key>`. The response names
  the key version that signed it, and the control plane reads that version's public key
  from `GET /v1/<mount>/keys/<key>`. The key id is derived as for local keys (the first 16
  hex characters of the SHA-256 of the public key) and sent in `x-pt-key-id`. Rotating the
  key in Vault is ADR-020's rotation: add the new public key to the gateways'
  `extra_public_keys` first. The control plane logs each new version's public key.
- **Credentials.** The Vault token comes from an environment variable (`token_env`,
  default `VAULT_TOKEN`), never from the file. The control plane refuses to start without
  it. TLS to Vault uses the gateways' client TLS settings (`[entitlements.vault.tls]`), and
  plain HTTP to a non-loopback Vault is refused.
- **Caching.** Signatures are cached by the body's hash (256 entries), so gateways
  long-polling for the same snapshot cost one Vault call.
- **Failure.** If Vault can't sign, the snapshot endpoint answers 503
  `signing_unavailable`. Gateways keep serving their cached snapshot (static stability,
  ADR-007).

## Consequences
- ✅ A leaked config file no longer leaks the ability to mint entitlements.
- ✅ No change for gateways or the controller: signatures and key ids look the same.
- ⚠️ Vault is now on the path of entitlement changes, but not of serving: a Vault outage
  delays new entitlements, and regions keep enforcing the last good snapshot.
- ⚠️ Only Vault Transit is implemented. Cloud KMSs (AWS KMS, Google Cloud KMS) would be
  other `Signer` variants. Most of them don't sign Ed25519 today.
- ⚠️ Token renewal is left to the environment, for example a Vault Agent sidecar that
  keeps the variable's token fresh by restarting the process.
