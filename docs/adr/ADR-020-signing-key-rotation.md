# ADR-020: Rotate snapshot signing keys with key ids and trusted sets

- **Status:** Accepted. Refines [ADR-007](ADR-007-regional-static-stability.md).
- **Date:** 2026-09-24

## Context
Entitlement snapshots are signed with Ed25519 (docs/12 §6). Gateways and the capacity
controller trusted exactly one public key. Rotating the key, whether scheduled or because
it leaked, meant verifiers rejected every new snapshot until they were reconfigured. On a
restart they would also reject their own last-known-good cache, which breaks static
stability. The options were:
- key ids with a trusted set in verifier configuration;
- announcing the next key inside snapshots;
- an offline root key that certifies short-lived signing keys.

## Decision
- **Key ids.** A key's id is the first 16 hex characters of the SHA-256 of its public key,
  so ids can't be mistyped or mismatched. The control plane sends it in `x-pt-key-id`
  with each snapshot, and `keygen` prints it.
- **Trusted sets.**
  - Gateways trust `[entitlements] public_key` plus `extra_public_keys`.
  - The capacity controller trusts a comma-separated `PT_SNAPSHOT_PUBLIC_KEY`.
  - A snapshot is verified with the key its id names. An unknown id is rejected with an
    error naming the key.
  - A snapshot without an id, from an older control plane, is tried against every
    trusted key.
- **The control plane signs with one key** (`[entitlements] signing_key`). Switching it
  means a restart, and a restart always publishes a newer snapshot version (versions start
  from the clock). So every verifier fetches a snapshot signed by the new key and
  rewrites its cache.
- **Visibility.** Caches store the key id. Gateways report their snapshot's key id in
  heartbeats and in `/internal/v1/entitlements`. `/internal/v1/regions` counts gateways by
  key id, so operators know when the old key is no longer in use.
- **Runbook** (docs/12 §6):
  1. Generate a key and add its public key to every verifier (GitOps).
  2. Restart the control plane with the new `signing_key`.
  3. When `/internal/v1/regions` shows no gateway on the old key id, remove the old public
     key.

  If a key leaks, do the same with urgency, and remove the leaked key as soon as step 2
  has propagated.

## Consequences
- ✅ Rotation needs no outage. A verifier that wasn't updated refuses new snapshots but
  keeps serving its cached one (tested).
- ✅ Nothing new in the snapshot's trust path: verifiers still only trust keys their
  operators configured.
- ⚠️ Rotation needs a configuration rollout to every verifier, twice. A leaked key stays
  trusted until that rollout finishes. An offline root with short-lived certificates would
  bound that, at the cost of expiry handling for cached snapshots.
- ⚠️ The signing key is still read from configuration. In production it belongs in a
  secret store or KMS, with signing done there.
