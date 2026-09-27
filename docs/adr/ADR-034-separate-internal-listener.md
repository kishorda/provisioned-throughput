# ADR-034: Serve region traffic on its own listener

- **Status:** Accepted. Amends [ADR-022](ADR-022-control-plane-tls.md).
- **Date:** 2026-09-26

## Context
The control plane served everything on one listener: the customer API (`/v1/...`) and
region traffic (`/internal/...`: snapshots, heartbeats, usage, incidents, steering). With
`[server.tls] client_ca`, every connection needed a client certificate. That's right for
gateways and controllers, but it also forced customers to present certificates. So mutual
TLS was only usable behind a separate ingress for the customer API.

## Decision
- An optional `[server.internal]` section (`listen`, `tls`) serves region traffic on its own
  address with its own TLS.
- When it's set, `server.listen` serves only the customer API, and
  `server.internal.listen` serves only `/internal/...`. Each answers 404 for the other's
  paths (`pt_control_plane::restrict`, `Surface`), and both answer `/healthz`.
- Without it, one listener serves everything, as before.
- The addresses must differ (config validation).
- Gateways (`[entitlements] control_plane_url`, `[usage_export] url`) and the capacity
  controller (`PT_CONTROL_PLANE_URL`) point at the internal listener.

## Consequences
- ✅ Region traffic can require client certificates (`[server.internal.tls] client_ca`)
  while customers use ordinary server TLS on `[server.tls]`.
- ✅ The two can be exposed differently: the internal listener on a private network only.
- ⚠️ A single process still serves both. A flood on one listener shares CPU with the
  other.
