# ADR-022: The control plane serves HTTPS, with optional mutual TLS

- **Status:** Accepted. Extends [ADR-021](ADR-021-database-tls.md).
- **Date:** 2026-09-24

## Context
Gateways send the control plane heartbeats and usage records (tenants, reservations,
token counts), and pull entitlement snapshots, across regions. The capacity controller
pulls snapshots too. Snapshots are signed (ADR-020), but everything went over plain HTTP:
usage could be read, and region tokens could be sniffed and replayed. TLS could terminate
in the control plane itself, at an ingress or mesh, or also carry the region identity in
client certificates.

## Decision
- **The control plane terminates TLS itself.** `[server.tls]` takes `cert` and `key`, and
  the server uses rustls with the ring provider (`TlsListener` for `axum::serve`).
  Handshakes run in their own tasks with a 10 s timeout, so slow clients can't block
  accepts. ALPN offers only `http/1.1`, because axum here has no HTTP/2 and advertising h2
  breaks HTTP/2 clients. The smoke test with curl found this.
- **Optional mutual TLS.** With `client_ca` set, every connection must present a
  certificate that CA signed. Region tokens remain the identity, so a leaked token alone
  isn't enough, and neither is a stolen certificate alone.
- **Clients enforce it.** Gateways (`[entitlements.tls]` for snapshots and heartbeats,
  `[usage_export.tls]` for usage) and the capacity controller (`PT_CONTROL_PLANE_CA`,
  `PT_CONTROL_PLANE_CLIENT_CERT`/`_KEY`) use `https://` with a private CA and a client
  certificate. The same policy as ADR-021 applies: plain `http://` to a non-loopback
  control plane is refused at startup unless `allow_insecure_transport` is set.
- **One client helper.** `pt_entitlement::client_tls::ControlPlaneTls` (feature `client`)
  holds the settings, the policy check, and the rustls reqwest builder for both.

## Consequences
- ✅ Snapshots, heartbeats, and usage are encrypted and the control plane is authenticated.
  With mutual TLS, only holders of a client certificate can even reach the API.
- ✅ Tested end to end with generated certificates on every run: snapshots, heartbeats,
  and usage export over mutual TLS. A missing client certificate, an untrusted server
  certificate, and plain HTTP to the TLS port all fail.
- ⚠️ The customer API shares the listener. With `client_ca` set, customers need client
  certificates too. In production, put the customer API behind its own ingress (public
  certificate, no mutual TLS), and give internal traffic this listener, or split the
  listeners.
- ⚠️ Certificates load at startup, so rotating them needs a restart.
- ⚠️ Gateway → Quota Coordinator stays plain HTTP. It's inside one region, and carries
  demand figures, not usage.
