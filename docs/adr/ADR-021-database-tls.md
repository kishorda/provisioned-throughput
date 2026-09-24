# ADR-021: Verified TLS to the databases by default

- **Status:** Accepted. Amends [ADR-017](ADR-017-sql-control-plane-store.md) and [ADR-019](ADR-019-clickhouse-usage-store.md).
- **Date:** 2026-09-24

## Context
The control plane connects to CockroachDB/PostgreSQL (reservations, key hashes,
invoices) and to ClickHouse (every usage record). Both connections were plaintext,
because the obvious TLS stacks need C builds: OpenSSL, or aws-lc through cmake. This
machine has no cmake. The operator already uses rustls with the ring provider, which
builds with only a C compiler.

## Decision
- **rustls with ring, everywhere.** sqlx uses `tls-rustls-ring-webpki`, and reqwest (for
  ClickHouse) uses `rustls-tls-webpki-roots`. Public roots are built in. No OpenSSL, no
  aws-lc, no cmake.
- **PostgreSQL/CockroachDB** use the standard URL parameters: `sslmode=verify-full` (or
  `verify-ca`), `sslrootcert` for a private CA, and `sslcert`/`sslkey` for client
  certificates (mutual TLS).
- **ClickHouse** uses an `https://` URL. Its config adds `ca_cert` for a private CA, and
  `client_cert`/`client_key` for mutual TLS.
- **Verified TLS is required for remote hosts.** At startup, the control plane refuses:
  - a database URL to a non-loopback host unless `sslmode` is `verify-ca` or
    `verify-full`. `require` isn't enough: it encrypts but doesn't check who's answering.
  - a ClickHouse `http://` URL to a non-loopback host.

  Loopback hosts and Unix sockets are allowed, so local development is unchanged.
  `allow_insecure_transport = true` (in `[store]` or `[telemetry.clickhouse]`) overrides
  the check, deliberately.

## Consequences
- ✅ Tenant data, key hashes, invoices, and usage are encrypted and server-authenticated in
  transit. Tested against PostgreSQL 18 with TLS and client-certificate authentication,
  and ClickHouse 26.9 over HTTPS with a private CA. Plaintext, a rogue CA, and a missing
  client certificate all fail.
- ✅ A misconfigured production URL fails at startup with a message saying how to fix it,
  instead of quietly running in plaintext.
- ⚠️ Certificates are read at startup. Rotating them means a restart. Pooled
  PostgreSQL connections pick up new files as they reconnect, but the ClickHouse client
  doesn't.
- Gateway-to-control-plane traffic uses TLS too: see [ADR-022](ADR-022-control-plane-tls.md).
