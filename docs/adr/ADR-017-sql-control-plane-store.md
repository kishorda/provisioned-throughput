# ADR-017: A Postgres-protocol store for the control plane

- **Status:** Accepted. Implements the storage in docs/07 §5 and docs/12 §5.
- **Date:** 2026-09-24

## Context
The control plane kept reservations, deployments, keys, idempotency keys, and incidents
in memory, so a restart lost everything. Gateways then served their cached snapshots until
the control plane was repopulated, which it never was. The design names CockroachDB
(multi-region, `REGIONAL BY ROW`). The existing migrations used CockroachDB-only syntax and
had never been applied. This machine has no database server, so tests need a database that
can be downloaded and run without root.

The store trait also had infallible reads: `get` returned `Option` and `list_live`
returned `Vec`. With a real database, an outage would read as "no reservations", and the
snapshot endpoint would publish empty entitlements that make every gateway drop every key.

## Decision
- **One SQL store, `SqlStore`, over the Postgres protocol** (sqlx; TLS added in ADR-021).
  CockroachDB in production and PostgreSQL for development and tests run the same
  queries.
- **Portable schema.** The unused migrations are consolidated into `0001_initial.sql`,
  written in the SQL both databases share (`TEXT`, separate `CREATE INDEX`, partial
  unique indexes, JSONB). CockroachDB-only settings (`REGIONAL BY ROW`, row-level TTL) are
  applied per deployment. Migrations are idempotent and recorded in `schema_migrations`.
  They're applied at startup unless `[store] migrate = false`.
- **Rows mirror the model.** A reservation is a main row, `deployments` (ordered),
  `api_keys` (hashes only), and append-only `provisioned_throughput_events`. Nested values
  are JSONB.
- **Concurrency.** An update is one transaction: `UPDATE … WHERE version = expected`,
  then the child rows. The database also enforces live-name uniqueness and one open
  incident per region, so races the service can't see still fail cleanly.
- **Fallible reads.** Every `Store` read returns `Result`. Failures become
  `ServiceError::Unavailable` → 503 `store_unavailable`. The snapshot endpoint returns 503
  rather than an empty snapshot, and the lifecycle and failover loops skip the run.
- **Planner restore.** At startup, `Service::restore_capacity` re-reserves every live
  reservation's shares and headroom in the planner.
- **Scope.** Only control-plane state. Usage records stay in the in-memory telemetry store
  until the ClickHouse pipeline (docs/09).
- **Precision.** `SystemClock` truncates to microseconds (TIMESTAMPTZ precision), so a
  resource reads back exactly as it was written.

## Consequences
- ✅ Restarts lose nothing but in-flight requests and soft state: heartbeats and region
  health rebuild within seconds. Tested end to end against PostgreSQL 18.
- ✅ A database outage degrades to 503s. Gateways keep serving their last snapshot, which
  preserves static stability (ADR-007).
- ✅ Several control-plane instances can share one database: concurrency is enforced by
  the store, not process memory.
- Running several instances: see [ADR-023](ADR-023-multi-instance-control-plane.md).
- ⚠️ If a commit fails ambiguously (the connection drops during COMMIT), the service undoes
  its planner changes even if the write landed. The next restart's `restore_capacity`
  corrects it.
- TLS to the database: see [ADR-021](ADR-021-database-tls.md).
- ⚠️ Idempotency keys older than 7 days are ignored and reusable, but only deleted by
  CockroachDB's row-level TTL. On PostgreSQL they need a periodic delete.
