# ADR-019: Usage records in ClickHouse, behind a fallible store

- **Status:** Accepted. Implements the metering store in docs/03 §2.2 and docs/09.
- **Date:** 2026-09-24

## Context
Usage records drive customer telemetry, SLA reports and credits, quotes from history,
and spillover charges on invoices (ADR-018). They were kept in memory, so a
control-plane restart lost the month's spillover charges and SLA data. The design names
ClickHouse, fed through Redpanda. The usage store trait was also infallible, so an outage
would have read as "no usage": invoices would have been finalised without spillover or
credits.

## Decision
- **A ClickHouse `UsageStore`** (`pt-telemetry/src/clickhouse.rs`) over the HTTP interface
  with `reqwest`, so there's no native driver and no C dependency. Values go in as query
  parameters (`{name:Type}`), never spliced into SQL.
- **One table, `usage_records`.**
  - `ReplacingMergeTree`, partitioned by month, ordered by
    `(tenant, reservation, at_ms, request_id)`.
  - `record` holds the full `UsageRecord` JSON and is the source of truth for reads, so
    records round-trip exactly. Tokens, WU, class, and outcome are also columns, for SQL
    aggregation.
  - Retention is a table TTL from `[telemetry] retention_days`, re-applied at every
    startup.
- **Exactly once.** Ingest skips request ids already stored (bloom-filter index) and
  duplicates within a batch. Concurrent duplicates can still both be inserted, so reads use
  `LIMIT 1 BY request_id`, and merges collapse identical rows.
- **Fallible store.** `append`, `range`, and `prune` return `Result`:
  - Ingest answers 503 `store_unavailable`, and gateways keep and retry the batch.
  - Reports, quotes, and invoices fail with 503 rather than showing no usage.
  - Invoice finalisation fails, stores nothing, and retries later.
- **Chosen at startup.** `[telemetry.clickhouse]` or `PT_CLICKHOUSE_URL` selects
  ClickHouse; `PT_CLICKHOUSE_PASSWORD` keeps the password out of files. Without it, usage
  stays in memory with a warning (`UsageBackend`).
- **Direct writes for now.** Gateways still push batches to the control plane's ingest
  API, which writes to ClickHouse. Redpanda in between (docs/09 §2) is a later step.

## Consequences
- ✅ Restarts lose no usage. Invoices and SLA credits are complete, and a store outage
  delays them instead of making them wrong.
- ✅ Tested against a real ClickHouse 26.9 server (round trip, exactly-once, races, TTL,
  outage), and end to end through ingest, a restart, and invoicing.
- ⚠️ Reads still load a reservation's records for the range and aggregate in Rust. For
  big tenants, move the usage and SLA aggregation into ClickHouse SQL over the typed
  columns.
- ⚠️ The existence check before insert is a read per batch, and a race can store a
  duplicate row until merges collapse it. Reads are correct either way.
- TLS to ClickHouse: see [ADR-021](ADR-021-database-tls.md).
- ⚠️ Without Redpanda, the control plane's ingest API is on the metering path. Gateways
  buffer 100,000 records and retry while it's down.
