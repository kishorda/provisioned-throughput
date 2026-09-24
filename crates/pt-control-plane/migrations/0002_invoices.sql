-- Final monthly invoices (docs/12 §7, ADR-018). Immutable once written: corrections are
-- adjustment lines on a later invoice. Drafts aren't stored. The document is the full
-- invoice as returned by the API; the other columns are for queries and reporting.

CREATE TABLE IF NOT EXISTS invoices (
    id            TEXT PRIMARY KEY,
    tenant        TEXT NOT NULL,
    period        TEXT NOT NULL,                  -- 2026-10
    total         INT8 NOT NULL,                  -- minor units
    currency      TEXT NOT NULL,
    finalized_at  TIMESTAMPTZ,
    document      JSONB NOT NULL,
    UNIQUE (tenant, period)
);
